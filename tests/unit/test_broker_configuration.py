import pytest
import redis

from taskforge.queue.broker import (
    MemoryBroker,
    RedisBroker,
    get_broker,
)


def test_redis_broker_fails_closed_on_connection_error(monkeypatch):
    """Verifies that when Redis connection fails in Redis broker mode,

    get_broker() raises a fatal RuntimeError rather than silently falling back to MemoryBroker.
    """
    # Reset global broker
    import taskforge.queue.broker as broker_mod

    broker_mod._global_broker = None

    class BrokenRedis(redis.Redis):
        def ping(self):
            raise redis.ConnectionError("Simulated Redis outage")

    broken_client = BrokenRedis()

    with pytest.raises(RuntimeError, match="FATAL: Unable to connect to Redis broker"):
        get_broker(broken_client)


def test_memory_broker_explicitly_configured(monkeypatch):
    """Verifies that MemoryBroker is only selected when broker_type is explicitly set to 'memory'."""
    import taskforge.queue.broker as broker_mod
    from taskforge.core.config import get_settings

    broker_mod._global_broker = None

    settings = get_settings()
    monkeypatch.setattr(settings, "broker_type", "memory")

    broker = get_broker()
    assert isinstance(broker, MemoryBroker)

    # Clean up
    broker_mod._global_broker = None


def test_redis_broker_queue_depth_does_not_mask_errors():
    """Verifies that RedisBroker.get_queue_depth() propagates Redis connection errors

    rather than masking them as an empty queue (0).
    """

    class BrokenRedis(redis.Redis):
        def zcard(self, name):
            raise redis.ConnectionError("Redis down")

    broken_client = BrokenRedis()
    broken_broker = RedisBroker(broken_client, queue_name="test:mask:q")
    with pytest.raises(redis.ConnectionError):
        broken_broker.get_queue_depth()


def test_fifo_score_formula_boundary_and_tier_separation():
    """G8.2 & G8.3 Mathematical Boundary & Separation Proof:

    Proves the exact-integer bounds of the formula: score = (priority * 10^12) - seq
    1. For every priority p in {1, 2, 3, 4}:
       - Lowest score at MAX_SEQ (10^12 - 1): min_score(p) = (p - 1)*10^12 + 1
       - Highest score at seq = 1:           max_score(p) = p*10^12 - 1
    2. Strict separation between adjacent tiers:
       min_score(p) - max_score(p - 1) = 2 > 0 for all p in {2, 3, 4}.
       The score bands are strictly disjoint with a safety margin of 2.
    3. IEEE-754 precision:
       Every integer up to 4 * 10^12 has exact representation in float (since 2^53 - 1 ~ 9.007 * 10^15).
    """
    MAX_SEQ = 10**12 - 1

    for p in range(2, 5):
        min_score_p = (p * 10**12) - MAX_SEQ
        max_score_prev = ((p - 1) * 10**12) - 1

        # Strict disjoint separation
        assert min_score_p > max_score_prev, f"Tier collision between P{p} and P{p - 1}!"
        assert min_score_p - max_score_prev == 2, (
            f"Expected margin of 2, got {min_score_p - max_score_prev}"
        )

        # Exact IEEE-754 integer representation check
        assert int(float(min_score_p)) == min_score_p
        assert int(float(max_score_prev)) == max_score_prev

    # Verify per-priority sequence keys in RedisBroker
    from taskforge.queue.redis_client import create_redis_client

    client = create_redis_client()
    try:
        client.ping()
        test_q = f"taskforge:boundary:test:{p}:tasks"
        broker = RedisBroker(client, queue_name=test_q)

        # Enqueue P3 at seq=1
        broker.enqueue("p3-new", 3, {"data": "p3"})
        # Enqueue P4 at extreme sequence (near boundary)
        seq_p4_key = f"{test_q}:seq:4"
        client.set(seq_p4_key, MAX_SEQ - 10)  # seq will become MAX_SEQ - 9
        broker.enqueue("p4-boundary", 4, {"data": "p4"})

        # ZPOPMAX must pop P4 first despite P4 having an extreme sequence of ~1 trillion!
        popped_1 = broker.dequeue()
        assert popped_1["task_id"] == "p4-boundary", "Priority tier inversion at boundary!"

        popped_2 = broker.dequeue()
        assert popped_2["task_id"] == "p3-new"

        # Clean up
        client.delete(test_q)
        client.delete(f"{test_q}:seq:3")
        client.delete(f"{test_q}:seq:4")
        client.delete(f"{test_q}:in_flight")
    finally:
        client.close()


def test_lua_ack_empty_token_bypass_strictly_rejected():
    """G8.2/G8.3: Verify that LUA_ACK strictly rejects acknowledgements with empty or missing delivery tokens."""
    import uuid

    from taskforge.queue.redis_client import create_redis_client

    client = create_redis_client()
    test_q = f"taskforge:test:ack:{uuid.uuid4().hex}:tasks"
    broker = RedisBroker(client, queue_name=test_q)
    try:
        broker.enqueue("task-ack-fencing", 2, {"data": "test"})
        msg = broker.dequeue()
        assert msg is not None
        legit_token = msg["delivery_token"]
        assert legit_token.startswith("del-")

        # In-flight depth is 1
        assert broker.get_in_flight_depth() == 1

        # 1. Bypass attempt with empty string -> strictly REJECTED
        assert broker.ack("task-ack-fencing", delivery_token="") is False
        assert broker.get_in_flight_depth() == 1

        # 2. Bypass attempt with None -> strictly REJECTED
        assert broker.ack("task-ack-fencing", delivery_token=None) is False
        assert broker.get_in_flight_depth() == 1

        # 3. Stale token attempt -> strictly REJECTED
        assert broker.ack("task-ack-fencing", delivery_token="del-stale-token") is False
        assert broker.get_in_flight_depth() == 1

        # 4. Legitimate token -> ACCEPTED
        assert broker.ack("task-ack-fencing", delivery_token=legit_token) is True
        assert broker.get_in_flight_depth() == 0

    finally:
        client.delete(test_q)
        client.delete(f"{test_q}:in_flight")
        client.delete(f"{test_q}:seq:2")
        client.close()


def test_lua_reclaim_enforces_sequence_boundary():
    """G8.2/G8.3: Verify that LUA_RECLAIM enforces MAX_SEQ and fails closed when sequence is exhausted,
    WITHOUT destructively removing the message from the in_flight set.
    """
    import time
    import uuid

    from taskforge.queue.redis_client import create_redis_client

    client = create_redis_client()
    test_q = f"taskforge:test:reclaim:boundary:{uuid.uuid4().hex}:tasks"
    broker = RedisBroker(client, queue_name=test_q, visibility_timeout_seconds=1)
    try:
        broker.enqueue("task-reclaim-boundary", 2, {"data": "test"})
        msg = broker.dequeue()
        assert msg is not None

        # Verify in_flight has 1 item
        assert broker.get_in_flight_depth() == 1

        # Simulate sequence exhaustion on priority 2
        seq_p2_key = f"{test_q}:seq:2"
        client.set(seq_p2_key, 999999999999)  # MAX_SEQ

        # Wait for visibility timeout
        time.sleep(1.2)

        # Reclaim should fail closed due to boundary exceeded
        with pytest.raises(redis.exceptions.ResponseError, match="Sequence boundary exceeded"):
            broker.reclaim_expired()

        # CRITICAL INVARIANT: The message MUST NOT have been removed from in_flight!
        assert broker.get_in_flight_depth() == 1, "Data loss: message was removed from in-flight before sequence check!"

    finally:
        client.delete(test_q)
        client.delete(f"{test_q}:in_flight")
        client.delete(f"{test_q}:seq:2")
        client.close()


def test_lua_reclaim_handles_malformed_envelope_and_multiple_messages_safely():
    """Finding 3: Verify LUA_RECLAIM does not destructively remove malformed messages,
    and handles multiple expired messages safely.
    """
    import json
    import time
    import uuid

    from taskforge.queue.redis_client import create_redis_client

    client = create_redis_client()
    test_q = f"taskforge:test:reclaim:malformed:{uuid.uuid4().hex}:tasks"
    broker = RedisBroker(client, queue_name=test_q, visibility_timeout_seconds=1)
    try:
        # Enqueue 2 valid tasks
        broker.enqueue("valid-task-1", 2, {"data": "valid-1"})
        broker.enqueue("valid-task-2", 3, {"data": "valid-2"})

        msg1 = broker.dequeue()
        msg2 = broker.dequeue()
        assert msg1 and msg2

        # Inject a malformed envelope directly into in_flight
        in_flight_key = f"{test_q}:in_flight"
        malformed_envelope = "{malformed-json-envelope"
        client.zadd(in_flight_key, {malformed_envelope: time.time() - 10.0})

        # Inject an envelope with invalid inner raw_message JSON
        bad_inner_envelope = json.dumps({"raw_message": "{corrupted-inner-json", "task_id": "bad-inner"})
        client.zadd(in_flight_key, {bad_inner_envelope: time.time() - 10.0})

        # Wait for valid tasks to expire
        time.sleep(1.2)

        # Total in-flight is 4 (2 valid + 2 malformed)
        assert broker.get_in_flight_depth() == 4

        # Run reclaim: valid messages should be reclaimed to ready queue
        reclaimed = broker.reclaim_expired()
        assert reclaimed == 2

        # The 2 malformed items remain safely in in-flight (not silently dropped into the void)
        assert broker.get_in_flight_depth() == 2

        # Both valid messages are now back in ready queue
        assert broker.get_queue_depth() == 2
        popped1 = broker.dequeue()
        popped2 = broker.dequeue()
        assert popped1 and popped2
        popped_ids = {popped1["task_id"], popped2["task_id"]}
        assert popped_ids == {"valid-task-1", "valid-task-2"}

    finally:
        client.delete(test_q)
        client.delete(f"{test_q}:in_flight")
        client.delete(f"{test_q}:seq:2")
        client.delete(f"{test_q}:seq:3")
        client.close()


def test_memory_broker_ack_requires_token():
    """Verify MemoryBroker also enforces non-empty delivery token matching."""
    from taskforge.queue.broker import MemoryBroker

    broker = MemoryBroker()
    broker.enqueue("mem-task-1", 2, {"data": "test"})
    msg = broker.dequeue()
    assert msg is not None
    token = msg["delivery_token"]

    # Reject without token
    assert broker.ack("mem-task-1", delivery_token="") is False
    assert broker.ack("mem-task-1", delivery_token=None) is False
    assert broker.ack("mem-task-1", delivery_token="wrong-token") is False

    # Accept with token
    assert broker.ack("mem-task-1", delivery_token=token) is True
