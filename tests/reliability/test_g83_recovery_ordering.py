"""
Milestone G8.3.1 — Recovery Ordering & Safe Delivery Disposition Test Suite.
Verifies deterministic ordering between PostgreSQL lease recovery and Redis message reclamation,
safe delivery acknowledgement contracts, and strict state eligibility boundaries.
"""

import threading
import time
import uuid
from datetime import timedelta
from unittest.mock import MagicMock

import pytest
import redis
from sqlalchemy import select

from taskforge.core.state_machine import TaskPriority, TaskStatus
from taskforge.database.models import Task, get_utc_now
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal
from taskforge.queue.broker import RedisBroker
from taskforge.queue.redis_client import create_redis_client
from taskforge.workers.dispatcher import WorkerDispatcher

TEST_KEY_PREFIX = "taskforge:g83:recovery:"


@pytest.fixture(scope="module")
def redis_conn():
    client = create_redis_client()
    try:
        client.ping()
        yield client
    finally:
        client.close()


@pytest.fixture
def test_queue(redis_conn):
    queue_name = f"{TEST_KEY_PREFIX}{uuid.uuid4().hex}:tasks"
    yield queue_name
    assert queue_name.startswith(TEST_KEY_PREFIX), f"Refusing to delete non-test key: {queue_name}"
    redis_conn.delete(queue_name)
    redis_conn.delete(f"{queue_name}:in_flight")
    redis_conn.delete(f"{queue_name}:seq:1")
    redis_conn.delete(f"{queue_name}:seq:2")
    redis_conn.delete(f"{queue_name}:seq:3")
    redis_conn.delete(f"{queue_name}:seq:4")


def test_g831_t01_db_recovery_commits_before_redis_reclaim(redis_conn, test_queue):
    """T01: Deterministic ordering test.
    Verifies that Stage 1 (PostgreSQL recovery) executes and commits strictly before
    Stage 2 (Redis in-flight reclamation) is invoked.
    """
    broker = RedisBroker(redis_conn, queue_name=test_queue)
    dispatcher = WorkerDispatcher(worker_id="test-dispatcher-t01", broker=broker)

    call_order = []

    original_recover = TaskRepository.recover_expired_leases
    original_reclaim = broker.reclaim_expired

    def instrumented_recover(self):
        call_order.append("postgres_recovery_started")
        result = original_recover(self)
        call_order.append("postgres_recovery_committed")
        return result

    def instrumented_reclaim(*args, **kwargs):
        call_order.append("redis_reclaim_started")
        return original_reclaim(*args, **kwargs)

    TaskRepository.recover_expired_leases = instrumented_recover
    broker.reclaim_expired = instrumented_reclaim

    try:
        rec_count, rcl_count = dispatcher.run_recovery_cycle(visibility_timeout_seconds=30.0)
        assert call_order == [
            "postgres_recovery_started",
            "postgres_recovery_committed",
            "redis_reclaim_started",
        ], f"Incorrect operation ordering: {call_order}"
    finally:
        TaskRepository.recover_expired_leases = original_recover
        broker.reclaim_expired = original_reclaim


def test_g831_t02_postgres_failure_aborts_redis_reclaim(redis_conn, test_queue):
    """T02: Database failure test.
    If PostgreSQL recovery fails or throws an exception, Redis reclamation must NEVER run,
    and the database exception must propagate cleanly.
    """
    broker = RedisBroker(redis_conn, queue_name=test_queue)
    dispatcher = WorkerDispatcher(worker_id="test-dispatcher-t02", broker=broker)

    mock_reclaim = MagicMock(return_value=0)
    broker.reclaim_expired = mock_reclaim

    original_recover = TaskRepository.recover_expired_leases

    def failing_recover(self):
        raise RuntimeError("Simulated transient PostgreSQL connection failure")

    TaskRepository.recover_expired_leases = failing_recover

    try:
        with pytest.raises(RuntimeError, match="Simulated transient PostgreSQL connection failure"):
            dispatcher.run_recovery_cycle(visibility_timeout_seconds=30.0)

        # Invariant: Redis reclaim must not have been invoked
        mock_reclaim.assert_not_called()
    finally:
        TaskRepository.recover_expired_leases = original_recover


def test_g831_t03_concurrent_recovery_workers_no_duplicate_recovery():
    """T03: Concurrent recovery test.
    Two recovery workers concurrently executing recover_expired_leases() under PostgreSQL row locks
    (SKIP LOCKED) must never recover the same task twice.
    """
    task_ids = []
    past_time = get_utc_now() - timedelta(seconds=60)

    with SessionLocal() as db:
        repo = TaskRepository(db)
        for i in range(10):
            t = repo.create_task(
                task_name=f"Concurrent Expired Task {i}",
                task_type="noop",
                payload={"i": i},
                priority=TaskPriority.HIGH.value,
            )
            # Force task into RUNNING with expired lease in the past
            t.status = TaskStatus.RUNNING.value
            t.lease_token = f"expired-token-{i}"
            t.lease_expires_at = past_time
            task_ids.append(t.id)
        db.commit()

    results_worker_1 = []
    results_worker_2 = []

    def _worker_1_sweep():
        with SessionLocal() as db1:
            repo1 = TaskRepository(db1)
            recovered = repo1.recover_expired_leases()
            results_worker_1.extend([r.task_id for r in recovered])

    def _worker_2_sweep():
        with SessionLocal() as db2:
            repo2 = TaskRepository(db2)
            recovered = repo2.recover_expired_leases()
            results_worker_2.extend([r.task_id for r in recovered])

    th1 = threading.Thread(target=_worker_1_sweep)
    th2 = threading.Thread(target=_worker_2_sweep)

    th1.start()
    th2.start()
    th1.join()
    th2.join()

    all_recovered = results_worker_1 + results_worker_2
    recovered_set = set(all_recovered)

    # Invariants:
    # 1. Exactly 10 tasks recovered
    assert len(all_recovered) == 10
    # 2. No duplicate recovery across workers (disjoint sets)
    assert len(recovered_set) == 10
    assert set(task_ids) == recovered_set
    assert set(results_worker_1).isdisjoint(set(results_worker_2))

    # 3. Tasks are now in RETRY_WAIT with cleared leases
    with SessionLocal() as db:
        tasks = list(db.scalars(select(Task).where(Task.id.in_(task_ids))).all())
        for t in tasks:
            assert t.status == TaskStatus.RETRY_WAIT.value
            assert t.lease_token is None
            assert t.retry_count == 1


def test_g831_t04_redis_failure_preserves_db_recovery(redis_conn, test_queue):
    """T04: Redis failure preservation test.
    If Redis reclamation raises an exception after PostgreSQL recovery has committed,
    the PostgreSQL transaction remains committed. The subsequent cycle safely retries reclaim.
    """
    past_time = get_utc_now() - timedelta(seconds=60)
    with SessionLocal() as db:
        repo = TaskRepository(db)
        task = repo.create_task(
            task_name="Redis Reclaim Failure Task",
            task_type="noop",
            payload={"fail_redis": True},
        )
        task.status = TaskStatus.RUNNING.value
        task.lease_token = "lease-exp-t04"
        task.lease_expires_at = past_time
        task_id = task.id
        db.commit()

    broker = RedisBroker(redis_conn, queue_name=test_queue)
    dispatcher = WorkerDispatcher(worker_id="test-dispatcher-t04", broker=broker)

    # Put task payload in Redis in-flight with an already-expired timestamp
    broker.enqueue(task_id, 2, {"task_id": task_id, "priority": 2})
    msg = broker.dequeue(timeout_seconds=0.1)
    assert msg is not None

    # Force expiration in Redis in_flight
    redis_conn.zadd(f"{test_queue}:in_flight", {msg["_raw_envelope"]: time.time() - 100})

    # Cycle 1: Simulate Redis failure during reclaim
    original_reclaim = broker.reclaim_expired

    def failing_reclaim(*args, **kwargs):
        raise redis.RedisError("Simulated Redis timeout during reclaim")

    broker.reclaim_expired = failing_reclaim

    with pytest.raises(redis.RedisError, match="Simulated Redis timeout"):
        dispatcher.run_recovery_cycle(visibility_timeout_seconds=0.1)

    # Invariant: DB recovery was committed despite Redis failure
    with SessionLocal() as db:
        t_after = db.get(Task, task_id)
        assert t_after.status == TaskStatus.RETRY_WAIT.value
        assert t_after.retry_count == 1
        assert t_after.lease_token is None

    # Cycle 2: Redis recovers; next cycle successfully reclaims the message
    broker.reclaim_expired = original_reclaim
    rec, rcl = dispatcher.run_recovery_cycle(visibility_timeout_seconds=0.1)
    assert rec == 0  # Already recovered in DB
    assert rcl == 1  # Reclaimed from Redis
    assert broker.get_queue_depth() == 1


def test_g831_t05_active_lease_rejection_withholds_dispatcher_ack(redis_conn, test_queue):
    """T05: Active lease rejection test (Dispatcher level).
    Worker A holds a valid unexpired PostgreSQL lease.
    Worker B receives a duplicate delivery from Redis.
    Worker B's execute_task returns OWNERSHIP_REJECTED.
    Dispatcher strictly withholds broker.ack(), preserving the message in Redis.
    """
    with SessionLocal() as db:
        repo = TaskRepository(db)
        task = repo.create_task(
            task_name="Active Leased Task",
            task_type="noop",
            payload={"protect": True},
        )
        task_id = task.id
        # Worker A acquires valid 60-second lease
        token_a = f"lease-active-a-{uuid.uuid4().hex}"
        assert repo.acquire_lease(task_id, "worker-A", lease_duration_seconds=60, lease_token=token_a)
        repo.update_status(task_id, TaskStatus.RUNNING, {"worker_id": "worker-A"}, lease_token=token_a)
        db.commit()

    broker = RedisBroker(redis_conn, queue_name=test_queue)
    broker.enqueue(task_id, 2, {"task_id": task_id, "priority": 2})

    # Worker B dispatcher pulls the delivery
    dispatcher_b = WorkerDispatcher(worker_id="worker-B", broker=broker)

    delivery = broker.dequeue(timeout_seconds=0.1)
    assert delivery is not None
    assert broker.get_in_flight_depth() == 1

    # Spy on broker.ack
    original_ack = broker.ack
    ack_calls = []

    def spy_ack(t_id, msg=None, delivery_token=None):
        ack_calls.append((t_id, delivery_token))
        return original_ack(t_id, msg, delivery_token=delivery_token)

    broker.ack = spy_ack

    try:
        # Worker B attempts execution of delivery
        dispatcher_b._task_wrapper(delivery)

        # Invariant 1: Dispatcher strictly did NOT ack
        assert len(ack_calls) == 0, f"Dispatcher erroneously acknowledged rejected delivery: {ack_calls}"

        # Invariant 2: Delivery remains in Redis in_flight
        assert broker.get_in_flight_depth() == 1

        # Invariant 3: Worker A's task in PostgreSQL remains untouched in RUNNING
        with SessionLocal() as db:
            t = db.get(Task, task_id)
            assert t.status == TaskStatus.RUNNING.value
            assert t.lease_token == token_a
    finally:
        broker.ack = original_ack


def test_g831_t06_crash_between_stages_recovers_on_subsequent_cycle(redis_conn, test_queue):
    """T06: Crash between recovery stages.
    Stage 1 completes and commits DB transition to RETRY_WAIT.
    The recovery worker process terminates/crashes before calling Redis reclaim.
    A subsequent recovery cycle successfully detects the expired Redis message and reclaims it.
    """
    past_time = get_utc_now() - timedelta(seconds=60)
    with SessionLocal() as db:
        repo = TaskRepository(db)
        task = repo.create_task(
            task_name="Stage Crash Task",
            task_type="noop",
            payload={"stage_crash": True},
        )
        task.status = TaskStatus.RUNNING.value
        task.lease_token = "lease-crash-t06"
        task.lease_expires_at = past_time
        task_id = task.id
        db.commit()

    broker = RedisBroker(redis_conn, queue_name=test_queue)
    broker.enqueue(task_id, 2, {"task_id": task_id, "priority": 2})
    msg = broker.dequeue(timeout_seconds=0.1)
    assert msg is not None

    # Expire Redis visibility
    redis_conn.zadd(f"{test_queue}:in_flight", {msg["_raw_envelope"]: time.time() - 100})

    # Worker 1 runs Stage 1 only, then crashes (abrupt termination before Stage 2)
    with SessionLocal() as db:
        repo = TaskRepository(db)
        recovered = repo.recover_expired_leases()
        assert len(recovered) == 1
        assert recovered[0].task_id == task_id

    # Simulated crash: Worker 1 dies here. Redis was not touched.
    assert broker.get_queue_depth() == 0
    assert broker.get_in_flight_depth() == 1

    # Worker 2 runs full recovery cycle
    dispatcher_2 = WorkerDispatcher(worker_id="worker-recovery-2", broker=broker)
    rec_count, rcl_count = dispatcher_2.run_recovery_cycle(visibility_timeout_seconds=0.1)

    # Invariants:
    # DB task was already in RETRY_WAIT, so rec_count == 0
    assert rec_count == 0
    # Redis delivery was safely reclaimed, so rcl_count == 1
    assert rcl_count == 1
    assert broker.get_queue_depth() == 1
    assert broker.get_in_flight_depth() == 0


def test_g831_t07_retryable_failure_is_not_acked_before_durable_retry_publication(
    redis_conn, test_queue, monkeypatch
):
    """T07: Retryable failure ACK withholding test.
    When a worker records a retryable failure without durable outbox publication,
    execute_task returns RETRY_PENDING_PUBLICATION.
    The dispatcher must NOT ACK the Redis delivery, ensuring delivery durability
    is preserved until transactional retry publication is confirmed.
    """
    with SessionLocal() as db:
        repo = TaskRepository(db)
        task = repo.create_task(
            task_name="Failing Retryable Task",
            task_type="failing",  # Built-in handler that raises an Exception
            payload={"will_fail": True},
            max_retries=3,
        )
        task_id = task.id
        db.commit()

    broker = RedisBroker(redis_conn, queue_name=test_queue)
    broker.enqueue(task_id, 2, {"task_id": task_id, "priority": 2})

    delivery = broker.dequeue(timeout_seconds=0.1)
    assert delivery is not None

    dispatcher = WorkerDispatcher(worker_id="worker-retry-t07", broker=broker)

    ack_calls = []
    original_ack = broker.ack

    def spy_ack(t_id, msg=None, delivery_token=None):
        ack_calls.append(t_id)
        return original_ack(t_id, msg, delivery_token=delivery_token)

    broker.ack = spy_ack

    try:
        from taskforge.workers.execution_engine import execute_task as orig_execute

        monkeypatch.setattr(
            "taskforge.workers.dispatcher.execute_task",
            lambda t_id, w_id, lease_manager=None: orig_execute(
                t_id, w_id, lease_manager=lease_manager, use_outbox=False
            ),
        )
        dispatcher._task_wrapper(delivery)

        # Invariant 1: Dispatcher strictly withheld ACK
        assert len(ack_calls) == 0, "Dispatcher erroneously acknowledged retryable failure before durable publication!"

        # Invariant 2: Task in DB is in RETRY_WAIT
        with SessionLocal() as db:
            t = db.get(Task, task_id)
            assert t.status == TaskStatus.RETRY_WAIT.value
            assert t.retry_count == 1

        # Invariant 3: Message remains in Redis in_flight
        assert broker.get_in_flight_depth() == 1
    finally:
        broker.ack = original_ack


def test_g831_t08_expired_running_cannot_bypass_recovery():
    """T08: Expired RUNNING lease acquisition rejection test.
    Under row lock in acquire_lease():
    1. A task in RUNNING with an unexpired lease cannot be acquired -> False.
    2. A task in RUNNING with an EXPIRED lease cannot be acquired directly -> False.
    3. Terminal tasks (SUCCESS, DEAD_LETTER, CANCELLED) cannot be acquired -> False.
    4. Only after recover_expired_leases() transitions the expired task to RETRY_WAIT
       can acquire_lease() succeed -> True.
    """
    past_time = get_utc_now() - timedelta(seconds=60)
    future_time = get_utc_now() + timedelta(seconds=60)

    with SessionLocal() as db:
        repo = TaskRepository(db)
        task = repo.create_task(
            task_name="State Boundary Task",
            task_type="noop",
            payload={"boundary": True},
        )
        task_id = task.id

        # 1. RUNNING with unexpired lease -> acquisition rejected
        task.status = TaskStatus.RUNNING.value
        task.lease_token = "lease-active"
        task.lease_expires_at = future_time
        db.commit()

        assert repo.acquire_lease(task_id, "worker-candidate-1") is False

        # 2. RUNNING with expired lease -> acquisition STILL rejected (cannot bypass recovery!)
        task.lease_expires_at = past_time
        db.commit()

        assert repo.acquire_lease(task_id, "worker-candidate-2") is False

        # 3. Terminal states -> acquisition rejected
        for term_status in (TaskStatus.SUCCESS, TaskStatus.DEAD_LETTER, TaskStatus.CANCELLED):
            task.status = term_status.value
            task.lease_token = None
            task.lease_expires_at = None
            db.commit()
            assert repo.acquire_lease(task_id, "worker-candidate-3") is False

        # 4. Now reset to RUNNING with expired lease, and run recover_expired_leases()
        task.status = TaskStatus.RUNNING.value
        task.lease_token = "lease-expired-bypass"
        task.lease_expires_at = past_time
        db.commit()

        recovered = repo.recover_expired_leases()
        assert len(recovered) == 1
        assert recovered[0].task_id == task_id
        assert recovered[0].status == TaskStatus.RETRY_WAIT.value

        # 5. Now that recovery committed RETRY_WAIT, acquire_lease() succeeds
        fresh_token = f"lease-new-{uuid.uuid4().hex}"
        assert repo.acquire_lease(task_id, "worker-healthy-replacement", lease_token=fresh_token) is True


def test_g831_t09_inconsistent_running_lease_metadata_rejected():
    """T09: Lease consistency test.
    Verifies that a task in RUNNING with inconsistent lease metadata
    (e.g. lease_token is None, but lease_expires_at is still in the future)
    is strictly rejected by acquire_lease().
    Only cleanly vacated leases (both lease_token is None AND lease_expires_at is None)
    are eligible for candidate acquisition.
    """
    now = get_utc_now()
    future_time = now + timedelta(seconds=120)

    with SessionLocal() as db:
        repo = TaskRepository(db)
        task = repo.create_task(
            task_name="Inconsistent Lease Task",
            task_type="noop",
            payload={"check": "inconsistent_metadata"},
        )
        task_id = task.id

        # Case 1: Inconsistent metadata (token None, but lease_expires_at in future)
        task.status = TaskStatus.RUNNING.value
        task.lease_token = None
        task.lease_expires_at = future_time
        db.commit()

        assert repo.acquire_lease(task_id, "worker-candidate-t09") is False, (
            "acquire_lease() permitted acquisition on inconsistent lease metadata (token None, expires_at in future)!"
        )

        # Case 2: Inconsistent metadata (token present, but lease_expires_at is None)
        task.lease_token = "lease-orphan-token"
        task.lease_expires_at = None
        db.commit()

        assert repo.acquire_lease(task_id, "worker-candidate-t09") is False, (
            "acquire_lease() permitted acquisition on inconsistent lease metadata (token present, expires_at None)!"
        )

        # Case 3: Cleanly vacated lease (both token and expires_at are None)
        task.lease_token = None
        task.lease_expires_at = None
        db.commit()

        assert repo.acquire_lease(task_id, "worker-candidate-t09") is True, (
            "acquire_lease() failed to claim cleanly vacated RUNNING task!"
        )


def test_g831_t10_retry_timing_and_scheduled_at_reclaim_guard(redis_conn, test_queue):
    """T10: Retry timing & scheduled_at guard test.
    Verifies that:
    1. A task with scheduled_at in the future strictly rejects acquire_lease(),
       preventing premature execution if a delivery is dequeued or reclaimed early.
    2. Once scheduled_at has arrived, acquire_lease() succeeds.
    3. Documented failover policy: Lease-expiry recovery uses immediate retry
       (scheduled_at is None) to minimize failover delay after worker death.
    """
    now = get_utc_now()
    future_scheduled = now + timedelta(seconds=60)

    with SessionLocal() as db:
        repo = TaskRepository(db)
        task = repo.create_task(
            task_name="Scheduled Delayed Task",
            task_type="noop",
            payload={"timing": "delayed"},
        )
        task_id = task.id

        # Task in RETRY_WAIT with future scheduled_at
        task.status = TaskStatus.RETRY_WAIT.value
        task.scheduled_at = future_scheduled
        task.lease_token = None
        task.lease_expires_at = None
        db.commit()

        # Invariant 1: Premature lease acquisition is strictly rejected
        assert repo.acquire_lease(task_id, "worker-premature") is False, (
            "acquire_lease() allowed premature claim before scheduled_at!"
        )

        # Invariant 2: When scheduled_at arrives (or is elapsed), lease acquisition succeeds
        task.scheduled_at = now - timedelta(seconds=1)
        db.commit()

        assert repo.acquire_lease(task_id, "worker-ontime") is True, (
            "acquire_lease() rejected claim after scheduled_at elapsed!"
        )

        # Invariant 3: Lease-expiry recovery sets scheduled_at to None (immediate failover policy)
        task.status = TaskStatus.RUNNING.value
        task.lease_token = "lease-crashed-worker"
        task.lease_expires_at = now - timedelta(seconds=30)
        task.scheduled_at = None
        db.commit()

        recovered = repo.recover_expired_leases()
        assert len(recovered) == 1
        assert recovered[0].task_id == task_id

        # Verify recovered task has scheduled_at None (immediate retry policy)
        db.refresh(task)
        assert task.scheduled_at is None
        assert task.status == TaskStatus.RETRY_WAIT.value
        # Replacement worker can claim immediately without backoff delay
        assert repo.acquire_lease(task_id, "worker-replacement-immediate") is True


def test_g831_t11_execution_mode_parity_and_dispatcher_ack_contract(redis_conn, test_queue, monkeypatch):
    """T11: Execution mode parity & Dispatcher ACK contract test.
    Verifies that:
    1. Both thread-based and process-isolated modes return ExecutionDisposition.
    2. Dispatcher applies the exact same ACK policy to both modes:
       - ACKs COMPLETED
       - Strictly withholds ACK on OWNERSHIP_REJECTED
       - Strictly withholds ACK on RETRY_PENDING_PUBLICATION
    """
    broker = RedisBroker(redis_conn, queue_name=test_queue)

    # 1. Thread mode dispatcher
    disp_thread = WorkerDispatcher(worker_id="disp-thread-t11", broker=broker, execution_mode="thread")
    # 2. Process mode dispatcher
    disp_proc = WorkerDispatcher(worker_id="disp-proc-t11", broker=broker, execution_mode="process")

    # Spy on broker.ack
    ack_history = []
    original_ack = broker.ack

    def recording_ack(t_id, msg=None, delivery_token=None):
        ack_history.append((t_id, delivery_token))
        return original_ack(t_id, msg, delivery_token=delivery_token)

    broker.ack = recording_ack

    try:
        # Case A: Thread mode COMPLETED -> Dispatcher ACKs
        with SessionLocal() as db:
            repo = TaskRepository(db)
            task_t = repo.create_task(
                task_name="Thread Success",
                task_type="noop",
                payload={"mode": "thread"},
            )
            task_t_id = task_t.id

        broker.enqueue(task_t_id, 2, {"task_id": task_t_id, "priority": 2})
        deliv_t = broker.dequeue(timeout_seconds=0.1)
        assert deliv_t is not None

        disp_thread._task_wrapper(deliv_t)
        assert len(ack_history) == 1
        assert ack_history[0][0] == task_t_id

        # Case B: Process-isolated COMPLETED -> Dispatcher ACKs
        with SessionLocal() as db:
            repo = TaskRepository(db)
            task_a = repo.create_task(
                task_name="Process Isolated Success",
                task_type="noop",
                payload={"mode": "process"},
                timeout_seconds=10,
            )
            task_a_id = task_a.id

        broker.enqueue(task_a_id, 2, {"task_id": task_a_id, "priority": 2, "timeout_seconds": 10})
        deliv_a = broker.dequeue(timeout_seconds=0.1)
        assert deliv_a is not None

        disp_proc._task_wrapper(deliv_a)
        assert len(ack_history) == 2
        assert ack_history[1][0] == task_a_id

        # Case B: Process-isolated OWNERSHIP_REJECTED -> Dispatcher withholds ACK
        with SessionLocal() as db:
            repo = TaskRepository(db)
            task_b = repo.create_task(
                task_name="Process Ownership Collision",
                task_type="noop",
                payload={"mode": "process"},
                timeout_seconds=10,
            )
            task_b_id = task_b.id
            # Worker A holds unexpired lease
            repo.acquire_lease(task_b_id, "worker-prior-owner", lease_duration_seconds=60)

        broker.enqueue(task_b_id, 2, {"task_id": task_b_id, "priority": 2, "timeout_seconds": 10})
        deliv_b = broker.dequeue(timeout_seconds=0.1)
        assert deliv_b is not None

        # Candidate process dispatcher attempts execution
        disp_proc._task_wrapper(deliv_b)
        # ACK must NOT have been called for task_b
        assert all(t[0] != task_b_id for t in ack_history), (
            "Dispatcher erroneously acknowledged OWNERSHIP_REJECTED delivery in process mode!"
        )

        # Case C: Process-isolated RETRY_PENDING_PUBLICATION -> Dispatcher withholds ACK
        from taskforge.workers.execution_engine import (
            execute_task_process_isolated as orig_proc_exec,
        )

        monkeypatch.setattr(
            "taskforge.workers.dispatcher.execute_task_process_isolated",
            lambda t_id, w_id, timeout_seconds=60, lease_manager=None: orig_proc_exec(
                t_id, w_id, timeout_seconds=timeout_seconds, lease_manager=lease_manager, use_outbox=False
            ),
        )
        with SessionLocal() as db:
            repo = TaskRepository(db)
            task_c = repo.create_task(
                task_name="Process Timeout Retryable",
                task_type="sleep",
                payload={"duration": 10},
                timeout_seconds=1,  # Forces timeout
                max_retries=3,
            )
            task_c_id = task_c.id

        broker.enqueue(task_c_id, 2, {"task_id": task_c_id, "priority": 2, "timeout_seconds": 1})
        deliv_c = broker.dequeue(timeout_seconds=0.1)
        assert deliv_c is not None

        disp_proc._task_wrapper(deliv_c)
        # ACK must NOT have been called for task_c
        assert all(t[0] != task_c_id for t in ack_history), (
            "Dispatcher erroneously acknowledged RETRY_PENDING_PUBLICATION delivery in process mode!"
        )

    finally:
        broker.ack = original_ack
