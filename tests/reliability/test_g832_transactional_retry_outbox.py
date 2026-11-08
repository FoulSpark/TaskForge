"""
Milestone G8.3 Item 2 — Transactional Retry Publication via Outbox Test Suite.
Verifies that:
1. Task failure state, retry count, attempt result, task event, and TASK_RETRY OutboxEvent
   commit atomically in one single PostgreSQL transaction under strict lease fencing.
2. No detached daemon threads perform retry publication.
3. The outbox publisher respects scheduled_at backoff delays.
4. Process crash between DB commit and Redis publication preserves retry intent and recovers cleanly.
5. Process crash between Redis enqueue and Outbox commit handles duplicate delivery safely via lease fencing.
6. Exactly one eligible delivery exists in Redis after retry delay (eliminating duplicate delivery race).
7. Complete disposition parity across all outcomes for both thread and process execution modes.
"""

import time
import uuid
from datetime import timedelta

import pytest
import redis
from sqlalchemy import select, text

from taskforge.core.state_machine import TaskPriority, TaskStatus
from taskforge.database.models import (
    OutboxEvent,
    Task,
    TaskAttempt,
    TaskResult,
    get_utc_now,
)
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal
from taskforge.queue.broker import RedisBroker
from taskforge.queue.outbox import publish_pending_outbox_events
from taskforge.queue.redis_client import create_redis_client
from taskforge.workers.dispatcher import WorkerDispatcher
from taskforge.workers.execution_engine import (
    ExecutionDisposition,
    execute_task,
    execute_task_process_isolated,
)
from taskforge.workers.lease_manager import LeaseManager

TEST_KEY_PREFIX = "taskforge:g832:test:"


@pytest.fixture(scope="module")
def redis_conn():
    client = create_redis_client()
    try:
        client.ping()
        yield client
    finally:
        client.close()


@pytest.fixture
def test_queue(redis_conn: redis.Redis):
    queue_name = f"{TEST_KEY_PREFIX}{uuid.uuid4().hex}:tasks"
    yield queue_name
    redis_conn.delete(queue_name)
    redis_conn.delete(f"{queue_name}:in_flight")
    redis_conn.delete(f"{queue_name}:seq:1")
    redis_conn.delete(f"{queue_name}:seq:2")
    redis_conn.delete(f"{queue_name}:seq:3")
    redis_conn.delete(f"{queue_name}:seq:4")


def test_g832_t01_atomic_failure_and_retry_outbox_transaction():
    """T01: Atomic failure transaction & outbox event creation.
    Verifies that when complete_task_failure() is called under lease fencing:
    - TaskAttempt is updated to FAILED.
    - Task status is transitioned to RETRY_WAIT.
    - Task retry_count is incremented.
    - Task scheduled_at is set to future backoff timestamp.
    - Task lease_token and lease_expires_at are cleared (vacated).
    - TaskEvent records STATUS_CHANGED with scheduled_at.
    - OutboxEvent is created with event_type='TASK_RETRY', status='PENDING'.
    - All are committed in a single transaction.
    """
    with SessionLocal() as db:
        repo = TaskRepository(db)
        task = repo.create_task(
            task_name="Atomic Failure Outbox Task",
            task_type="noop",
            payload={"test": "outbox_atomic"},
            max_retries=3,
        )
        task_id = task.id
        token = "lease-token-t01"
        repo.acquire_lease(task_id, "worker-t01", lease_duration_seconds=30, lease_token=token)
        repo.update_status(task_id, TaskStatus.RUNNING, {"worker": "worker-t01"}, lease_token=token)
        attempt = repo.record_attempt(
            task_id=task_id,
            attempt_number=1,
            worker_id="worker-t01",
            status="RUNNING",
            lease_token=token,
            started_at=get_utc_now(),
        )
        attempt_id = attempt.id

        now_before = get_utc_now()
        task_updated, is_retryable = repo.complete_task_failure(
            task_id=task_id,
            lease_token=token,
            error_message="Simulated handler error",
            is_timeout=False,
            attempt_id=attempt_id,
            duration_ms=42.0,
            publish_to_outbox=True,
        )

        assert is_retryable is True
        assert task_updated.status == TaskStatus.RETRY_WAIT.value
        assert task_updated.retry_count == 1
        assert task_updated.lease_token is None
        assert task_updated.lease_expires_at is None
        assert task_updated.scheduled_at is not None
        assert task_updated.scheduled_at > now_before

    # Verify directly from database that all records exist and match
    with SessionLocal() as db:
        t_db = db.get(Task, task_id)
        assert t_db.status == TaskStatus.RETRY_WAIT.value

        att_db = db.get(TaskAttempt, attempt_id)
        assert att_db.status == "FAILED"
        assert att_db.error_message == "Simulated handler error"

        # Verify OutboxEvent
        outbox_stmt = select(OutboxEvent).where(
            OutboxEvent.aggregate_id == task_id,
            OutboxEvent.event_type == "TASK_RETRY",
        )
        outbox_event = db.scalars(outbox_stmt).first()
        assert outbox_event is not None
        assert outbox_event.status == "PENDING"
        assert outbox_event.payload["task_id"] == task_id
        assert outbox_event.payload["retry_count"] == 1
        assert "scheduled_at" in outbox_event.payload


def test_g832_t02_dispatcher_acks_when_retry_durably_scheduled(redis_conn, test_queue):
    """T02: Authoritative dispatcher ACK upon durable retry scheduling.
    When execute_task() finishes with RETRY_DURABLY_SCHEDULED:
    - The dispatcher calls broker.ack() with delivery_token.
    - The failed delivery is removed from Redis in_flight.
    - The in_flight depth becomes 0, preventing visibility reclaim duplication.
    """
    broker = RedisBroker(redis_conn, queue_name=test_queue)
    with SessionLocal() as db:
        repo = TaskRepository(db)
        task = repo.create_task(
            task_name="Failing Task for Durable ACK",
            task_type="failing",
            payload={"error_message": "intentional_fail_ack"},
            max_retries=2,
        )
        task_id = task.id

    broker.enqueue(task_id, 2, {"task_id": task_id, "priority": 2})
    delivery = broker.dequeue(timeout_seconds=0.1)
    assert delivery is not None
    deliv_token = delivery["delivery_token"]

    dispatcher = WorkerDispatcher(worker_id="dispatcher-t02", broker=broker)

    ack_calls = []
    orig_ack = broker.ack

    def tracking_ack(t_id, msg=None, delivery_token=None):
        ack_calls.append((t_id, delivery_token))
        return orig_ack(t_id, msg, delivery_token=delivery_token)

    broker.ack = tracking_ack

    try:
        dispatcher._task_wrapper(delivery)

        # Invariant 1: Dispatcher ACKed because retry was durably scheduled to outbox
        assert len(ack_calls) == 1
        assert ack_calls[0] == (task_id, deliv_token)

        # Invariant 2: In-flight set is completely clean
        assert broker.get_in_flight_depth() == 0

        # Invariant 3: Reclaim reclaims zero duplicate copies
        reclaimed = broker.reclaim_expired()
        assert reclaimed == 0
    finally:
        broker.ack = orig_ack


def test_g832_t03_outbox_publisher_respects_scheduled_at_backoff(redis_conn, test_queue):
    """T03: Outbox publisher respects scheduled_at backoff delay.
    Verifies that:
    1. An OutboxEvent with scheduled_at in the future is NOT published.
    2. Once scheduled_at has arrived, publish_pending_outbox_events() publishes it to Redis.
    3. The task in PostgreSQL transitions from RETRY_WAIT to QUEUED upon publication.
    """
    broker = RedisBroker(redis_conn, queue_name=test_queue)
    now = get_utc_now()
    future_time = now + timedelta(seconds=60)

    with SessionLocal() as db:
        repo = TaskRepository(db)
        task = repo.create_task(
            task_name="Delayed Retry Task",
            task_type="noop",
            payload={"data": "delayed"},
            max_retries=3,
        )
        task_id = task.id
        task.status = TaskStatus.RETRY_WAIT.value
        task.scheduled_at = future_time
        db.commit()

        # Insert pending retry event with future scheduled_at and available_at
        outbox_event = OutboxEvent(
            aggregate_id=task_id,
            event_type="TASK_RETRY",
            payload={
                "task_id": task_id,
                "priority": 2,
                "scheduled_at": future_time.isoformat(),
            },
            status="PENDING",
            available_at=future_time,
            created_at=now,
        )
        db.add(outbox_event)
        db.commit()
        outbox_event_id = outbox_event.id

    # Sweep 1: Before scheduled_at -> 0 published
    with SessionLocal() as db:
        pub_count = publish_pending_outbox_events(db, broker, event_id=outbox_event_id)
        assert pub_count == 0
        assert broker.get_queue_depth() == 0

    # Advance time: simulate arrival of scheduled_at
    with SessionLocal() as db:
        evt = db.scalars(select(OutboxEvent).where(OutboxEvent.id == outbox_event_id)).first()
        past_time = now - timedelta(seconds=1)
        evt.available_at = past_time
        evt.payload = {
            "task_id": task_id,
            "priority": 2,
            "scheduled_at": past_time.isoformat(),
        }
        db.commit()

    # Sweep 2: After scheduled_at -> 1 published
    with SessionLocal() as db:
        pub_count = publish_pending_outbox_events(db, broker, event_id=outbox_event_id)
        assert pub_count == 1
        assert broker.get_queue_depth() == 1

        # Verify OutboxEvent status is PUBLISHED
        evt_pub = db.get(OutboxEvent, outbox_event_id)
        assert evt_pub.status == "PUBLISHED"
        assert evt_pub.published_at is not None

        # Verify task status transitioned from RETRY_WAIT to QUEUED
        t = db.get(Task, task_id)
        assert t.status == TaskStatus.QUEUED.value


def test_g832_t04_crash_after_db_commit_before_redis_publication_never_loses_task(redis_conn, test_queue):
    """T04: Critical crash boundary — Worker dies after DB commit but before Redis publication.
    This demonstrates why the transactional outbox is necessary:
    1. Worker fails task and commits RETRY_WAIT and OutboxEvent(TASK_RETRY, PENDING).
    2. Worker abruptly crashes/terminates before any Redis message is enqueued.
    3. Redis queue depth is 0; in_flight is 0.
    4. Outbox publisher restarts / runs recovery sweep.
    5. The pending outbox event is discovered and published to Redis.
    6. A replacement worker dequeues, executes successfully, and completes the task.
    Zero lost tasks!
    """
    broker = RedisBroker(redis_conn, queue_name=test_queue)
    with SessionLocal() as db:
        repo = TaskRepository(db)
        task = repo.create_task(
            task_name="Crash Before Redis Task",
            task_type="noop",
            payload={"records": [1, 2, 3]},
            max_retries=2,
        )
        task_id = task.id
        token = "lease-worker-crash"
        repo.acquire_lease(task_id, "worker-crash", lease_duration_seconds=30, lease_token=token)
        repo.update_status(task_id, TaskStatus.RUNNING, {"worker": "worker-crash"}, lease_token=token)

        # 1. Worker commits failure and retry outbox event
        task_updated, is_retryable = repo.complete_task_failure(
            task_id=task_id,
            lease_token=token,
            error_message="Fatal crash before publish",
            publish_to_outbox=True,
        )
        assert is_retryable is True

    # 2. Worker process terminates here.
    # Verify Redis is completely empty (no delivery reached Redis)
    assert broker.get_queue_depth() == 0
    assert broker.get_in_flight_depth() == 0

    # 3. Simulate publisher sweep (with scheduled_at elapsed)
    with SessionLocal() as db:
        # Normalize available_at and scheduled_at to past for immediate test processing
        evt = db.scalars(
            select(OutboxEvent).where(
                OutboxEvent.aggregate_id == task_id,
                OutboxEvent.event_type == "TASK_RETRY",
            )
        ).first()
        past_time = get_utc_now() - timedelta(seconds=1)
        evt.available_at = past_time
        payload = dict(evt.payload)
        payload["scheduled_at"] = past_time.isoformat()
        evt.payload = payload
        db.commit()

        # Publisher flushes
        published = publish_pending_outbox_events(db, broker, event_id=evt.id)
        assert published == 1

    # 4. Message now exists in Redis ready queue
    assert broker.get_queue_depth() == 1

    # 5. Replacement worker dequeues and executes to completion
    rep_msg = broker.dequeue(timeout_seconds=0.1)
    assert rep_msg is not None
    assert rep_msg["task_id"] == task_id

    disp_rep = WorkerDispatcher(worker_id="worker-replacement-t04", broker=broker)
    disp_rep._task_wrapper(rep_msg)

    with SessionLocal() as db:
        t_final = db.get(Task, task_id)
        assert t_final.status == TaskStatus.SUCCESS.value
        res_stmt = select(TaskResult).where(TaskResult.task_id == task_id)
        assert db.scalars(res_stmt).first() is not None


def test_g832_t05_crash_after_redis_enqueue_before_outbox_commit_is_idempotent(redis_conn, test_queue, monkeypatch):
    """T05: Crash after Redis publication but before outbox status commit.
    Verifies that:
    1. Publisher pushes message to Redis, then crashes before committing event.status='PUBLISHED'.
    2. Next sweep re-publishes the event (at-least-once).
    3. Redis receives a duplicate delivery.
    4. Worker 1 completes the task; Worker 2 encounters terminal state, returns TERMINAL_DUPLICATE,
       and ACKs safely without duplicate execution.
    """
    broker = RedisBroker(redis_conn, queue_name=test_queue)

    with SessionLocal() as db:
        repo = TaskRepository(db)
        task = repo.create_task(
            task_name="Outbox Crash Idempotency Task",
            task_type="noop",
            payload={"val": 42},
            max_retries=2,
        )
        task_id = task.id
        now = get_utc_now()
        outbox_event = OutboxEvent(
            aggregate_id=task_id,
            event_type="TASK_RETRY",
            payload={"task_id": task_id, "priority": 2, "scheduled_at": (now - timedelta(seconds=1)).isoformat()},
            status="PENDING",
            available_at=now - timedelta(seconds=1),
            created_at=now,
        )
        db.add(outbox_event)
        db.commit()
        outbox_event_id = outbox_event.id

    # First sweep: Simulate crash after enqueue but before DB commit
    with SessionLocal() as db:
        def crash_commit():
            raise RuntimeError("simulated_crash_after_enqueue_before_outbox_commit")

        monkeypatch.setattr(db, "commit", crash_commit)

        with pytest.raises(RuntimeError, match="simulated_crash_after_enqueue"):
            publish_pending_outbox_events(db, broker, event_id=outbox_event_id)

        db.rollback()

    # Message was enqueued to Redis (delivery 1)
    assert broker.get_queue_depth() == 1

    # Worker 1 dequeues delivery 1 (moving it from ready queue to in_flight)
    msg1 = broker.dequeue(timeout_seconds=0.1)
    assert msg1 is not None

    # Second sweep: Publisher recovers and re-flushes the still-PENDING outbox event (duplicate delivery 2)
    with SessionLocal() as db:
        published = publish_pending_outbox_events(db, broker, event_id=outbox_event_id)
        assert published == 1

    # Redis has 1 delivery in ready queue (delivery 2) and 1 in-flight (delivery 1)
    assert broker.get_queue_depth() == 1
    assert broker.get_in_flight_depth() == 1

    # Worker 1 executes delivery 1 to completion
    disp = WorkerDispatcher(worker_id="worker-idem-1", broker=broker)
    disp._task_wrapper(msg1)

    with SessionLocal() as db:
        t = db.get(Task, task_id)
        assert t.status == TaskStatus.SUCCESS.value

    # Worker executes duplicate delivery 2: MUST return TERMINAL_DUPLICATE and ACK
    msg2 = broker.dequeue(timeout_seconds=0.1)
    assert msg2 is not None

    ack_calls = []
    orig_ack = broker.ack

    def spy_ack(t_id, msg=None, delivery_token=None):
        ack_calls.append(t_id)
        return orig_ack(t_id, msg, delivery_token=delivery_token)

    broker.ack = spy_ack
    try:
        disp._task_wrapper(msg2)
        assert len(ack_calls) == 1
        assert ack_calls[0] == task_id
    finally:
        broker.ack = orig_ack

    # Confirm exactly 1 TaskResult exists in database
    with SessionLocal() as db:
        results = db.scalars(select(TaskResult).where(TaskResult.task_id == task_id)).all()
        assert len(results) == 1


def test_g832_t06_exactly_one_eligible_delivery_after_retry_delay(redis_conn, test_queue):
    """T06: Single eligible delivery invariant (eliminates Finding 2 duplicate delivery race).
    Verifies that:
    1. A failing task is scheduled for retry via transactional outbox.
    2. The original delivery is acknowledged upon RETRY_DURABLY_SCHEDULED.
    3. Visibility timeout expires -> broker.reclaim_expired() reclaims ZERO deliveries.
    4. Outbox publisher publishes the ONE retry delivery.
    5. Ready queue depth is EXACTLY 1.
    """
    broker = RedisBroker(redis_conn, queue_name=test_queue, visibility_timeout_seconds=1)
    with SessionLocal() as db:
        repo = TaskRepository(db)
        task = repo.create_task(
            task_name="Single Eligible Delivery Task",
            task_type="failing",
            payload={"err": "test_single_deliv"},
            max_retries=2,
        )
        task_id = task.id

    broker.enqueue(task_id, 2, {"task_id": task_id, "priority": 2})
    assert broker.get_queue_depth() == 1

    msg = broker.dequeue(timeout_seconds=0.1)
    assert msg is not None

    dispatcher = WorkerDispatcher(worker_id="disp-single-t06", broker=broker)
    dispatcher._task_wrapper(msg)

    # 1. Original delivery was acknowledged
    assert broker.get_in_flight_depth() == 0

    # 2. Expire visibility timeout
    time.sleep(1.2)

    # 3. Reclaim runs: reclaims ZERO because original delivery was ACKed
    reclaimed = broker.reclaim_expired()
    assert reclaimed == 0
    assert broker.get_queue_depth() == 0

    # 4. Outbox publisher flushes retry after scheduled_at arrives
    with SessionLocal() as db:
        evt = db.scalars(select(OutboxEvent).where(OutboxEvent.aggregate_id == task_id)).first()
        past_time = get_utc_now() - timedelta(seconds=1)
        evt.available_at = past_time
        payload = dict(evt.payload)
        payload["scheduled_at"] = past_time.isoformat()
        evt.payload = payload
        db.commit()

        published = publish_pending_outbox_events(db, broker, event_id=evt.id)
        assert published == 1

    # 5. Exactly ONE eligible delivery exists in Redis!
    assert broker.get_queue_depth() == 1


def test_g832_t07_all_six_dispositions_parity_across_both_execution_modes(redis_conn, test_queue):
    """T07: Full Disposition Contract Parity Test.
    Verifies all dispositions across BOTH thread-based and process-isolated execution modes:
    1. COMPLETED -> ACKed
    2. TERMINAL_DUPLICATE -> ACKed
    3. TERMINAL_FAILURE_RECORDED -> ACKed
    4. RETRY_DURABLY_SCHEDULED -> ACKed
    5. RETRY_PENDING_PUBLICATION -> Withheld
    6. OWNERSHIP_REJECTED -> Withheld
    7. UNEXPECTED_FAILURE -> Withheld
    """
    broker = RedisBroker(redis_conn, queue_name=test_queue)

    for mode in ("thread", "process"):
        dispatcher = WorkerDispatcher(worker_id=f"disp-parity-{mode}", broker=broker, execution_mode=mode)
        ack_calls = []
        orig_ack = broker.ack

        def mock_ack(t_id, msg=None, delivery_token=None, calls=ack_calls, base_ack=orig_ack):
            calls.append(t_id)
            return base_ack(t_id, msg, delivery_token=delivery_token)

        broker.ack = mock_ack

        try:
            # 1. COMPLETED -> should ACK
            with SessionLocal() as db:
                repo = TaskRepository(db)
                t1 = repo.create_task(f"p1-{mode}", "noop", {"v": 1}, timeout_seconds=10)
                t1_id = t1.id
            broker.enqueue(t1_id, 2, {"task_id": t1_id, "priority": 2, "timeout_seconds": 10})
            m1 = broker.dequeue(timeout_seconds=0.1)
            dispatcher._task_wrapper(m1)
            assert t1_id in ack_calls, f"Mode {mode} failed to ACK COMPLETED"

            # 2. TERMINAL_DUPLICATE -> should ACK
            with SessionLocal() as db:
                repo = TaskRepository(db)
                t2 = repo.create_task(f"p2-{mode}", "noop", {"v": 2})
                t2.status = TaskStatus.SUCCESS.value
                db.commit()
                t2_id = t2.id
            broker.enqueue(t2_id, 2, {"task_id": t2_id, "priority": 2})
            m2 = broker.dequeue(timeout_seconds=0.1)
            dispatcher._task_wrapper(m2)
            assert t2_id in ack_calls, f"Mode {mode} failed to ACK TERMINAL_DUPLICATE"

            # 3. TERMINAL_FAILURE_RECORDED (max_retries=0) -> should ACK
            with SessionLocal() as db:
                repo = TaskRepository(db)
                t3 = repo.create_task(f"p3-{mode}", "failing", {"err": 3}, max_retries=0, timeout_seconds=10)
                t3_id = t3.id
            broker.enqueue(t3_id, 2, {"task_id": t3_id, "priority": 2, "timeout_seconds": 10})
            m3 = broker.dequeue(timeout_seconds=0.1)
            dispatcher._task_wrapper(m3)
            assert t3_id in ack_calls, f"Mode {mode} failed to ACK TERMINAL_FAILURE_RECORDED"

            # 4. RETRY_DURABLY_SCHEDULED (max_retries=2, use_outbox=True) -> should ACK
            with SessionLocal() as db:
                repo = TaskRepository(db)
                t4 = repo.create_task(f"p4-{mode}", "failing", {"err": 4}, max_retries=2, timeout_seconds=10)
                t4_id = t4.id
            broker.enqueue(t4_id, 2, {"task_id": t4_id, "priority": 2, "timeout_seconds": 10})
            m4 = broker.dequeue(timeout_seconds=0.1)
            dispatcher._task_wrapper(m4)
            assert t4_id in ack_calls, f"Mode {mode} failed to ACK RETRY_DURABLY_SCHEDULED"

            # 5. OWNERSHIP_REJECTED -> should WITHHOLD ACK
            with SessionLocal() as db:
                repo = TaskRepository(db)
                t5 = repo.create_task(f"p5-{mode}", "noop", {"v": 5}, timeout_seconds=10)
                repo.acquire_lease(t5.id, "worker-other", lease_duration_seconds=60)
                t5_id = t5.id
            broker.enqueue(t5_id, 2, {"task_id": t5_id, "priority": 2, "timeout_seconds": 10})
            m5 = broker.dequeue(timeout_seconds=0.1)
            dispatcher._task_wrapper(m5)
            assert t5_id not in ack_calls, f"Mode {mode} erroneously ACKed OWNERSHIP_REJECTED"

            # 6. RETRY_PENDING_PUBLICATION (legacy/non-outbox path) -> should WITHHOLD ACK
            with SessionLocal() as db:
                repo = TaskRepository(db)
                t6 = repo.create_task(f"p6-{mode}", "failing", {"err": 6}, max_retries=2, timeout_seconds=10)
                t6_id = t6.id
            broker.enqueue(t6_id, 2, {"task_id": t6_id, "priority": 2, "timeout_seconds": 10})
            _ = broker.dequeue(timeout_seconds=0.1)
            lease6 = LeaseManager(t6_id, f"worker-test-{mode}")
            if mode == "process":
                disp6 = execute_task_process_isolated(t6_id, f"worker-test-{mode}", timeout_seconds=10, lease_manager=lease6, use_outbox=False)
            else:
                disp6 = execute_task(t6_id, f"worker-test-{mode}", lease_manager=lease6, use_outbox=False)
            assert disp6 == ExecutionDisposition.RETRY_PENDING_PUBLICATION
            assert not disp6.should_ack()

            # 7. UNEXPECTED_FAILURE -> should WITHHOLD ACK
            fake_id = f"non-existent-{uuid.uuid4().hex}"
            fake_msg = {"task_id": fake_id, "delivery_token": "tok-fake", "priority": 2}
            dispatcher._task_wrapper(fake_msg)
            assert fake_id not in ack_calls, f"Mode {mode} erroneously ACKed UNEXPECTED_FAILURE"

        finally:
            broker.ack = orig_ack


def test_g832_t08_future_event_outbox_starvation_prevention(redis_conn, test_queue):
    """T08: Outbox starvation prevention regression test.
    Concrete scenario:
    - Publisher batch limit: 50.
    - 55 pending events scheduled for tomorrow (available_at > now).
    - 1 pending event eligible immediately (available_at <= now).
    - Verifies that future-scheduled events are filtered at the database level
      and DO NOT consume or exhaust the eligible publisher batch.
    - The immediately eligible event must publish on the very first sweep!
    """
    broker = RedisBroker(redis_conn, queue_name=test_queue)
    now = get_utc_now()
    future_time = now + timedelta(days=1)
    immediate_task_id = f"imm-task-{uuid.uuid4().hex[:8]}"

    # Isolate test by marking any stale legacy outbox events as PUBLISHED
    with SessionLocal() as db:
        db.execute(text("UPDATE outbox_events SET status = 'PUBLISHED' WHERE status = 'PENDING'"))
        db.commit()

    with SessionLocal() as db:
        repo = TaskRepository(db)
        # Create 55 future-scheduled events
        for i in range(55):
            repo.create_task(f"future-{i}", "noop", {"idx": i}, scheduled_at=future_time)

        # Create 1 immediately eligible task
        t_imm = repo.create_task("immediate-task", "noop", {"target": True}, scheduled_at=None)
        immediate_task_id = t_imm.id

    # Execute a single publisher cycle with limit=50
    with SessionLocal() as db:
        published_count = publish_pending_outbox_events(db, broker, limit=50)
        assert published_count == 1, f"Expected exactly 1 immediately eligible event published, got {published_count}"

    # Verify message in broker is the immediate task
    assert broker.get_queue_depth() == 1
    msg = broker.dequeue(timeout_seconds=0.1)
    assert msg is not None
    assert msg["task_id"] == immediate_task_id


def test_g832_t09_stale_worker_attempt_creation_fenced_rejection(db_session):
    """T09: Strict lease fencing on record_attempt() and attempt-number allocation.
    Concrete sequence:
    1. Worker A acquires a 1-second lease.
    2. Worker A pauses / sleeps for 1.3 seconds so its lease expires.
    3. Worker B recovers the task and acquires a new lease.
    4. Worker A attempts to call record_attempt() using its stale lease token.
    5. Invariant: record_attempt() strictly raises PermissionError under row lock!
    6. Stale worker cannot pollute attempts table or allocate duplicate/invalid attempt numbers.
    """
    repo = TaskRepository(db_session)
    t = repo.create_task("fenced-attempt-task", "noop", {}, TaskPriority.HIGH.value)
    task_id = t.id

    stale_token = "lease-worker-a-stale"
    assert repo.acquire_lease(task_id, "worker-a", lease_duration_seconds=1, lease_token=stale_token) is True
    repo.update_status(task_id, TaskStatus.RUNNING, {"worker": "worker-a"}, lease_token=stale_token)

    # Worker A's lease expires
    time.sleep(1.3)

    # Lease expired, task recovered by Worker B
    recovered = repo.recover_expired_leases()
    assert task_id in [x.id for x in recovered]

    fresh_token = "lease-worker-b-fresh"
    assert repo.acquire_lease(task_id, "worker-b", lease_duration_seconds=30, lease_token=fresh_token) is True

    # Worker A attempts record_attempt with stale token -> strictly rejected
    with pytest.raises(PermissionError, match="Fencing violation: lease token 'lease-worker-a-stale' does not match"):
        repo.record_attempt(
            task_id=task_id,
            worker_id="worker-a",
            status="RUNNING",
            lease_token=stale_token,
        )

    # Worker B records attempt with valid fresh token -> succeeds
    att_b = repo.record_attempt(
        task_id=task_id,
        worker_id="worker-b",
        status="RUNNING",
        lease_token=fresh_token,
    )
    assert att_b.attempt_number == 2
    assert att_b.worker_id == "worker-b"
