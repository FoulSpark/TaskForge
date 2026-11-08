import json
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
import redis

from taskforge.core.state_machine import TaskPriority, TaskStatus
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.queue.broker import RedisBroker
from taskforge.queue.redis_client import create_redis_client
from taskforge.workers.execution_engine import execute_task
from taskforge.workers.lease_manager import LeaseManager

TEST_KEY_PREFIX = "taskforge:g82:test:"
EVIDENCE_DIR = Path("benchmarks/scenarios/g82")


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
    assert queue_name.startswith(TEST_KEY_PREFIX), f"Refusing to delete non-test key: {queue_name}"
    redis_conn.delete(queue_name)


def save_test_evidence(test_name: str, payload: dict[str, Any]) -> None:
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    evidence_path = EVIDENCE_DIR / f"{test_name}.json"
    with open(evidence_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


# ============================================================================
# Gate 8.2.3: In-Flight Visibility & Worker Crash Recovery
# ============================================================================
def test_g823_worker_crash_before_lease_acquisition_pop_and_vanish(
    redis_conn, test_queue, db_session
):
    """Acceptance Criterion G8.2.3 — Critical Baseline Defect:

    Demonstrates the 'Pop-and-Vanish' defect:
    1. A task is staged in DB (QUEUED) and Redis queue.
    2. Consumer pops it via ZPOPMAX, which deletes it immediately from Redis.
    3. Consumer terminates before LeaseManager.acquire() / DB update.
    4. Recovery sweep runs after timeout:
       - Redis queue remains 0 (message vanished).
       - PostgreSQL record remains QUEUED without an active lease.
       - The task is permanently lost from execution and never recovered.
    """
    repo = TaskRepository(db_session)
    broker = RedisBroker(redis_conn, queue_name=test_queue)

    task = repo.create_task(
        task_name="Pop and Vanish Vulnerability Task",
        task_type="json_transform",
        payload={"vulnerable": True},
        priority=TaskPriority.MEDIUM.value,
    )
    task_id = task.id

    # Enqueue into Redis
    broker.enqueue(task_id, task.priority, task.payload)
    assert broker.get_queue_depth() == 1

    # Worker pops from Redis (ZPOPMAX removes it from sorted set immediately)
    popped_msg = broker.dequeue(timeout_seconds=0.1)
    assert popped_msg is not None
    assert popped_msg["task_id"] == task_id

    # Queue depth in Redis is immediately 0
    assert broker.get_queue_depth() == 0

    # SIMULATE CRASH: Worker process killed before lease acquisition
    # Neither lease.acquire() nor repo.record_attempt() is called.
    time.sleep(0.5)

    # Recovery sweep runs
    recovered = repo.recover_expired_leases()
    recovered_ids = [t.id for t in recovered]

    db_session.expire_all()
    current_task = repo.get_by_id(task_id)

    # Invariant checks exposing the defect:
    # 1. Task was NOT in recovered_leases because status is still QUEUED (not RUNNING)
    assert task_id not in recovered_ids
    # 2. Redis queue remains 0 (task vanished from broker)
    assert broker.get_queue_depth() == 0
    # 3. Task in DB is stranded in QUEUED indefinitely without a worker
    assert current_task.status == TaskStatus.QUEUED.value
    assert current_task.lease_token is None

    evidence = {
        "gate": "8.2.3",
        "scenario": "worker_crash_before_lease_acquisition",
        "defect_demonstrated": "pop_and_vanish",
        "task_id": task_id,
        "broker_queue_depth_after_pop": 0,
        "db_task_status": current_task.status,
        "db_lease_token": current_task.lease_token,
        "recovered_by_lease_reaper": False,
        "analysis": "ZPOPMAX immediately removes the item from Redis. A crash before lease acquisition strands the task in QUEUED state with no broker presence and no recoverable lease.",
    }
    save_test_evidence("worker_crash_recovery", evidence)


def test_g823_worker_crash_during_execution_lease_recovery(redis_conn, test_queue, db_session):
    """Acceptance Criterion G8.2.3 — Crash After Lease Acquisition:

    1. Worker acquires lease and transitions task to RUNNING.
    2. Worker terminates abruptly without releasing lease.
    3. Wait until lease expires.
    4. Recovery sweep identifies expired lease, increments retry_count,
       logs LEASE_EXPIRED_RECOVERED event, and transitions task to RETRY_WAIT.
    5. A second worker completes the task to SUCCESS.
    """
    repo = TaskRepository(db_session)

    created = repo.create_task(
        task_name="Execution Crash Recovery Task",
        task_type="json_transform",
        payload={"records": [10, 20, 30]},
        priority=TaskPriority.HIGH.value,
        max_retries=2,
    )
    task_id = created.id

    # Worker 1 acquires short 1-second lease
    worker_1 = "worker-crash-01"
    token_1 = f"lease-{uuid.uuid4().hex}"
    assert repo.acquire_lease(task_id, worker_1, lease_duration_seconds=1, lease_token=token_1) is True
    repo.update_status(task_id, TaskStatus.RUNNING, {"worker_id": worker_1}, lease_token=token_1)

    repo.record_attempt(
        task_id=task_id,
        attempt_number=1,
        worker_id=worker_1,
        status="RUNNING",
    )

    # Worker 1 crashes (no release_lease called). Wait 1.3s for lease expiration
    time.sleep(1.3)

    # Recovery sweep runs
    recovered_tasks = repo.recover_expired_leases()
    assert task_id in [t.id for t in recovered_tasks]

    db_session.expire_all()
    task_after = repo.get_by_id(task_id)
    assert task_after.status == TaskStatus.RETRY_WAIT.value
    assert task_after.retry_count == 1
    assert task_after.lease_token is None

    # Audit event logged
    recovery_events = [e for e in task_after.events if e.event_type == "LEASE_EXPIRED_RECOVERED"]
    assert len(recovery_events) == 1

    # Worker 2 picks up recovered task and completes execution
    worker_2 = "worker-healthy-02"
    success = execute_task(task_id, worker_2)
    assert success.is_success()

    db_session.expire_all()
    final_task = repo.get_by_id(task_id)
    assert final_task.status == TaskStatus.SUCCESS.value
    assert len(final_task.attempts) == 2  # Attempt 1 (crashed) + Attempt 2 (succeeded)
    assert final_task.result is not None


def test_g823_worker_lease_renewal_protects_long_running_task(db_session):
    """Acceptance Criterion G8.2.3 — Lease Auto-Renewal:

    A worker with an active LeaseManager auto-renews its lease in the background.
    A concurrent recovery sweep must NOT steal the lease while the worker is healthy.
    """
    repo = TaskRepository(db_session)
    task = repo.create_task(
        task_name="Long Running Protected Task",
        task_type="json_transform",
        payload={"data": "active"},
    )
    task_id = task.id

    worker_id = "worker-active-renewing"
    lease = LeaseManager(task_id, worker_id, lease_duration=2)
    assert lease.acquire() is True

    # Sleep 2.5 seconds (exceeding initial 2s lease). Renewal thread runs at 1s intervals
    time.sleep(2.5)

    # A concurrent recovery sweep runs
    recovered = repo.recover_expired_leases()
    assert task_id not in [t.id for t in recovered], "Active worker's lease was stolen!"

    db_session.expire_all()
    current = repo.get_by_id(task_id)
    assert current.lease_token == lease.lease_token

    lease.release()


def test_g823_in_flight_visibility_reclaims_crash_before_lease_acquisition(
    redis_conn, test_queue, db_session
):
    """Acceptance Criterion G8.2.3 — Remediation Verification:

    Verifies that the RedisBroker's two-phase in-flight visibility queue successfully
    resolves the 'Pop-and-Vanish' architectural defect:
    1. A task is enqueued to Redis with a short 1-second visibility timeout.
    2. A worker dequeues the task (atomically moving it to in_flight).
    3. The worker crashes immediately before database lease acquisition.
    4. Visibility timeout expires.
    5. The dispatcher / broker reclaims expired in-flight messages back to the ready queue.
    6. A healthy worker dequeues the task, completes execution, and calls broker.ack().
    7. Ready queue depth == 0 and in-flight queue depth == 0. Zero tasks lost.
    """
    repo = TaskRepository(db_session)
    # Instantiate broker with 1-second visibility timeout
    broker = RedisBroker(redis_conn, queue_name=test_queue, visibility_timeout_seconds=1)

    task = repo.create_task(
        task_name="In-Flight Visibility Recovery Task",
        task_type="json_transform",
        payload={"visibility_test": True},
        priority=TaskPriority.CRITICAL.value,
    )
    task_id = task.id

    # 1. Enqueue
    broker.enqueue(task_id, task.priority, task.payload)
    assert broker.get_queue_depth() == 1
    assert broker.get_in_flight_depth() == 0

    # 2. Worker pops from ready queue -> atomically moved to in_flight
    popped_msg = broker.dequeue(timeout_seconds=0.1)
    assert popped_msg is not None
    assert popped_msg["task_id"] == task_id

    # Ready queue is 0, in-flight queue is 1
    assert broker.get_queue_depth() == 0
    assert broker.get_in_flight_depth() == 1

    # 3. Simulate Worker crash before lease acquisition (no ack, no lease.acquire)
    # Wait 1.3s for visibility timeout to expire
    time.sleep(1.3)

    # 4. Reclaim expired in-flight messages (simulating Dispatcher reaper cycle)
    reclaimed = broker.reclaim_expired(timeout_seconds=0.1)
    assert reclaimed == 1, f"Expected 1 reclaimed task, got {reclaimed}"

    # Task is now back in ready queue!
    assert broker.get_queue_depth() == 1
    assert broker.get_in_flight_depth() == 0

    # 5. Healthy worker picks it up
    recovered_msg = broker.dequeue(timeout_seconds=0.1)
    assert recovered_msg is not None
    assert recovered_msg["task_id"] == task_id

    # Execute task to SUCCESS
    assert execute_task(task_id, "worker-healthy-reclaimed").is_success()

    # 6. Acknowledge completion
    acked = broker.ack(task_id, recovered_msg)
    assert acked is True

    # Final broker state: completely clean
    assert broker.get_queue_depth() == 0
    assert broker.get_in_flight_depth() == 0

    evidence = {
        "gate": "8.2.3",
        "scenario": "in_flight_visibility_recovery_verified",
        "task_id": task_id,
        "reclaimed_count": reclaimed,
        "post_reclaim_queue_depth": 1,
        "execution_status": "SUCCESS",
        "final_in_flight_depth": 0,
        "defect_resolved": "pop_and_vanish",
    }
    save_test_evidence("in_flight_recovery_remediation", evidence)


def test_recovery_ordering_postgresql_before_redis_prevents_stranding(redis_conn, test_queue, db_session):
    """Finding 1 Deterministic Proof:
    Verifies that executing PostgreSQL lease recovery BEFORE Redis in-flight reclaim
    strictly prevents replacement workers from dequeuing a delivery before database state is ready.
    """
    broker = RedisBroker(redis_conn, queue_name=test_queue, visibility_timeout_seconds=1)
    repo = TaskRepository(db_session)

    # 1. Enqueue task in DB and Redis
    task = repo.create_task("recovery-ordering-task", "noop", {"test": "order"}, TaskPriority.HIGH.value)
    task_id = task.id
    broker.enqueue(task_id, 3, {"task_id": task_id, "data": "order"})

    # 2. Worker 1 dequeues and acquires 1-second lease
    msg = broker.dequeue()
    assert msg is not None
    token_1 = "lease-worker-1"
    assert repo.acquire_lease(task_id, "worker-1", lease_duration_seconds=1, lease_token=token_1) is True
    repo.update_status(task_id, TaskStatus.RUNNING, {"worker": "worker-1"}, lease_token=token_1)

    # Worker 1 crashes without completing or renewing lease
    time.sleep(1.3)

    # RECOVERY PHASE:
    # Deterministic step 1: Recover expired PostgreSQL leases FIRST
    recovered_tasks = repo.recover_expired_leases()
    reaped_map = {x.id: x for x in recovered_tasks}
    assert task_id in reaped_map
    assert reaped_map[task_id].status == TaskStatus.RETRY_WAIT.value
    assert reaped_map[task_id].lease_token is None

    # Pause point: between Step 1 and Step 2
    # At this pause point, Redis ready queue is empty because Step 2 hasn't run yet.
    assert broker.get_queue_depth() == 0
    concurrent_pop = broker.dequeue(timeout_seconds=0.1)
    assert concurrent_pop is None, "Queue must not deliver message before Redis reclaim step!"

    # Deterministic step 2: Reclaim expired Redis deliveries
    reclaimed = broker.reclaim_expired()
    assert reclaimed == 1
    assert broker.get_queue_depth() == 1

    # Step 3: Replacement worker dequeues reclaimed delivery
    msg_rep = broker.dequeue(timeout_seconds=0.1)
    assert msg_rep is not None
    assert msg_rep["task_id"] == task_id

    # Replacement worker acquires lease: SUCCEEDS immediately because DB state is already RETRY_WAIT with no lease
    token_2 = "lease-worker-2-replacement"
    assert repo.acquire_lease(task_id, "worker-2", lease_duration_seconds=30, lease_token=token_2) is True
    repo.update_status(task_id, TaskStatus.QUEUED, {"worker": "worker-2"}, lease_token=token_2)
    repo.update_status(task_id, TaskStatus.RUNNING, {"worker": "worker-2"}, lease_token=token_2)

    # Clean execution and completion
    completed = repo.complete_task_success(task_id, token_2, {"result": "recovered_cleanly"})
    assert completed.status == TaskStatus.SUCCESS.value
    broker.ack(task_id, delivery_token=msg_rep["delivery_token"])

    # Clean up
    redis_conn.delete(test_queue)
    redis_conn.delete(f"{test_queue}:in_flight")
    redis_conn.delete(f"{test_queue}:seq:3")


def test_failure_retry_produces_exactly_one_eligible_delivery(redis_conn, test_queue, db_session):
    """Finding 2: Verify that when a task fails and is scheduled for retry:
    1. The original delivery is acknowledged with its delivery_token.
    2. Exactly one eligible delivery exists in the queue after retry backoff.
    3. Redis visibility timeout does NOT reclaim a duplicate copy.
    """
    from taskforge.queue.broker import set_global_broker
    from taskforge.workers.dispatcher import WorkerDispatcher

    broker = RedisBroker(redis_conn, queue_name=test_queue, visibility_timeout_seconds=1)
    set_global_broker(broker)

    repo = TaskRepository(db_session)
    # Create a task configured with registered 'failing' handler and 2 retries
    task = repo.create_task("fail-retry-task", "failing", {"error_message": "intentional_fail"}, TaskPriority.HIGH.value, max_retries=2)
    task_id = task.id

    dispatcher = WorkerDispatcher(worker_id="test-fail-dispatcher", concurrency=1, broker=broker)

    # Enqueue task
    broker.enqueue(task_id, 3, {"task_id": task_id, "data": "fail_test"})
    assert broker.get_queue_depth() == 1

    # Run dispatcher loop for 1 dispatch
    msg = broker.dequeue()
    assert msg is not None
    assert msg.get("delivery_token") is not None

    # Wrap task execution through dispatcher's wrapper
    dispatcher._task_wrapper(msg)

    # 1. Under G8.3 Item 2 durable retry outbox contract, the original delivery is ACKed
    # because retry publication is durably committed in PostgreSQL outbox_events.
    assert broker.get_in_flight_depth() == 0, "Original delivery must be ACKed once retry is durably in outbox"

    # 2. Wait for retry backoff to elapse and publish via transactional outbox
    time.sleep(1.1)
    from sqlalchemy import select

    from taskforge.database.models import OutboxEvent
    from taskforge.queue.outbox import publish_pending_outbox_events

    evt = db_session.scalars(
        select(OutboxEvent).where(
            OutboxEvent.aggregate_id == task_id,
            OutboxEvent.event_type == "TASK_RETRY",
        )
    ).first()
    assert evt is not None
    published = publish_pending_outbox_events(db_session, broker, event_id=evt.id)
    assert published == 1

    # Run broker reclaim: should reclaim ZERO because original delivery was acked
    reclaimed = broker.reclaim_expired()
    assert reclaimed == 0

    # 3. Queue MUST contain EXACTLY ONE delivery (the retry)
    assert broker.get_queue_depth() == 1, f"Expected exactly 1 retry delivery, got {broker.get_queue_depth()}"

    # Dequeue and verify it is the retry for the same task
    retry_msg = broker.dequeue()
    assert retry_msg is not None
    assert retry_msg["task_id"] == task_id

    # No leftover deliveries anywhere
    assert broker.get_queue_depth() == 0
    assert broker.get_in_flight_depth() == 1

    # Clean up
    broker.ack(task_id, delivery_token=retry_msg["delivery_token"])
    redis_conn.delete(test_queue)
    redis_conn.delete(f"{test_queue}:in_flight")
    redis_conn.delete(f"{test_queue}:seq:3")
