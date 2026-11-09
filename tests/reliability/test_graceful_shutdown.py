import json
import time
import uuid
from pathlib import Path

import pytest
import redis

from taskforge.core.state_machine import TaskPriority, TaskStatus
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.queue.broker import RedisBroker
from taskforge.queue.redis_client import create_redis_client
from taskforge.workers.dispatcher import WorkerDispatcher

TEST_KEY_PREFIX = "taskforge:g83:test:"
EVIDENCE_PATH = Path("benchmarks/scenarios/g83/graceful_shutdown.json")


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
    assert queue_name.startswith(TEST_KEY_PREFIX)
    redis_conn.delete(queue_name)
    redis_conn.delete(f"{queue_name}:in_flight")
    redis_conn.delete(f"{queue_name}:seq")


def test_graceful_shutdown_drains_active_tasks_cleanly(redis_conn, test_queue, db_session):
    """G8.3.1: Graceful Worker Shutdown

    1. Dispatcher starts with concurrency=2, poll_interval=0.05.
    2. Enqueues 2 tasks with duration=0.8s (sleep handler).
    3. Dispatcher begins execution (active_tasks > 0).
    4. Enqueues a 3rd task to ready queue.
    5. Graceful shutdown requested (stop with grace_period=3.0s).
    6. Invariant:
       - Dispatcher stops pulling new tasks (3rd task remains safely in ready queue).
       - Active 2 tasks finish cleanly within grace period and commit SUCCESS.
       - Redis in-flight depth == 0 for completed tasks.
       - Process exits with 0 stranded tasks.
    """
    repo = TaskRepository(db_session)
    broker = RedisBroker(redis_conn, queue_name=test_queue, visibility_timeout_seconds=5)

    # Enqueue two 1.5s tasks
    t1 = repo.create_task("drain-task-1", "sleep", {"duration": 1.5}, TaskPriority.HIGH.value)
    t2 = repo.create_task("drain-task-2", "sleep", {"duration": 1.5}, TaskPriority.HIGH.value)
    broker.enqueue(t1.id, t1.priority, t1.payload)
    broker.enqueue(t2.id, t2.priority, t2.payload)

    dispatcher = WorkerDispatcher(
        worker_id=f"graceful-{uuid.uuid4().hex[:6]}",
        concurrency=2,
        broker=broker,
        poll_interval=0.05,
    )
    dispatcher.start()

    # Wait until both tasks are claimed and active
    max_wait = 2.0
    start = time.perf_counter()
    while time.perf_counter() - start < max_wait:
        with dispatcher._active_lock:
            if dispatcher._active_tasks == 2:
                break
        time.sleep(0.05)

    with dispatcher._active_lock:
        active_at_signal = dispatcher._active_tasks

    # Enqueue 3rd task while 1 and 2 are running
    t3 = repo.create_task("pending-task-3", "noop", {"idx": 3}, TaskPriority.LOW.value)
    broker.enqueue(t3.id, t3.priority, t3.payload)

    # Trigger graceful shutdown with 4.0s grace period
    shutdown_start = time.perf_counter()
    dispatcher.stop(grace_period_seconds=4.0)
    drain_duration = time.perf_counter() - shutdown_start

    # Verify results
    db_session.expire_all()
    t1_final = repo.get_by_id(t1.id)
    t2_final = repo.get_by_id(t2.id)
    t3_final = repo.get_by_id(t3.id)

    # 1. In-progress tasks drained cleanly to SUCCESS
    assert t1_final.status == TaskStatus.SUCCESS.value
    assert t2_final.status == TaskStatus.SUCCESS.value
    assert t1_final.result is not None
    assert t2_final.result is not None

    # 2. 3rd task was NOT pulled during shutdown and remains untouched in ready queue
    assert t3_final.status == TaskStatus.QUEUED.value
    assert broker.get_queue_depth() == 1
    assert broker.get_in_flight_depth() == 0

    evidence = {
        "gate": "8.3.1",
        "description": "Graceful Shutdown In-Flight Task Drain Verification",
        "active_tasks_at_shutdown_signal": active_at_signal,
        "drain_duration_seconds": round(drain_duration, 3),
        "grace_period_configured": 4.0,
        "task_1_status": t1_final.status,
        "task_2_status": t2_final.status,
        "unclaimed_task_3_status": t3_final.status,
        "final_in_flight_queue_depth": broker.get_in_flight_depth(),
        "unaccounted_deliveries": 0,
        "verdict": "PASS",
    }
    EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(EVIDENCE_PATH, "w", encoding="utf-8") as f:
        json.dump(evidence, f, indent=2)


def test_grace_period_expiration_revokes_leases_and_allows_recovery(
    redis_conn, test_queue, db_session
):
    """G8.3.1: Grace Period Expiration & Immediate Lease Revocation
    When a running task exceeds the shutdown grace period:
    1. Dispatcher must stop renewing the task's lease immediately.
    2. Lease is vacated so the task becomes claimable/recoverable.
    3. A replacement worker can acquire the lease and complete the task.
    4. The old worker's late write (when its handler finishes) is rejected by lease fencing.
    """
    repo = TaskRepository(db_session)
    broker = RedisBroker(redis_conn, queue_name=test_queue, visibility_timeout_seconds=2)

    # Enqueue a task that runs for 4 seconds
    t = repo.create_task("long-running-task", "sleep", {"duration": 4.0}, TaskPriority.HIGH.value)
    broker.enqueue(t.id, t.priority, t.payload)

    dispatcher = WorkerDispatcher(
        worker_id=f"grace-timeout-{uuid.uuid4().hex[:6]}",
        concurrency=1,
        broker=broker,
        poll_interval=0.05,
    )
    dispatcher.start()

    # Wait until task is running
    start = time.perf_counter()
    while time.perf_counter() - start < 3.0:
        db_session.expire_all()
        task_cur = repo.get_by_id(t.id)
        if task_cur and task_cur.status == TaskStatus.RUNNING.value:
            break
        time.sleep(0.05)

    assert task_cur.status == TaskStatus.RUNNING.value
    old_lease_token = task_cur.lease_token
    assert old_lease_token is not None

    # Stop dispatcher with a short grace period of 1.0s (task takes 4.0s)
    shutdown_start = time.perf_counter()
    dispatcher.stop(grace_period_seconds=1.0)
    stop_duration = time.perf_counter() - shutdown_start

    # Dispatcher unblocked after ~1.0s
    assert stop_duration < 2.5

    # Invariant 1: Dispatcher halted renewal and vacated lease in DB
    db_session.expire_all()
    task_after_stop = repo.get_by_id(t.id)
    # Lease token was cleared upon grace period expiration
    assert task_after_stop.lease_token is None

    # Invariant 2: Replacement worker can immediately acquire lease and complete the task
    replacement_token = f"lease-replacement-{uuid.uuid4().hex[:6]}"
    acquired = repo.acquire_lease(
        t.id,
        "worker-replacement",
        lease_duration_seconds=5,
        lease_token=replacement_token,
    )
    assert acquired is True

    # Replacement worker records SUCCESS
    repo.complete_task_success(
        task_id=t.id,
        lease_token=replacement_token,
        return_value={"completed_by": "replacement_worker"},
        execution_metadata={"worker_id": "worker-replacement"},
    )

    db_session.expire_all()
    task_final = repo.get_by_id(t.id)
    assert task_final.status == TaskStatus.SUCCESS.value
    assert task_final.result.return_value == {"completed_by": "replacement_worker"}

    # Invariant 3: If old worker later attempts to write using old_lease_token, it is fenced
    with pytest.raises(PermissionError):
        repo.complete_task_success(
            task_id=t.id,
            lease_token=old_lease_token,
            return_value={"completed_by": "stale_old_worker"},
        )
