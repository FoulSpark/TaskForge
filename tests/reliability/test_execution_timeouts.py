import time

import pytest

from taskforge.core.state_machine import TaskPriority, TaskStatus
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.workers.execution_engine import execute_task


@pytest.fixture
def repo(db_session):
    return TaskRepository(db_session)


def test_handler_completes_within_timeout(repo, db_session):
    """G8.3.6: Verify handler completing well before timeout succeeds cleanly."""
    task = repo.create_task(
        task_name="Normal Duration Task",
        task_type="sleep",
        payload={"duration": 0.1},
        priority=TaskPriority.MEDIUM.value,
        timeout_seconds=3,
    )
    task_id = task.id

    start = time.perf_counter()
    success = execute_task(task_id, "worker-timeout-test-1")
    duration = time.perf_counter() - start

    assert success.is_success()
    assert duration < 2.0

    db_session.expire_all()
    updated = repo.get_by_id(task_id)
    assert updated.status == TaskStatus.SUCCESS.value
    assert updated.result is not None


def test_handler_exceeding_timeout_aborts_without_blocking_executor(repo, db_session):
    """G8.3.6: Critical Inspection Verification:

    A handler sleeping 4 seconds with timeout_seconds=1 must:
    1. Raise TimeoutError inside execute_task.
    2. Shut down ThreadPoolExecutor with wait=False, returning IMMEDIATELY (~1.0s).
    3. NOT block for 4 seconds upon exiting the executor.
    4. Transition task to RETRY_WAIT or FAILED with timeout error message.
    """
    task = repo.create_task(
        task_name="Timeout Sleep Task",
        task_type="sleep",
        payload={"duration": 4.0},
        priority=TaskPriority.HIGH.value,
        timeout_seconds=1,
        max_retries=1,
    )
    task_id = task.id

    start = time.perf_counter()
    success = execute_task(task_id, "worker-timeout-test-2")
    duration = time.perf_counter() - start

    # execute_task returns non-success disposition on failure/timeout
    assert not success.is_success()
    # CRITICAL: Verify execute_task did NOT block for 4 seconds!
    assert duration < 2.5, (
        f"Executor blocked waiting for thread to exit! Duration was {duration:.2f}s"
    )

    db_session.expire_all()
    updated = repo.get_by_id(task_id)
    # Task failed attempt 1; since max_retries=1, retry_count=1 -> status RETRY_WAIT
    assert updated.status in (TaskStatus.RETRY_WAIT.value, TaskStatus.FAILED.value)

    # Check attempt record contains timeout error
    assert len(updated.attempts) >= 1
    assert "timed out after 1" in (updated.attempts[-1].error_message or "").lower()


def test_handler_infinite_loop_timeout_non_blocking(repo, db_session):
    """G8.3.6: Uncooperative handler with CPU spin (simulated by sleep) is aborted in 1s."""
    task = repo.create_task(
        task_name="Uncooperative Hang Task",
        task_type="sleep",
        payload={"duration": 10.0},
        priority=TaskPriority.CRITICAL.value,
        timeout_seconds=1,
        max_retries=0,  # No retries -> immediately terminal
    )
    task_id = task.id

    start = time.perf_counter()
    success = execute_task(task_id, "worker-timeout-test-3")
    duration = time.perf_counter() - start

    assert not success.is_success()
    assert duration < 2.5, (
        f"Executor blocked waiting for uncooperative thread! Duration was {duration:.2f}s"
    )

    db_session.expire_all()
    updated = repo.get_by_id(task_id)
    # max_retries=0 -> transitions to DEAD_LETTER or FAILED
    assert updated.status in (TaskStatus.DEAD_LETTER.value, TaskStatus.FAILED.value)


def test_stale_handler_post_timeout_write_rejected_by_fencing(repo, db_session):
    """G8.3.6: Verify that if a timed-out handler finishes after timeout,

    its attempt to record a result or update status is strictly fenced and rejected.
    """
    token_stale = "lease-stale-timed-out"
    task = repo.create_task(
        task_name="Fenced Timeout Task",
        task_type="noop",
        payload={"test": True},
        priority=TaskPriority.HIGH.value,
        timeout_seconds=1,
    )
    task_id = task.id

    # 1. Acquire lease with token_stale
    assert (
        repo.acquire_lease(
            task_id, "worker-timeout", lease_duration_seconds=1, lease_token=token_stale
        )
        is True
    )
    repo.update_status(
        task_id, TaskStatus.RUNNING, {"worker_id": "worker-timeout"}, lease_token=token_stale
    )

    # 2. Timeout occurs, lease expires naturally (or is released)
    time.sleep(1.2)
    reaped = repo.recover_expired_leases()
    assert task_id in [t.id for t in reaped]

    # 3. Stale worker finishes its late computation and tries to persist result or update status
    with pytest.raises(PermissionError):
        repo.record_result(
            task_id=task_id,
            return_value={"late_result": "should_be_rejected"},
            execution_metadata={"worker": "stale"},
            lease_token=token_stale,
        )

    with pytest.raises(PermissionError):
        repo.update_status(
            task_id,
            TaskStatus.SUCCESS,
            {"worker_id": "worker-timeout"},
            lease_token=token_stale,
        )

    # Critical: Stale worker attempting atomic success or failure completion must also be rejected
    with pytest.raises(PermissionError):
        repo.complete_task_success(
            task_id=task_id,
            lease_token=token_stale,
            return_value={"late_result": "stale"},
        )

    with pytest.raises(PermissionError):
        repo.complete_task_failure(
            task_id=task_id,
            lease_token=token_stale,
            error_message="late failure report",
        )

    # Invariant: Database remains free of stale mutations
    db_session.expire_all()
    task_final = repo.get_by_id(task_id)
    assert task_final.result is None
    assert task_final.status == TaskStatus.RETRY_WAIT.value


def test_hard_process_timeout_terminates_cpu_spin():
    """G8.3.6: Hard Process Isolation Timeout.
    Demonstrates that process isolation with OS termination (Popen.kill)
    halts CPU-spinning handlers immediately, unlike in-thread execution.
    Uses a genuine CPU-bound loop calculation to verify CPU reclamation.
    """
    import subprocess
    import sys

    # Spawn an infinite CPU-spin calculation loop
    proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "x = 1\nwhile True:\n    x = (x * 1664525 + 1013904223) & 0xFFFFFFFF",
        ]
    )
    start = time.perf_counter()
    timeout_seconds = 1.0

    try:
        proc.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        # Hard process kill (Win32 TerminateProcess / POSIX SIGKILL)
        proc.kill()
        proc.wait(timeout=2.0)

    elapsed = time.perf_counter() - start
    assert proc.poll() is not None, "Process was not terminated!"
    assert elapsed < 2.5, f"Process termination delayed: {elapsed:.2f}s"


def test_execute_task_process_isolated_hard_timeout(repo, db_session):
    """G8.3.6: Integrated Process-Isolated Hard Timeout.
    Verifies TaskForge's integrated execute_task_process_isolated runner
    spawns a subprocess, halts runaway handlers, and recovers cleanly.
    """
    from taskforge.workers.execution_engine import execute_task_process_isolated

    task = repo.create_task(
        task_name="Process Isolated Spin Task",
        task_type="sleep",
        payload={"duration": 10.0},
        priority=TaskPriority.CRITICAL.value,
        timeout_seconds=1,
    )
    task_id = task.id

    start = time.perf_counter()
    success = execute_task_process_isolated(task_id, "worker-iso-test", timeout_seconds=1)
    duration = time.perf_counter() - start

    assert not success.is_success()
    assert duration < 3.0, f"Process isolation timed out late: {duration:.2f}s"


def test_dispatcher_process_isolated_execution_mode_kills_runaway_task(repo, db_session):
    """Finding 4: Verify WorkerDispatcher configured with execution_mode='process'
    dispatches task into a dedicated OS process via process_runner, kills it on hard timeout,
    records failure under lease fencing in DB, and acknowledges delivery in broker.
    """
    from taskforge.queue.broker import MemoryBroker
    from taskforge.workers.dispatcher import WorkerDispatcher

    broker = MemoryBroker()
    dispatcher = WorkerDispatcher(
        worker_id="proc-dispatcher-test",
        concurrency=1,
        broker=broker,
        execution_mode="process",
    )

    task = repo.create_task(
        task_name="Runaway Process Task",
        task_type="sleep",
        payload={"duration": 10.0},
        priority=TaskPriority.HIGH.value,
        timeout_seconds=1,
        max_retries=1,
    )
    task_id = task.id

    broker.enqueue(task_id, 3, {"task_id": task_id, "timeout_seconds": 1})
    msg = broker.dequeue()
    assert msg is not None
    assert msg.get("delivery_token") is not None

    start = time.perf_counter()
    # Dispatch through dispatcher wrapper
    dispatcher._task_wrapper(msg)
    elapsed = time.perf_counter() - start

    # Hard timeout occurred and terminated child process in ~1-2s (not 10s!)
    assert elapsed < 4.0, f"Dispatcher failed to kill runaway process in time: {elapsed:.2f}s"

    # Delivery acknowledged under G8.3 Item 2 durable retry outbox contract (RETRY_DURABLY_SCHEDULED)
    assert broker.get_in_flight_depth() == 0

    # DB state verified: attempt timed out and task transitioned to RETRY_WAIT under lease fencing
    db_session.expire_all()
    updated_task = repo.get_by_id(task_id)
    assert updated_task.status in (TaskStatus.RETRY_WAIT.value, TaskStatus.QUEUED.value)
    assert updated_task.lease_token is None
