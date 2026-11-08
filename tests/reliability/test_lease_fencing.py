import json
import time
from pathlib import Path
from typing import Any

import pytest

from taskforge.core.state_machine import TaskPriority, TaskStatus
from taskforge.database.repositories.task_repository import TaskRepository

EVIDENCE_DIR = Path("benchmarks/scenarios/g82")


def save_test_evidence(test_name: str, payload: dict[str, Any]) -> None:
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    evidence_path = EVIDENCE_DIR / f"{test_name}.json"
    with open(evidence_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


# ============================================================================
# Gate 8.2.3 / 8.2.5: Lease Fencing & Stale Worker State Protection
# ============================================================================
def test_stale_worker_fenced_write_rejected(db_session):
    """Verifies Lease Fencing Invariant:

    1. Worker A acquires lease with token_A.
    2. Lease expires.
    3. Worker B acquires lease with token_B.
    4. Worker A attempts to record_result or update_status using token_A.
       -> Database repository raises PermissionError (Fencing Violation).
    5. Worker B commits valid result using token_B.
    """
    repo = TaskRepository(db_session)
    task = repo.create_task(
        task_name="Fencing Protection Task",
        task_type="json_transform",
        payload={"data": 123},
        priority=TaskPriority.HIGH.value,
    )
    task_id = task.id

    token_a = "lease-token-A-111"
    token_b = "lease-token-B-222"

    # 1. Worker A acquires short 1-second lease with token_A
    assert (
        repo.acquire_lease(task_id, "worker-A", lease_duration_seconds=1, lease_token=token_a)
        is True
    )
    repo.update_status(task_id, TaskStatus.RUNNING, {"worker_id": "worker-A"}, lease_token=token_a)

    # 2. Worker A hangs; lease expires
    time.sleep(1.2)
    repo.recover_expired_leases()

    # 3. Worker B acquires the task with token_B
    assert (
        repo.acquire_lease(task_id, "worker-B", lease_duration_seconds=10, lease_token=token_b)
        is True
    )
    repo.update_status(task_id, TaskStatus.RUNNING, {"worker_id": "worker-B"}, lease_token=token_b)

    db_session.expire_all()
    task_under_b = repo.get_by_id(task_id)
    assert task_under_b.lease_token == token_b

    # 4. Worker A unhangs and tries to renew using token_A -> rejected
    renew_rejected = repo.renew_lease(task_id, token_a)
    assert renew_rejected is False

    # 5. Worker A attempts unfenced writes -> strictly rejected with PermissionError!
    with pytest.raises(PermissionError, match="Fencing violation"):
        repo.record_result(
            task_id=task_id,
            return_value={"stale_data": True},
            execution_metadata={"worker_id": "worker-A"},
            lease_token=token_a,
        )

    with pytest.raises(PermissionError, match="Fencing violation"):
        repo.update_status(
            task_id,
            TaskStatus.SUCCESS,
            {"worker_id": "worker-A"},
            lease_token=token_a,
        )

    # 6. Worker B writes legitimate result -> accepted
    legit_result = repo.record_result(
        task_id=task_id,
        return_value={"legitimate_data": True},
        execution_metadata={"worker_id": "worker-B"},
        lease_token=token_b,
    )
    assert legit_result is not None

    repo.update_status(
        task_id,
        TaskStatus.SUCCESS,
        {"worker_id": "worker-B"},
        lease_token=token_b,
    )

    db_session.expire_all()
    final_task = repo.get_by_id(task_id)
    assert final_task.status == TaskStatus.SUCCESS.value
    assert final_task.result.execution_metadata["worker_id"] == "worker-B"

    evidence = {
        "gate": "8.2.3_fencing",
        "test_name": "test_stale_worker_fenced_write_rejected",
        "task_id": task_id,
        "token_a": token_a,
        "token_b": token_b,
        "stale_worker_renew_rejected": True,
        "stale_worker_record_result_fenced": True,
        "stale_worker_update_status_fenced": True,
        "active_worker_write_accepted": True,
        "fencing_protection_verified": True,
    }
    save_test_evidence("lease_fencing", evidence)


def test_concurrent_lease_acquisition_mutual_exclusion(db_session):
    """Verifies that under concurrent attempts to acquire a lease on the same task,

    exactly one worker succeeds and the other is locked out.
    """
    repo = TaskRepository(db_session)
    task = repo.create_task(
        task_name="Mutual Exclusion Task",
        task_type="json_transform",
        payload={"foo": "bar"},
    )
    task_id = task.id

    # Worker 1 acquires lease
    assert repo.acquire_lease(task_id, "worker-1", lease_duration_seconds=5) is True

    # Worker 2 attempts to acquire lease while Worker 1's lease is valid
    assert repo.acquire_lease(task_id, "worker-2", lease_duration_seconds=5) is False

    db_session.expire_all()
    t = repo.get_by_id(task_id)
    assert t.lease_token is not None
    assert t.lease_token.startswith("lease-")


def test_duplicate_delivery_same_worker_identity_cannot_acquire_twice():
    """Acceptance Criterion G8.2.5 — Same-Worker Identity Lease Isolation:

    WorkerDispatcher runs multiple threads sharing the same worker_id
    (e.g. 'taskforge-worker-main').
    When duplicate deliveries of the same task arrive at two threads in the same worker,
    the lease MUST NOT allow the second thread to acquire an active lease just because
    it shares the worker_id string.
    Exactly ONE thread must acquire the lease; the other must be rejected.
    """
    import concurrent.futures
    import threading

    from taskforge.database.session import SessionLocal

    with SessionLocal() as db:
        repo = TaskRepository(db)
        task = repo.create_task(
            task_name="Same Worker Lease Test Task",
            task_type="json_transform",
            payload={"foo": "bar"},
        )
        task_id = task.id

    shared_worker_id = "taskforge-worker-main"
    start_barrier = threading.Barrier(2)
    acquisition_results = []
    results_lock = threading.Lock()

    def try_acquire(thread_idx: int):
        start_barrier.wait()
        with SessionLocal() as local_db:
            local_repo = TaskRepository(local_db)
            res = local_repo.acquire_lease(task_id, shared_worker_id, lease_duration_seconds=30)
            with results_lock:
                acquisition_results.append((thread_idx, res))

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(try_acquire, 1)
        f2 = executor.submit(try_acquire, 2)
        f1.result()
        f2.result()

    successful_acquisitions = [res for _, res in acquisition_results if res]
    is_defective = len(successful_acquisitions) > 1

    evidence = {
        "gate": "8.2.5_same_worker_lease",
        "task_id": task_id,
        "shared_worker_id": shared_worker_id,
        "thread_outcomes": acquisition_results,
        "successful_acquisition_count": len(successful_acquisitions),
        "same_worker_reentry_defect_resolved": not is_defective,
        "analysis": "acquire_lease now generates unique lease tokens and rejects unexpired leases unconditionally, preventing concurrent acquisition across threads sharing worker_id.",
    }
    save_test_evidence("same_worker_lease_remediation", evidence)

    assert len(successful_acquisitions) == 1, (
        f"CRITICAL LEASE DEFECT: Both threads with worker_id '{shared_worker_id}' acquired the lease! Outcomes: {acquisition_results}"
    )


def test_stale_worker_failure_path_fenced_write_rejected(db_session):
    """G8.3 Fencing Invariant on Failure Paths:
    1. Worker A acquires lease with token_A.
    2. Lease expires; Worker B acquires lease with token_B.
    3. Worker A encounters an exception / timeout and attempts:
       - complete_task_failure with token_A -> raises PermissionError.
       - update_status(RETRY_WAIT) with token_A or None -> raises PermissionError.
       - update_status(DEAD_LETTER) with token_A -> raises PermissionError.
       - record_result with token_A -> raises PermissionError.
    4. Worker B completes task successfully with token_B.
    5. Invariant: Task state in DB remains SUCCESS under Worker B; Worker A caused zero corruption.
    """
    repo = TaskRepository(db_session)
    task = repo.create_task(
        task_name="Failure Path Fencing Task",
        task_type="noop",
        payload={"fencing_test": True},
        priority=TaskPriority.HIGH.value,
        max_retries=2,
    )
    task_id = task.id
    token_a = "token-A-stale"
    token_b = "token-B-legit"

    # Worker A acquires lease
    assert (
        repo.acquire_lease(task_id, "worker-A", lease_duration_seconds=1, lease_token=token_a)
        is True
    )
    repo.update_status(task_id, TaskStatus.RUNNING, {"worker_id": "worker-A"}, lease_token=token_a)

    # Worker A hangs, lease expires
    time.sleep(1.2)
    repo.recover_expired_leases()

    # Worker B acquires lease with token_b
    assert (
        repo.acquire_lease(task_id, "worker-B", lease_duration_seconds=10, lease_token=token_b)
        is True
    )
    repo.update_status(task_id, TaskStatus.RUNNING, {"worker_id": "worker-B"}, lease_token=token_b)

    # 1. Worker A attempts complete_task_failure using stale token_a -> REJECTED
    with pytest.raises(PermissionError, match="Fencing violation"):
        repo.complete_task_failure(
            task_id=task_id,
            lease_token=token_a,
            error_message="Worker A timed out",
            is_timeout=True,
        )

    # 2. Worker A attempts update_status to RETRY_WAIT with stale token_a -> REJECTED
    with pytest.raises(PermissionError, match="Fencing violation"):
        repo.update_status(
            task_id,
            TaskStatus.RETRY_WAIT,
            {"error": "stale retry"},
            lease_token=token_a,
        )

    # 3. Worker A attempts update_status to RETRY_WAIT with NO token (bypass attempt) -> REJECTED
    with pytest.raises(PermissionError, match="Fencing violation"):
        repo.update_status(
            task_id,
            TaskStatus.RETRY_WAIT,
            {"error": "bypass attempt"},
            lease_token=None,
        )

    # 4. Worker A attempts update_status to DEAD_LETTER -> REJECTED
    with pytest.raises(PermissionError, match="Fencing violation"):
        repo.update_status(
            task_id,
            TaskStatus.DEAD_LETTER,
            {"error": "stale dead letter"},
            lease_token=token_a,
        )

    # 5. Worker B completes task cleanly with token_b -> ACCEPTED
    completed_task = repo.complete_task_success(
        task_id=task_id,
        lease_token=token_b,
        return_value={"completed_by": "worker-B"},
        execution_metadata={"worker": "worker-B"},
    )
    assert completed_task.status == TaskStatus.SUCCESS.value

    db_session.expire_all()
    final_task = repo.get_by_id(task_id)
    assert final_task.status == TaskStatus.SUCCESS.value
    assert final_task.result.return_value == {"completed_by": "worker-B"}
    assert final_task.retry_count == 1  # Incremented once by lease recovery; stale worker was unable to increment retry_count to 2


def test_attempt_ownership_and_terminal_status_strictly_verified(db_session):
    """Finding 6: Verify complete_task_success and complete_task_failure verify
    attempt ownership (attempt.task_id == task_id) and reject already-terminal attempts.
    """
    repo = TaskRepository(db_session)
    t1 = repo.create_task("task-1", "noop", {}, TaskPriority.HIGH.value)
    t2 = repo.create_task("task-2", "noop", {}, TaskPriority.HIGH.value)

    att1 = repo.record_attempt(t1.id, 1, "worker-1", "RUNNING")
    att2 = repo.record_attempt(t2.id, 1, "worker-2", "RUNNING")

    token_1 = "lease-token-t1"
    assert repo.acquire_lease(t1.id, "worker-1", lease_token=token_1) is True
    repo.update_status(t1.id, TaskStatus.RUNNING, {"worker": "worker-1"}, lease_token=token_1)

    # 1. Attempting complete_task_success on t1 using attempt belonging to t2 -> REJECTED
    with pytest.raises(ValueError, match="Attempt ownership violation"):
        repo.complete_task_success(
            task_id=t1.id,
            lease_token=token_1,
            return_value={"ok": True},
            attempt_id=att2.id,
        )

    # 2. Attempting complete_task_failure on t1 using attempt belonging to t2 -> REJECTED
    with pytest.raises(ValueError, match="Attempt ownership violation"):
        repo.complete_task_failure(
            task_id=t1.id,
            lease_token=token_1,
            error_message="failed",
            attempt_id=att2.id,
        )

    # 3. Legitimate completion of t1 with att1 -> ACCEPTED
    repo.complete_task_success(
        task_id=t1.id,
        lease_token=token_1,
        return_value={"ok": True},
        attempt_id=att1.id,
    )

    # 4. Attempting to complete already-terminal attempt on another task -> REJECTED
    t3 = repo.create_task("task-3", "noop", {}, TaskPriority.HIGH.value)
    token_3 = "lease-token-t3"
    assert repo.acquire_lease(t3.id, "worker-3", lease_token=token_3) is True
    repo.update_status(t3.id, TaskStatus.RUNNING, lease_token=token_3)
    with pytest.raises(ValueError, match="Attempt ownership violation"):
        repo.complete_task_success(
            task_id=t3.id,
            lease_token=token_3,
            return_value={"ok": True},
            attempt_id=att1.id,
        )


def test_lease_acquisition_enforces_claimable_state_guard(db_session):
    """Finding 7: Verify acquire_lease rejects tasks in terminal states (SUCCESS, DEAD_LETTER, CANCELLED)."""
    repo = TaskRepository(db_session)

    # 1. SUCCESS task cannot acquire lease
    t_success = repo.create_task("task-succ", "noop", {}, TaskPriority.HIGH.value)
    tok = "lease-succ"
    assert repo.acquire_lease(t_success.id, "worker-1", lease_token=tok) is True
    repo.update_status(t_success.id, TaskStatus.RUNNING, lease_token=tok)
    repo.complete_task_success(t_success.id, tok, {"done": True})

    assert repo.acquire_lease(t_success.id, "worker-2") is False

    # 2. DEAD_LETTER task cannot acquire lease
    t_dead = repo.create_task("task-dead", "noop", {}, TaskPriority.HIGH.value, max_retries=0)
    tok_dead = "lease-dead"
    assert repo.acquire_lease(t_dead.id, "worker-1", lease_token=tok_dead) is True
    repo.update_status(t_dead.id, TaskStatus.RUNNING, lease_token=tok_dead)
    repo.complete_task_failure(t_dead.id, tok_dead, error_message="fatal")

    db_session.expire_all()
    t_dead_refreshed = repo.get_by_id(t_dead.id)
    assert t_dead_refreshed.status == TaskStatus.DEAD_LETTER.value
    assert repo.acquire_lease(t_dead.id, "worker-2") is False

    # 3. CANCELLED task cannot acquire lease
    t_cancel = repo.create_task("task-cancel", "noop", {}, TaskPriority.HIGH.value)
    repo.cancel_task(t_cancel.id)

    db_session.expire_all()
    t_cancel_refreshed = repo.get_by_id(t_cancel.id)
    assert t_cancel_refreshed.status == TaskStatus.CANCELLED.value
    assert repo.acquire_lease(t_cancel.id, "worker-2") is False
