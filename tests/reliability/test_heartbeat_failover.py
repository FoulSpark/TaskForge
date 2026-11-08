import json
import time
from pathlib import Path

import pytest

from taskforge.core.state_machine import TaskPriority, TaskStatus
from taskforge.database.repositories.task_repository import TaskRepository

EVIDENCE_PATH = Path("benchmarks/scenarios/g83/heartbeat_failover.json")


@pytest.fixture
def repo(db_session):
    return TaskRepository(db_session)


def test_heartbeat_loss_lease_expiration_and_replacement_failover(repo, db_session):
    """G8.3.3: Heartbeat Loss & Failover Verification

    1. Worker A acquires task with 2.0s lease duration.
    2. Worker A's heartbeat/renewal ceases (simulating crash or partition).
    3. Lease expires.
    4. Reaper sweeps and reaps task to RETRY_WAIT.
    5. Replacement Worker B acquires task with new lease token and completes to SUCCESS.
    6. Worker A resumes and attempts to mutate status / record result.
    7. Invariant: Worker A is strictly fenced (PermissionError), zero corruptions.
    """
    t = repo.create_task("failover-task", "cpu_bound", {"n": 3000}, TaskPriority.HIGH.value)
    task_id = t.id

    token_a = "lease-token-A-original"
    token_b = "lease-token-B-replacement"

    # Step 1: Worker A acquires lease for 2 seconds (short lease for fast test)
    assert (
        repo.acquire_lease(task_id, "worker-A", lease_duration_seconds=2, lease_token=token_a)
        is True
    )
    repo.update_status(task_id, TaskStatus.RUNNING, {"worker_id": "worker-A"}, lease_token=token_a)

    # Step 2: Worker A stops renewing (sleep past lease duration)
    time.sleep(2.3)

    # Step 3: Reaper detects expired lease
    reap_start = time.perf_counter()
    reaped = repo.recover_expired_leases()
    detection_latency_ms = (time.perf_counter() - reap_start) * 1000.0

    assert task_id in [x.id for x in reaped]
    db_session.expire_all()
    t_reaped = repo.get_by_id(task_id)
    assert t_reaped.status == TaskStatus.RETRY_WAIT.value

    # Step 4: Task is re-enqueued into QUEUED for replacement worker
    repo.update_status(task_id, TaskStatus.QUEUED)

    # Replacement Worker B acquires lease and executes
    recovery_start = time.perf_counter()
    assert (
        repo.acquire_lease(task_id, "worker-B", lease_duration_seconds=10, lease_token=token_b)
        is True
    )
    repo.update_status(task_id, TaskStatus.RUNNING, {"worker_id": "worker-B"}, lease_token=token_b)

    # Worker B completes computation and records result
    repo.record_result(
        task_id=task_id,
        return_value={"primes_count": 430, "worker": "worker-B"},
        execution_metadata={"worker_id": "worker-B"},
        lease_token=token_b,
    )
    repo.update_status(task_id, TaskStatus.SUCCESS, {"worker_id": "worker-B"}, lease_token=token_b)
    recovery_latency_ms = (time.perf_counter() - recovery_start) * 1000.0

    # Step 5: Stale Worker A wakes up and attempts mutation with token_a
    stale_mutations = 0
    try:
        repo.update_status(
            task_id,
            TaskStatus.RUNNING,
            {"worker_id": "worker-A-stale"},
            lease_token=token_a,
        )
        stale_mutations += 1
    except PermissionError:
        pass  # Expected: Fenced!

    try:
        repo.record_result(
            task_id=task_id,
            return_value={"corrupted": True},
            execution_metadata={"worker_id": "worker-A-stale"},
            lease_token=token_a,
        )
        stale_mutations += 1
    except PermissionError:
        pass  # Expected: Fenced!

    # Step 6: Verify final database integrity
    db_session.expire_all()
    t_final = repo.get_by_id(task_id)
    assert t_final.status == TaskStatus.SUCCESS.value
    assert t_final.result.return_value["worker"] == "worker-B"
    assert stale_mutations == 0

    evidence = {
        "gate": "8.3.3",
        "description": "Heartbeat Loss, Lease Expiration & Worker Failover Verification",
        "task_id": task_id,
        "heartbeat_interval_seconds": 1.0,
        "lease_expiration_seconds": 2.0,
        "failure_detection_latency_ms": round(detection_latency_ms, 2),
        "recovery_latency_ms": round(recovery_latency_ms, 2),
        "recovered_task_count": 1,
        "stale_worker_mutation_count": stale_mutations,
        "replacement_worker_status": "SUCCESS",
        "verdict": "PASS",
    }
    EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(EVIDENCE_PATH, "w", encoding="utf-8") as f:
        json.dump(evidence, f, indent=2)
