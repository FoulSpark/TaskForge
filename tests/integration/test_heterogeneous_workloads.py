import json
import statistics
import time
from pathlib import Path
from typing import Any

import pytest

from taskforge.core.state_machine import TaskPriority, TaskStatus
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.workers.execution_engine import execute_task

EVIDENCE_PATH = Path("benchmarks/scenarios/g83/workload_correctness.json")


@pytest.fixture
def repo(db_session):
    return TaskRepository(db_session)


def test_deterministic_workload_correctness_and_latency_profiles(repo, db_session):
    """G8.3.5: Heterogeneous Workload Execution Correctness

    Validates deterministic outputs and records latency profiles for:
    - noop: minimal overhead baseline
    - cpu_bound: prime number sieve computation
    - io_bound: simulated I/O concurrency
    - failing: dead-letter retry boundary
    - sleep: lease duration handling
    """
    EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    profiles: dict[str, Any] = {}

    # 1. NOOP Workload (10 iterations for baseline distribution)
    noop_latencies = []
    for i in range(10):
        t = repo.create_task("noop-test", "noop", {"idx": i}, TaskPriority.HIGH.value)
        start = time.perf_counter()
        assert execute_task(t.id, f"worker-noop-{i}").is_success()
        noop_latencies.append((time.perf_counter() - start) * 1000.0)

        db_session.expire_all()
        res = repo.get_by_id(t.id).result
        assert res.return_value == {"status": "ok", "payload": {"idx": i, "_current_attempt": 1}}

    profiles["noop"] = {
        "iterations": 10,
        "p50_ms": round(statistics.median(noop_latencies), 2),
        "mean_ms": round(statistics.mean(noop_latencies), 2),
        "max_ms": round(max(noop_latencies), 2),
        "deterministic_verification": "PASS",
    }

    # 2. CPU_BOUND Workload (limit = 10,000 -> 1,229 primes, largest = 9,973)
    cpu_latencies = []
    for i in range(5):
        t = repo.create_task("cpu-test", "cpu_bound", {"n": 10000}, TaskPriority.CRITICAL.value)
        start = time.perf_counter()
        assert execute_task(t.id, f"worker-cpu-{i}").is_success()
        cpu_latencies.append((time.perf_counter() - start) * 1000.0)

        db_session.expire_all()
        res = repo.get_by_id(t.id).result
        assert res.return_value["primes_count"] == 1229
        assert res.return_value["largest_prime"] == 9973

    profiles["cpu_bound"] = {
        "iterations": 5,
        "primes_count": 1229,
        "largest_prime": 9973,
        "p50_ms": round(statistics.median(cpu_latencies), 2),
        "mean_ms": round(statistics.mean(cpu_latencies), 2),
        "deterministic_verification": "PASS",
    }

    # 3. IO_BOUND Workload (delay = 0.05s)
    io_latencies = []
    for i in range(5):
        t = repo.create_task(
            "io-test",
            "io_bound",
            {"delay_seconds": 0.02, "bytes_read": 2048},
            TaskPriority.MEDIUM.value,
        )
        start = time.perf_counter()
        assert execute_task(t.id, f"worker-io-{i}").is_success()
        io_latencies.append((time.perf_counter() - start) * 1000.0)

        db_session.expire_all()
        res = repo.get_by_id(t.id).result
        assert res.return_value["bytes_read"] == 2048
        assert res.return_value["status"] == "simulated_io_complete"

    profiles["io_bound"] = {
        "iterations": 5,
        "bytes_read": 2048,
        "p50_ms": round(statistics.median(io_latencies), 2),
        "mean_ms": round(statistics.mean(io_latencies), 2),
        "deterministic_verification": "PASS",
    }

    # 4. FAILING Workload (max_retries = 2 -> attempts 1, 2, 3 -> DEAD_LETTER)
    t_fail = repo.create_task(
        "fail-test",
        "failing",
        {"error_message": "Forced error for DLQ verification"},
        TaskPriority.LOW.value,
        max_retries=2,
    )
    fail_id = t_fail.id
    # Attempt 1 -> RETRY_WAIT
    assert not execute_task(fail_id, "worker-fail").is_success()
    # Wait for attempt 1 backoff (1s) to elapse before attempt 2
    time.sleep(1.1)
    # Attempt 2 -> RETRY_WAIT
    assert not execute_task(fail_id, "worker-fail").is_success()
    # Wait for attempt 2 backoff (2s) to elapse before attempt 3
    time.sleep(2.1)
    # Attempt 3 -> max_retries exhausted -> DEAD_LETTER
    assert not execute_task(fail_id, "worker-fail").is_success()

    db_session.expire_all()
    failed_task = repo.get_by_id(fail_id)
    assert failed_task.status == TaskStatus.DEAD_LETTER.value
    assert len(failed_task.attempts) == 3
    for att in failed_task.attempts:
        assert "Forced error for DLQ verification" in (att.error_message or "")

    profiles["failing"] = {
        "max_retries": 2,
        "attempts_recorded": 3,
        "final_status": failed_task.status,
        "dlq_boundary_verified": "PASS",
    }

    # 5. SLEEP Workload (duration = 0.2s)
    t_sleep = repo.create_task("sleep-test", "sleep", {"duration": 0.2}, TaskPriority.MEDIUM.value)
    start = time.perf_counter()
    assert execute_task(t_sleep.id, "worker-sleep").is_success()
    sleep_elapsed_ms = (time.perf_counter() - start) * 1000.0

    db_session.expire_all()
    sleep_res = repo.get_by_id(t_sleep.id).result
    assert sleep_res.return_value["slept_seconds"] == 0.2

    profiles["sleep"] = {
        "duration_sec": 0.2,
        "elapsed_ms": round(sleep_elapsed_ms, 2),
        "deterministic_verification": "PASS",
    }

    evidence = {
        "gate": "8.3.5",
        "description": "Heterogeneous Workload Deterministic Correctness & Latency Profiles",
        "handlers_tested": ["noop", "cpu_bound", "io_bound", "failing", "sleep"],
        "profiles": profiles,
        "overall_status": "PASS",
    }
    with open(EVIDENCE_PATH, "w", encoding="utf-8") as f:
        json.dump(evidence, f, indent=2)
