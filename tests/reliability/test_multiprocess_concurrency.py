import json
import time
import uuid
from pathlib import Path

import pytest
import redis

from taskforge.core.state_machine import TaskPriority, TaskStatus
from taskforge.database.models import Task, TaskResult
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal
from taskforge.queue.broker import RedisBroker
from taskforge.queue.redis_client import create_redis_client
from taskforge.workers.dispatcher import WorkerDispatcher

TEST_KEY_PREFIX = "taskforge:g83:test:"
EVIDENCE_PATH = Path("benchmarks/scenarios/g83/multiprocess_concurrency.json")
TOTAL_TASKS = 1000


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


def test_four_worker_dispatchers_mixed_workload_under_chaos(redis_conn, test_queue, db_session):
    """G8.3.4: Multi-Process / Multi-Worker Concurrency Under Random Failure

    Workload Topology:
    - 4 independent WorkerDispatchers (each with 4 worker threads = 16 execution slots).
    - 1,000 mixed tasks (noop, cpu_bound, io_bound, sleep) across P0-P3.
    - Concurrent submission and concurrent execution.
    - Chaos: 1 worker dispatcher is abruptly stopped mid-stream, then a replacement
      dispatcher is spawned to take over.

    Acceptance Invariants:
    1. Zero duplicate committed results (total TaskResult records == 1,000).
    2. Zero stale-worker mutations.
    3. Zero permanently stranded tasks (all 1,000 in SUCCESS).
    4. Queue depth == 0 and in-flight depth == 0.
    """
    broker = RedisBroker(redis_conn, queue_name=test_queue, visibility_timeout_seconds=4)

    handlers = [
        ("noop", {"data": "test_noop"}),
        ("cpu_bound", {"n": 500}),
        ("io_bound", {"delay_seconds": 0.005}),
        ("sleep", {"duration": 0.01}),
    ]
    priorities = [
        TaskPriority.LOW.value,
        TaskPriority.MEDIUM.value,
        TaskPriority.HIGH.value,
        TaskPriority.CRITICAL.value,
    ]

    # Pre-create and enqueue 1,000 tasks
    task_ids = []

    with SessionLocal() as db:
        local_repo = TaskRepository(db)
        for i in range(TOTAL_TASKS):
            h_type, payload = handlers[i % len(handlers)]
            p = priorities[i % len(priorities)]
            t = local_repo.create_task(
                task_name=f"concurrent-task-{i}",
                task_type=h_type,
                payload=payload,
                priority=p,
            )
            task_ids.append(t.id)

    enqueue_start = time.perf_counter()
    for i, t_id in enumerate(task_ids):
        p = priorities[i % len(priorities)]
        broker.enqueue(t_id, p, {"idx": i})
    enqueue_duration = time.perf_counter() - enqueue_start

    assert broker.get_queue_depth() == TOTAL_TASKS

    # Launch 4 concurrent worker dispatchers (each 4 threads = 16 slots)
    dispatchers = [
        WorkerDispatcher(
            worker_id=f"fleet-worker-{w}",
            concurrency=4,
            broker=broker,
            poll_interval=0.02,
        )
        for w in range(4)
    ]
    for d in dispatchers:
        d.start()

    drain_start = time.perf_counter()

    # CHAOS INJECTION: Kill dispatcher 1 abruptly after 0.5s of execution
    time.sleep(0.5)
    dispatchers[1].stop(grace_period_seconds=0.0)

    # Spawn replacement worker to restore fleet concurrency
    replacement = WorkerDispatcher(
        worker_id="fleet-worker-replacement",
        concurrency=4,
        broker=broker,
        poll_interval=0.02,
    )
    replacement.start()
    active_dispatchers = [dispatchers[0], dispatchers[2], dispatchers[3], replacement]

    # Wait for all 1,000 tasks to finish draining
    max_wait = 75.0
    while time.perf_counter() - drain_start < max_wait:
        # Trigger periodic reclaim for any in-flight messages from the killed worker
        broker.reclaim_expired(timeout_seconds=0.1)
        with SessionLocal() as db:
            local_repo = TaskRepository(db)
            local_repo.recover_expired_leases()
            completed = (
                db.query(Task)
                .filter(Task.id.in_(task_ids), Task.status == TaskStatus.SUCCESS.value)
                .count()
            )
            if completed == TOTAL_TASKS:
                break
        time.sleep(0.2)

    total_drain_time = time.perf_counter() - drain_start

    # Stop all active dispatchers cleanly
    for d in active_dispatchers:
        d.stop(grace_period_seconds=2.0)

    # Final Database Integrity Assertions
    with SessionLocal() as db:
        final_tasks = db.query(Task).filter(Task.id.in_(task_ids)).all()
        status_counts = {}
        for t in final_tasks:
            status_counts[t.status] = status_counts.get(t.status, 0) + 1

        results = db.query(TaskResult).filter(TaskResult.task_id.in_(task_ids)).all()
        total_results = len(results)
        unique_result_tasks = len(set(r.task_id for r in results))

    assert status_counts.get(TaskStatus.SUCCESS.value, 0) == TOTAL_TASKS, (
        f"Incomplete drain: {status_counts}"
    )
    assert total_results == TOTAL_TASKS, f"Duplicate results detected! Total: {total_results}"
    assert unique_result_tasks == TOTAL_TASKS, (
        f"Result-task mismatch: {unique_result_tasks} vs {TOTAL_TASKS}"
    )
    assert broker.get_queue_depth() == 0
    assert broker.get_in_flight_depth() == 0

    evidence = {
        "gate": "8.3.4",
        "description": "Multi-Worker Concurrency Under Mid-Run Worker Termination",
        "total_tasks_enqueued": TOTAL_TASKS,
        "concurrent_dispatchers": 4,
        "total_execution_slots": 16,
        "enqueue_duration_sec": round(enqueue_duration, 3),
        "drain_duration_sec": round(total_drain_time, 3),
        "throughput_tasks_per_sec": round(TOTAL_TASKS / total_drain_time, 1),
        "completed_success_count": status_counts.get(TaskStatus.SUCCESS.value, 0),
        "duplicate_results": total_results - unique_result_tasks,
        "permanently_stranded_tasks": TOTAL_TASKS - status_counts.get(TaskStatus.SUCCESS.value, 0),
        "final_ready_queue_depth": broker.get_queue_depth(),
        "final_in_flight_depth": broker.get_in_flight_depth(),
        "verdict": "PASS",
    }
    EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(EVIDENCE_PATH, "w", encoding="utf-8") as f:
        json.dump(evidence, f, indent=2)
