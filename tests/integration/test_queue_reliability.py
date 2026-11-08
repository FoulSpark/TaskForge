import concurrent.futures
import json
import random
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
import redis

from taskforge.core.state_machine import TaskPriority
from taskforge.queue.broker import RedisBroker
from taskforge.queue.redis_client import create_redis_client

TEST_KEY_PREFIX = "taskforge:g82:test:"
EVIDENCE_DIR = Path("benchmarks/scenarios/g82")


@pytest.fixture(scope="module")
def redis_conn():
    """Provides a shared Redis client, verified with ping and closed on teardown."""
    client = create_redis_client()
    try:
        client.ping()
        yield client
    finally:
        client.close()


@pytest.fixture
def test_queue(redis_conn: redis.Redis):
    """Provides an isolated Redis queue key with guaranteed teardown cleanup."""
    queue_name = f"{TEST_KEY_PREFIX}{uuid.uuid4().hex}:tasks"
    yield queue_name
    # Strict safeguard: only delete keys starting with the test prefix
    assert queue_name.startswith(TEST_KEY_PREFIX), f"Refusing to delete non-test key: {queue_name}"
    redis_conn.delete(queue_name)


def save_test_evidence(test_name: str, payload: dict[str, Any]) -> None:
    """Save structured machine-readable test evidence for auditing."""
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    evidence_path = EVIDENCE_DIR / f"{test_name}.json"
    with open(evidence_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


# ============================================================================
# Gate 8.2.1: Priority Ordering Under Concurrent Producers
# ============================================================================
def test_g821_priority_ordering_under_concurrent_producers(redis_conn, test_queue):
    """Acceptance Criterion G8.2.1:

    10 concurrent producers enqueue 1,000 tasks across P1 (LOW) to P4 (CRITICAL).
    Verify that:
      1. Every enqueued task is eventually claimable (1,000 / 1,000).
      2. Higher-priority tasks are dequeued before lower-priority tasks.
      3. No task is duplicated or lost.
      4. Measure equal-priority tie-breaking against enqueue sequence.
    """
    broker = RedisBroker(redis_conn, queue_name=test_queue)
    num_producers = 10
    tasks_per_producer = 100
    total_tasks = num_producers * tasks_per_producer

    # Priorities to distribute: 1=LOW, 2=MEDIUM, 3=HIGH, 4=CRITICAL
    priorities = [
        TaskPriority.LOW.value,
        TaskPriority.MEDIUM.value,
        TaskPriority.HIGH.value,
        TaskPriority.CRITICAL.value,
    ]

    enqueued_log: list[dict[str, Any]] = []
    log_lock = threading.Lock()

    def producer_worker(producer_id: int):
        for seq in range(tasks_per_producer):
            # Deterministic distribution across priorities
            priority = priorities[(producer_id * tasks_per_producer + seq) % len(priorities)]
            task_id = f"task-p{producer_id:02d}-{seq:03d}-{uuid.uuid4().hex[:6]}"
            payload = {"producer_id": producer_id, "seq": seq}

            # Small random sleep to simulate asynchronous concurrent arrivals
            time.sleep(random.uniform(0.0001, 0.0008))

            broker.enqueue(task_id=task_id, priority=priority, payload=payload)
            enqueue_time = time.perf_counter()

            with log_lock:
                enqueued_log.append(
                    {
                        "task_id": task_id,
                        "priority": priority,
                        "enqueue_time": enqueue_time,
                        "producer_id": producer_id,
                    }
                )

    start_time = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_producers) as executor:
        futures = [executor.submit(producer_worker, i) for i in range(num_producers)]
        concurrent.futures.wait(futures, timeout=15.0)
    enqueue_duration = time.perf_counter() - start_time

    assert len(enqueued_log) == total_tasks, (
        f"Expected {total_tasks} enqueued, got {len(enqueued_log)}"
    )

    # Dequeue all tasks
    dequeue_start = time.perf_counter()
    dequeued_tasks: list[dict[str, Any]] = []
    while True:
        item = broker.dequeue(timeout_seconds=0.05)
        if item is None:
            break
        dequeued_tasks.append(item)
    dequeue_duration = time.perf_counter() - dequeue_start

    # Invariant 1: Completeness (No task lost or duplicated)
    assert len(dequeued_tasks) == total_tasks, (
        f"Lost tasks: dequeued {len(dequeued_tasks)} of {total_tasks}"
    )
    dequeued_ids = [t["task_id"] for t in dequeued_tasks]
    assert len(set(dequeued_ids)) == total_tasks, "Duplicate task claims detected in dequeue"

    # Invariant 2: Priority Ordering (Higher priority claimed before lower priority)
    dequeued_priorities = [t["priority"] for t in dequeued_tasks]
    expected_sorted_priorities = sorted(dequeued_priorities, reverse=True)
    assert dequeued_priorities == expected_sorted_priorities, (
        "Strict priority violation: Tasks were not dequeued in non-increasing priority order!"
    )

    # Invariant 3: Equal-Priority FIFO Tie-Breaking Analysis
    # Compare dequeued order against enqueue arrival order for each priority bucket
    fifo_analysis: dict[int, dict[str, Any]] = {}
    total_fifo_violations = 0

    for prio in [
        TaskPriority.CRITICAL.value,
        TaskPriority.HIGH.value,
        TaskPriority.MEDIUM.value,
        TaskPriority.LOW.value,
    ]:
        enqueued_prio_ids = [e["task_id"] for e in enqueued_log if e["priority"] == prio]
        dequeued_prio_ids = [d["task_id"] for d in dequeued_tasks if d["priority"] == prio]

        matches = [e == d for e, d in zip(enqueued_prio_ids, dequeued_prio_ids, strict=False)]
        match_count = sum(matches)
        violations = len(enqueued_prio_ids) - match_count
        total_fifo_violations += violations

        fifo_analysis[prio] = {
            "total_items": len(enqueued_prio_ids),
            "fifo_matches": match_count,
            "fifo_violations": violations,
            "fifo_accuracy_pct": round(
                (match_count / len(enqueued_prio_ids)) * 100 if enqueued_prio_ids else 100.0, 2
            ),
        }

    # Save structured audit evidence
    evidence = {
        "gate": "8.2.1",
        "description": "Priority Ordering Under Concurrent Producers",
        "configuration": {
            "producers": num_producers,
            "tasks_per_producer": tasks_per_producer,
            "total_tasks": total_tasks,
            "queue_name": test_queue,
        },
        "timings": {
            "enqueue_duration_sec": round(enqueue_duration, 4),
            "dequeue_duration_sec": round(dequeue_duration, 4),
            "enqueue_rate_tasks_per_sec": round(total_tasks / enqueue_duration, 2),
            "dequeue_rate_tasks_per_sec": round(total_tasks / dequeue_duration, 2),
        },
        "results": {
            "total_enqueued": total_tasks,
            "total_dequeued": len(dequeued_tasks),
            "unique_task_ids": len(set(dequeued_ids)),
            "lost_tasks": total_tasks - len(dequeued_tasks),
            "priority_order_verified": True,
            "fifo_tie_breaking_analysis": fifo_analysis,
            "total_fifo_tie_break_violations": total_fifo_violations,
        },
    }
    save_test_evidence("priority_ordering", evidence)


# ============================================================================
# Gate 8.2.2: Atomic Claims Under Concurrent Consumers
# ============================================================================
def test_g822_atomic_claims_under_concurrent_consumers(redis_conn, test_queue):
    """Acceptance Criterion G8.2.2:

    20 concurrent consumers drain 2,000 queued tasks from shared Redis.
    Verify that:
      1. Every task is claimed at most once (zero duplicate claims).
      2. No task is lost (all 2,000 tasks accounted for).
      3. Redis queue is completely drained (depth == 0).
      4. Work is distributed across all active consumers.
    """
    broker = RedisBroker(redis_conn, queue_name=test_queue)
    num_consumers = 20
    total_tasks = 2000

    # Fast pipeline batch enqueue
    pipe = redis_conn.pipeline()
    original_ids = []
    for i in range(total_tasks):
        task_id = f"task-2k-{i:04d}-{uuid.uuid4().hex[:6]}"
        original_ids.append(task_id)
        prio = (i % 4) + 1
        msg = json.dumps({"task_id": task_id, "priority": prio, "payload": {"index": i}})
        pipe.zadd(test_queue, {msg: prio})
    pipe.execute()

    assert broker.get_queue_depth() == total_tasks, f"Setup failed: expected {total_tasks} in queue"

    claims: list[dict[str, Any]] = []
    claims_lock = threading.Lock()
    start_barrier = threading.Barrier(num_consumers)

    def consumer_worker(consumer_id: int):
        start_barrier.wait()  # Synchronize all consumers to start simultaneously
        local_claims = []
        while True:
            item = broker.dequeue(timeout_seconds=0.05)
            if item is None:
                # Retry once to avoid transient empty race
                time.sleep(0.01)
                item = broker.dequeue(timeout_seconds=0.05)
                if item is None:
                    break
            local_claims.append(
                {
                    "consumer_id": f"consumer-{consumer_id:02d}",
                    "task_id": item["task_id"],
                    "priority": item["priority"],
                    "timestamp": time.perf_counter(),
                }
            )

        with claims_lock:
            claims.extend(local_claims)

    start_time = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_consumers) as executor:
        futures = [executor.submit(consumer_worker, i) for i in range(num_consumers)]
        concurrent.futures.wait(futures, timeout=20.0)
    drain_duration = time.perf_counter() - start_time

    # Invariant 1: Total claimed count must match total enqueued count
    assert len(claims) == total_tasks, (
        f"Expected {total_tasks} claims, got {len(claims)}. Lost: {total_tasks - len(claims)}"
    )

    # Invariant 2: Zero duplicate claims
    claimed_ids = [c["task_id"] for c in claims]
    unique_claimed_ids = set(claimed_ids)
    duplicate_count = len(claimed_ids) - len(unique_claimed_ids)
    assert duplicate_count == 0, (
        f"Atomic claim violated! {duplicate_count} duplicate claims detected."
    )

    # Invariant 3: All original tasks accounted for
    assert unique_claimed_ids == set(original_ids), "Claimed set does not match original tasks!"

    # Invariant 4: Queue completely drained
    final_depth = broker.get_queue_depth()
    assert final_depth == 0, f"Queue not empty! Remaining tasks: {final_depth}"

    # Consumer distribution metrics
    consumer_distribution: dict[str, int] = {}
    for c in claims:
        cid = c["consumer_id"]
        consumer_distribution[cid] = consumer_distribution.get(cid, 0) + 1

    # Ensure all 20 consumers participated
    assert len(consumer_distribution) == num_consumers, (
        f"Only {len(consumer_distribution)} / {num_consumers} consumers claimed tasks!"
    )

    unaccounted_count = total_tasks - len(unique_claimed_ids)
    evidence = {
        "gate": "8.2.2",
        "description": "Atomic Claims Under Concurrent Consumers",
        "configuration": {
            "consumers": num_consumers,
            "total_tasks": total_tasks,
            "queue_name": test_queue,
        },
        "results": {
            "submitted_count": total_tasks,
            "unique_claimed_count": len(unique_claimed_ids),
            "duplicate_claim_count": duplicate_count,
            "unaccounted_task_count": unaccounted_count,
            "completed_count": len(claims),
            "failed_count": 0,
            "elapsed_seconds": round(drain_duration, 4),
            "drain_rate_tasks_per_sec": round(total_tasks / drain_duration, 2),
            "final_queue_depth": final_depth,
            "consumer_distribution": consumer_distribution,
        },
    }
    save_test_evidence("concurrent_claims", evidence)
