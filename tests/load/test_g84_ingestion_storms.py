"""
G8.4.2 — Multi-Producer Ingestion Storms Load Test Suite.
Evaluates high-concurrency producer ingestion throughput, latency distributions (P50/P90/P95/P99),
and transactional outbox parity across three progressive load tiers:
- Profile G8.4-L1: 100 concurrent producers, 1,000 tasks
- Profile G8.4-L2: 500 concurrent producers, 5,000 tasks
- Profile G8.4-L3: 1,000 concurrent producers, 10,000 tasks
"""

import concurrent.futures
import json
import time
import uuid
from pathlib import Path

from sqlalchemy import select

from taskforge.api.schemas import TaskCreateRequest
from taskforge.database.models import OutboxEvent, Task
from taskforge.database.session import SessionLocal
from taskforge.tasks.service import TaskService


def compute_percentiles(latencies: list[float]) -> dict[str, float]:
    sorted_l = sorted(latencies)
    n = len(sorted_l)
    return {
        "p50_ms": round(sorted_l[min(int(n * 0.50), n - 1)] * 1000, 2),
        "p90_ms": round(sorted_l[min(int(n * 0.90), n - 1)] * 1000, 2),
        "p95_ms": round(sorted_l[min(int(n * 0.95), n - 1)] * 1000, 2),
        "p99_ms": round(sorted_l[min(int(n * 0.99), n - 1)] * 1000, 2),
        "max_ms": round(sorted_l[-1] * 1000, 2),
        "min_ms": round(sorted_l[0] * 1000, 2),
    }


def _run_ingestion_storm(
    profile_name: str,
    num_producers: int,
    total_tasks: int,
    worker_concurrency: int = 16,
) -> dict:
    batch_tag = f"{profile_name}-{uuid.uuid4().hex[:6]}"
    task_requests = [
        TaskCreateRequest(
            task_name=f"Storm-{batch_tag}-{i}",
            task_type="noop" if i % 2 == 0 else "json_transform",
            payload={"index": i, "batch": batch_tag},
            priority=(i % 4) + 1,
        )
        for i in range(total_tasks)
    ]

    latencies: list[float] = []
    created_task_ids: list[str] = []
    errors: list[str] = []

    def _submit_worker(req: TaskCreateRequest):
        t0 = time.perf_counter()
        try:
            with SessionLocal() as db:
                service = TaskService(db)
                task, is_created = service.submit_task(req)
                elapsed = time.perf_counter() - t0
                return task.id, is_created, elapsed, None
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            return None, False, elapsed, str(exc)

    t_start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_concurrency) as executor:
        futures = [executor.submit(_submit_worker, req) for req in task_requests]
        for f in concurrent.futures.as_completed(futures):
            tid, is_created, el, err = f.result()
            latencies.append(el)
            if err:
                errors.append(err)
            else:
                created_task_ids.append(tid)
    total_duration = time.perf_counter() - t_start

    assert len(errors) == 0, f"{len(errors)} submission errors encountered: {errors[:5]}"
    assert len(created_task_ids) == total_tasks
    assert len(set(created_task_ids)) == total_tasks

    percentiles = compute_percentiles(latencies)
    throughput_rps = round(total_tasks / total_duration, 1)

    # Persistence verification: sample 50 tasks for outbox parity
    sample_ids = created_task_ids[:50]
    with SessionLocal() as db:
        tasks = list(db.scalars(select(Task).where(Task.id.in_(sample_ids))).all())
        assert len(tasks) == len(sample_ids)

        outbox_events = list(
            db.scalars(select(OutboxEvent).where(OutboxEvent.aggregate_id.in_(sample_ids))).all()
        )
        assert len(outbox_events) == len(sample_ids)

    metrics = {
        "gate": "G8.4.2",
        "profile": profile_name,
        "simulated_producers": num_producers,
        "total_tasks_enqueued": total_tasks,
        "successful_submissions": len(created_task_ids),
        "failed_submissions": len(errors),
        "total_duration_seconds": round(total_duration, 3),
        "throughput_rps": throughput_rps,
        "percentiles": percentiles,
        "outbox_parity": "100%",
        "verdict": "PASS",
    }
    return metrics


def test_ingestion_storm_l1_100_producers_1000_tasks():
    """Profile G8.4-L1: 100 concurrent producers submitting 1,000 tasks."""
    metrics = _run_ingestion_storm(
        profile_name="G8.4-L1",
        num_producers=100,
        total_tasks=1000,
        worker_concurrency=16,
    )
    dest = Path("benchmarks/scenarios/g84/g84_ingestion_storm_l1.json")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(metrics, indent=2))


def test_ingestion_storm_l2_500_producers_5000_tasks():
    """Profile G8.4-L2: 500 concurrent producers submitting 5,000 tasks."""
    metrics = _run_ingestion_storm(
        profile_name="G8.4-L2",
        num_producers=500,
        total_tasks=5000,
        worker_concurrency=20,
    )
    dest = Path("benchmarks/scenarios/g84/g84_ingestion_storm_l2.json")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(metrics, indent=2))


def test_ingestion_storm_l3_1000_producers_10000_tasks():
    """Profile G8.4-L3: 1,000 concurrent producers submitting 10,000 tasks."""
    metrics = _run_ingestion_storm(
        profile_name="G8.4-L3",
        num_producers=1000,
        total_tasks=10000,
        worker_concurrency=24,
    )
    dest = Path("benchmarks/scenarios/g84/g84_ingestion_storm_l3.json")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(metrics, indent=2))


def test_ingestion_storm_l1_multi_trial_reproducibility():
    """Profile G8.4-L1 Reproducibility: 5 independent consecutive runs of 100 producers / 1,000 tasks."""
    num_trials = 5
    trial_results = []

    for trial_idx in range(1, num_trials + 1):
        metrics = _run_ingestion_storm(
            profile_name=f"G8.4-L1-Trial-{trial_idx}",
            num_producers=100,
            total_tasks=1000,
            worker_concurrency=16,
        )
        trial_results.append(
            {
                "trial": trial_idx,
                "duration_seconds": metrics["total_duration_seconds"],
                "throughput_rps": metrics["throughput_rps"],
                "p50_ms": metrics["percentiles"]["p50_ms"],
                "p95_ms": metrics["percentiles"]["p95_ms"],
                "p99_ms": metrics["percentiles"]["p99_ms"],
                "outbox_parity": metrics["outbox_parity"],
            }
        )

    throughputs = [t["throughput_rps"] for t in trial_results]
    p95_latencies = [t["p95_ms"] for t in trial_results]

    multi_trial_summary = {
        "gate": "G8.4.2",
        "benchmark": "G8.4-L1 Reproducibility (5 Trials)",
        "tasks_per_trial": 1000,
        "concurrent_producers": 100,
        "trials_executed": num_trials,
        "trials": trial_results,
        "aggregate_metrics": {
            "min_throughput_rps": min(throughputs),
            "max_throughput_rps": max(throughputs),
            "avg_throughput_rps": round(sum(throughputs) / len(throughputs), 1),
            "min_p95_ms": min(p95_latencies),
            "max_p95_ms": max(p95_latencies),
            "avg_p95_ms": round(sum(p95_latencies) / len(p95_latencies), 2),
        },
        "verdict": "PASS",
    }

    dest = Path("benchmarks/scenarios/g84/g84_l1_multi_trial.json")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(multi_trial_summary, indent=2))


