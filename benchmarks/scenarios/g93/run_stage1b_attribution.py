"""Stage 1B: Diagnostic Instrumentation & Empirical Lifecycle Attribution Harness.

Executes W-01, W-02 (n=1000), and W-03 to produce:
1. Full 16-event lifecycle attribution breakdown.
2. Per-task correlation and timestamp completeness report.
3. Outbox-lag analysis (eligible lag, UNAVAILABLE_RACE, UNCLAIMED).
4. Broker-dwell analysis (client-observed dwell, negative interval frequency/magnitude).
5. Concurrency race/overlap analysis (outbox_published_commit vs redis_dequeue_complete).
6. Non-additive percentiles (p50, p95, p99) calculated directly on sample vectors.
7. Profiling harness overhead comparison (uninstrumented vs instrumented).
8. Non-blocking queue diagnostic integrity report (drops by event type, unpersisted records).
"""

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import sys
import threading
import time
from typing import Any
import uuid

import psycopg
from sqlalchemy import text

# Ensure src/ is on sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Disposable benchmark infrastructure
BENCHMARK_DB_NAME = "taskforge_g93_benchmark"
BENCHMARK_PSYCOPG_URL = f"postgresql://taskforge:taskforge_dev@localhost:5432/{BENCHMARK_DB_NAME}"
BENCHMARK_DB_URL = f"postgresql+psycopg://taskforge:taskforge_dev@localhost:5432/{BENCHMARK_DB_NAME}"
BENCHMARK_QUEUE_PREFIX = "taskforge:g93:b1:"

print(f"[Stage 1B] Initializing benchmark database '{BENCHMARK_DB_NAME}'...")
admin_conn = psycopg.connect("postgresql://taskforge:taskforge_dev@localhost:5432/postgres", autocommit=True)
with admin_conn.cursor() as cur:
    cur.execute(f"DROP DATABASE IF EXISTS {BENCHMARK_DB_NAME} WITH (FORCE)")
    cur.execute(f"CREATE DATABASE {BENCHMARK_DB_NAME} OWNER taskforge")
admin_conn.close()

os.environ["DATABASE_URL"] = BENCHMARK_DB_URL
os.environ["TASKFORGE_TESTING"] = "1"
os.environ["APP_ENV"] = "development"

# Run migrations
os.chdir(str(REPO_ROOT))
from alembic import command
from alembic.config import Config

alembic_cfg = Config(os.path.abspath(str(REPO_ROOT / "alembic.ini")))
alembic_cfg.set_main_option("script_location", os.path.abspath(str(REPO_ROOT / "migrations")))
alembic_cfg.set_main_option("sqlalchemy.url", BENCHMARK_DB_URL)
command.upgrade(alembic_cfg, "head")

from fastapi.testclient import TestClient

from taskforge.database.session import SessionLocal
from taskforge.main import app
from taskforge.monitoring.profiler import DiagnosticProfiler
from taskforge.queue.broker import RedisBroker
from taskforge.queue.outbox import OutboxPublisherWorker
from taskforge.queue.redis_client import create_redis_client
from taskforge.workers.dispatcher import WorkerDispatcher


def truncate_benchmark_tables() -> None:
    with SessionLocal() as db:
        db.execute(text("TRUNCATE tasks, task_attempts, task_results, outbox_events, task_events CASCADE"))
        db.commit()


def compute_distribution(samples_ms: list[float]) -> dict[str, float]:
    if not samples_ms:
        return {"p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0, "max_ms": 0.0, "min_ms": 0.0, "mean_ms": 0.0, "stddev_ms": 0.0, "count": 0}
    sorted_s = sorted(samples_ms)
    n = len(sorted_s)

    def _pct(p: float) -> float:
        k = (n - 1) * p
        f = int(k)
        c = min(f + 1, n - 1)
        d0 = sorted_s[f] * (c - k)
        d1 = sorted_s[c] * (k - f)
        return round(d0 + d1, 3)

    return {
        "p50_ms": _pct(0.50),
        "p95_ms": _pct(0.95),
        "p99_ms": _pct(0.99),
        "max_ms": round(sorted_s[-1], 3),
        "min_ms": round(sorted_s[0], 3),
        "mean_ms": round(statistics.mean(sorted_s), 3),
        "stddev_ms": round(statistics.stdev(sorted_s), 3) if n > 1 else 0.0,
        "count": n,
    }


def run_single_workload(
    workload_cfg: dict[str, Any],
    trial_id: str,
    enable_profiling: bool,
    redis_client: Any,
    raw_dir: Path,
) -> dict[str, Any]:
    wid = workload_cfg["workload_id"]
    name = workload_cfg["name"]
    task_type = workload_cfg["task_type"]
    payload = workload_cfg["payload"]
    concurrency = workload_cfg["concurrency"]
    warmup_count = workload_cfg["warmup_tasks"]
    measured_count = workload_cfg["measured_tasks"]
    priority = workload_cfg["priority"]

    if wid == "W-02":
        assert payload.get("n") == 1000, f"Critical invariant violation: W-02 must be n=1000, got {payload.get('n')}"

    truncate_benchmark_tables()

    profiler = DiagnosticProfiler.get_instance()
    raw_events_path = raw_dir / f"events_16_g93_{wid}_{trial_id}_{int(time.time())}.jsonl"

    if enable_profiling:
        profiler.start(raw_events_path)

    queue_name = f"{BENCHMARK_QUEUE_PREFIX}{wid}:{trial_id}:{uuid.uuid4().hex[:6]}"
    broker = RedisBroker(redis_client, queue_name=queue_name, visibility_timeout_seconds=30)

    outbox_pub = OutboxPublisherWorker(poll_interval=0.05, broker=broker)
    outbox_pub.start()

    dispatcher = WorkerDispatcher(
        worker_id=f"g93-b1-worker-{wid}-{trial_id}",
        concurrency=concurrency,
        broker=broker,
        poll_interval=0.05,
    )
    dispatcher.start()

    client = TestClient(app)

    print(f"\n[Stage 1B] Running {wid} ({name}) | Trial: {trial_id} | Profiling: {enable_profiling}")

    # 1. Warmup Phase (profiling inactive during warmup)
    warmup_ids = []
    for i in range(warmup_count):
        res = client.post(
            "/api/v1/tasks",
            json={
                "task_name": f"warmup-{wid}-{i}",
                "task_type": task_type,
                "payload": payload,
                "priority": priority,
            },
        )
        assert res.status_code == 201
        warmup_ids.append(res.json()["id"])

    # Await warmup drain
    w_start = time.time()
    while time.time() - w_start < 20.0:
        with SessionLocal() as db:
            completed_warmup = db.execute(
                text("SELECT COUNT(*) FROM tasks WHERE id = ANY(:ids) AND status = 'SUCCESS'"),
                {"ids": warmup_ids},
            ).scalar()
            if completed_warmup == warmup_count:
                break
        time.sleep(0.05)

    # 2. Measured Phase
    task_records: dict[str, dict[str, Any]] = {}
    records_lock = threading.Lock()

    def submit_task(idx: int):
        t0_req = time.perf_counter()
        t0_epoch = time.time()
        res = client.post(
            "/api/v1/tasks",
            json={
                "task_name": f"g93-b1-{wid}-{trial_id}-{idx}",
                "task_type": task_type,
                "payload": payload,
                "priority": priority,
            },
        )
        t1_resp = time.perf_counter()
        if res.status_code != 201:
            return {"status": "FAILED", "idx": idx, "error": res.text}
        tid = res.json()["id"]
        return {
            "status": "SUBMITTED",
            "idx": idx,
            "task_id": tid,
            "t0_epoch": t0_epoch,
            "t0_req": t0_req,
            "t1_resp": t1_resp,
            "submission_latency_ms": (t1_resp - t0_req) * 1000.0,
        }

    start_wall = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(submit_task, i) for i in range(measured_count)]
        for f in as_completed(futures):
            r = f.result()
            if r["status"] == "SUBMITTED":
                with records_lock:
                    task_records[r["task_id"]] = r

    assert len(task_records) == measured_count, f"Submitted {len(task_records)} != {measured_count}"

    deadline = time.time() + 90.0
    completed_ids = set()
    while time.time() < deadline:
        with SessionLocal() as db:
            rows = db.execute(
                text("SELECT id FROM tasks WHERE id = ANY(:ids) AND status IN ('SUCCESS', 'FAILURE', 'DEAD_LETTER')"),
                {"ids": list(task_records.keys())},
            ).fetchall()
            for row in rows:
                completed_ids.add(row[0])
            if len(completed_ids) == measured_count:
                break
        time.sleep(0.05)

    end_wall = time.perf_counter()
    total_duration_sec = end_wall - start_wall

    outbox_pub.stop()
    dispatcher.stop(grace_period_seconds=5.0)

    flush_report = {}
    raw_hash = ""
    if enable_profiling:
        flush_report = profiler.stop(drain_timeout=5.0)
        raw_hash = hashlib.sha256(raw_events_path.read_bytes()).hexdigest()

    # Collect attempt execution overhead
    with SessionLocal() as db:
        attempts_rows = db.execute(
            text("""
                SELECT ta.task_id, ta.started_at, ta.completed_at, ta.duration_ms
                FROM task_attempts ta
                WHERE ta.task_id = ANY(:ids) AND ta.attempt_number = 1
            """),
            {"ids": list(task_records.keys())},
        ).mappings().all()

    exec_overheads = []
    for att in attempts_rows:
        if att["duration_ms"] is not None:
            exec_overheads.append(float(att["duration_ms"]))
        elif att["completed_at"] and att["started_at"]:
            exec_overheads.append((att["completed_at"] - att["started_at"]).total_seconds() * 1000.0)

    achieved_tps = round(len(completed_ids) / total_duration_sec, 2)

    workload_summary = {
        "workload_id": wid,
        "name": name,
        "trial_id": trial_id,
        "profiling_enabled": enable_profiling,
        "measured_tasks": measured_count,
        "completed_tasks": len(completed_ids),
        "duration_seconds": round(total_duration_sec, 3),
        "throughput_tps": achieved_tps,
        "execution_overhead_p50_ms": round(statistics.median(exec_overheads), 3) if exec_overheads else 0.0,
        "flush_report": flush_report,
        "raw_events_path": str(raw_events_path) if enable_profiling else None,
        "raw_events_sha256": raw_hash if enable_profiling else None,
    }

    # If profiling was enabled, analyze the 16-event lifecycle
    if enable_profiling and raw_events_path.exists():
        analysis = analyze_16_events(raw_events_path, task_records)
        workload_summary["lifecycle_attribution"] = analysis

    return workload_summary


def analyze_16_events(events_path: Path, task_records: dict[str, dict[str, Any]]) -> dict[str, Any]:
    lines = [json.loads(line) for line in events_path.read_text(encoding="utf-8").strip().splitlines() if line.strip()]

    ALL_16_EVENTS = [
        "api_request_received",
        "task_transaction_commit",
        "api_response_sent",
        "outbox_claim_start",
        "outbox_claim_commit",
        "redis_enqueue_start",
        "redis_enqueue_complete",
        "outbox_published_commit",
        "redis_dequeue_complete",
        "lease_claim_start",
        "lease_commit",
        "handler_start",
        "handler_end",
        "terminal_transaction_start",
        "terminal_commit",
        "redis_ack_complete",
    ]

    event_counts = defaultdict(int)
    events_by_task = defaultdict(dict)
    batch_claims = {}

    for entry in lines:
        ev = entry["event"]
        tid = entry.get("task_id")
        bid = entry.get("batch_id")
        t_mono = entry["t_mono_ns"]
        t_epoch = entry["t_epoch_utc"]

        event_counts[ev] += 1

        if bid and ev in ("outbox_claim_start", "outbox_claim_commit"):
            if bid not in batch_claims:
                batch_claims[bid] = {}
            batch_claims[bid][ev] = {"t_mono": t_mono, "t_epoch": t_epoch}

        if tid:
            events_by_task[tid][ev] = {
                "t_mono_ns": t_mono,
                "t_epoch_utc": t_epoch,
                "batch_id": bid,
            }

    # Per-task interval extraction
    t_api_server = []
    t_outbox_lag_eligible = []
    t_outbox_claim = []
    t_redis_enq = []
    t_outbox_commit = []
    t_broker_dwell = []
    t_worker_lease = []
    t_handler_exec = []
    t_terminal_persist = []
    t_redis_ack = []
    t_e2e = []
    race_overlaps = []

    outbox_lag_races = 0
    outbox_unclaimed = 0
    broker_dwell_negatives = []

    for tid, tinfo in task_records.items():
        tev = events_by_task.get(tid, {})

        # End-to-end latency: terminal_commit - api_request_received
        if "terminal_commit" in tev and "api_request_received" in tev:
            e2e_ms = (tev["terminal_commit"]["t_mono_ns"] - tev["api_request_received"]["t_mono_ns"]) / 1_000_000.0
            t_e2e.append(e2e_ms)

        # 1. API Server
        if "task_transaction_commit" in tev and "api_request_received" in tev:
            d = (tev["task_transaction_commit"]["t_mono_ns"] - tev["api_request_received"]["t_mono_ns"]) / 1_000_000.0
            t_api_server.append(d)

        # 2. Outbox Lag
        bid = tev.get("redis_enqueue_start", {}).get("batch_id")
        if bid and bid in batch_claims and "outbox_claim_start" in batch_claims[bid] and "task_transaction_commit" in tev:
            claim_start_ns = batch_claims[bid]["outbox_claim_start"]["t_mono"]
            commit_return_ns = tev["task_transaction_commit"]["t_mono_ns"]
            if claim_start_ns > commit_return_ns:
                lag_ms = (claim_start_ns - commit_return_ns) / 1_000_000.0
                t_outbox_lag_eligible.append(lag_ms)
            else:
                outbox_lag_races += 1
        elif "task_transaction_commit" in tev and "redis_enqueue_start" not in tev:
            outbox_unclaimed += 1

        # 3. Outbox Claim
        if bid and bid in batch_claims and "outbox_claim_commit" in batch_claims[bid] and "outbox_claim_start" in batch_claims[bid]:
            claim_dur_ms = (batch_claims[bid]["outbox_claim_commit"]["t_mono"] - batch_claims[bid]["outbox_claim_start"]["t_mono"]) / 1_000_000.0
            t_outbox_claim.append(claim_dur_ms)

        # 4. Redis Enqueue
        if "redis_enqueue_complete" in tev and "redis_enqueue_start" in tev:
            enq_ms = (tev["redis_enqueue_complete"]["t_mono_ns"] - tev["redis_enqueue_start"]["t_mono_ns"]) / 1_000_000.0
            t_redis_enq.append(enq_ms)

        # 5. Outbox Commit
        if "outbox_published_commit" in tev and "redis_enqueue_complete" in tev:
            pub_ms = (tev["outbox_published_commit"]["t_mono_ns"] - tev["redis_enqueue_complete"]["t_mono_ns"]) / 1_000_000.0
            t_outbox_commit.append(pub_ms)

        # 6. Broker Dwell (Client-Observed Broker Dwell Estimate)
        if "redis_dequeue_complete" in tev and "redis_enqueue_complete" in tev:
            dwell_ms = (tev["redis_dequeue_complete"]["t_mono_ns"] - tev["redis_enqueue_complete"]["t_mono_ns"]) / 1_000_000.0
            if dwell_ms < 0:
                broker_dwell_negatives.append(dwell_ms)
            else:
                t_broker_dwell.append(dwell_ms)

        # Publication / Dequeue race overlap
        if "outbox_published_commit" in tev and "redis_dequeue_complete" in tev:
            overlap_ms = (tev["outbox_published_commit"]["t_mono_ns"] - tev["redis_dequeue_complete"]["t_mono_ns"]) / 1_000_000.0
            if overlap_ms > 0:
                race_overlaps.append(overlap_ms)

        # 7. Worker Lease
        if "lease_commit" in tev and "lease_claim_start" in tev:
            l_ms = (tev["lease_commit"]["t_mono_ns"] - tev["lease_claim_start"]["t_mono_ns"]) / 1_000_000.0
            t_worker_lease.append(l_ms)

        # 8. Handler Execution
        if "handler_end" in tev and "handler_start" in tev:
            h_ms = (tev["handler_end"]["t_mono_ns"] - tev["handler_start"]["t_mono_ns"]) / 1_000_000.0
            t_handler_exec.append(h_ms)

        # 9. Terminal Persistence
        if "terminal_commit" in tev and "terminal_transaction_start" in tev:
            term_ms = (tev["terminal_commit"]["t_mono_ns"] - tev["terminal_transaction_start"]["t_mono_ns"]) / 1_000_000.0
            t_terminal_persist.append(term_ms)

        # 10. Redis ACK
        if "redis_ack_complete" in tev and "terminal_commit" in tev:
            ack_ms = (tev["redis_ack_complete"]["t_mono_ns"] - tev["terminal_commit"]["t_mono_ns"]) / 1_000_000.0
            t_redis_ack.append(ack_ms)

    total_tasks = len(task_records)
    neg_dwell_pct = round((len(broker_dwell_negatives) / total_tasks) * 100.0, 2) if total_tasks else 0.0

    return {
        "timestamp_completeness": {
            "expected_events_per_task": 14,  # 14 task-specific events + 2 batch claim events
            "event_counts": dict(event_counts),
            "tracked_tasks": len(events_by_task),
        },
        "outbox_lag_analysis": {
            "eligible_lag_count": len(t_outbox_lag_eligible),
            "unavailable_race_count": outbox_lag_races,
            "unclaimed_count": outbox_unclaimed,
            "distribution": compute_distribution(t_outbox_lag_eligible),
        },
        "broker_dwell_analysis": {
            "client_observed_dwell_distribution": compute_distribution(t_broker_dwell),
            "negative_intervals": {
                "count": len(broker_dwell_negatives),
                "frequency_percent": neg_dwell_pct,
                "min_ms": round(min(broker_dwell_negatives), 3) if broker_dwell_negatives else 0.0,
                "max_ms": round(max(broker_dwell_negatives), 3) if broker_dwell_negatives else 0.0,
                "mean_ms": round(statistics.mean(broker_dwell_negatives), 3) if broker_dwell_negatives else 0.0,
            },
        },
        "publication_dequeue_race_overlap": {
            "overlap_count": len(race_overlaps),
            "distribution": compute_distribution(race_overlaps),
        },
        "stage_distributions": {
            "t_api_server_ms": compute_distribution(t_api_server),
            "t_outbox_claim_ms": compute_distribution(t_outbox_claim),
            "t_redis_enq_ms": compute_distribution(t_redis_enq),
            "t_outbox_commit_ms": compute_distribution(t_outbox_commit),
            "t_worker_lease_ms": compute_distribution(t_worker_lease),
            "t_handler_exec_ms": compute_distribution(t_handler_exec),
            "t_terminal_persist_ms": compute_distribution(t_terminal_persist),
            "t_redis_ack_ms": compute_distribution(t_redis_ack),
            "t_end_to_end_ms": compute_distribution(t_e2e),
        },
    }


def main():
    print("[Stage 1B] Executing Full Lifecycle Attribution Campaign...")
    defs_path = REPO_ROOT / "benchmarks/scenarios/g90/workload_definitions.json"
    workload_configs = json.loads(defs_path.read_text(encoding="utf-8"))["workloads"]
    defs_hash = hashlib.sha256(defs_path.read_bytes()).hexdigest()

    raw_dir = REPO_ROOT / "benchmarks/raw/g93_b1"
    raw_dir.mkdir(parents=True, exist_ok=True)
    redis_client = create_redis_client()

    results = {
        "milestone": "G9.3_stage1b_attribution",
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "git_provenance": {
            "base_authorized_sha": "c795315e3f49f8352406458bdbef4a692d62e9c9",
            "frozen_baseline_tag": "v0.8.0-phase8-freeze",
            "frozen_baseline_commit": "da27047f56fe2eec75acdb8502f505542cf61202",
        },
        "workload_definitions_sha256": defs_hash,
        "runs": {},
        "overhead_comparison": {},
    }

    # Execute Campaign:
    # For each workload:
    # 1. Uninstrumented Run (Baseline)
    # 2. Instrumented Attribution Run
    for w_cfg in workload_configs:
        wid = w_cfg["workload_id"]

        # Run 1: Uninstrumented Baseline
        res_baseline = run_single_workload(
            workload_cfg=w_cfg,
            trial_id=f"run_uninstrumented_{wid}",
            enable_profiling=False,
            redis_client=redis_client,
            raw_dir=raw_dir,
        )

        # Run 2: Instrumented Attribution
        res_instrumented = run_single_workload(
            workload_cfg=w_cfg,
            trial_id=f"run_instrumented_{wid}",
            enable_profiling=True,
            redis_client=redis_client,
            raw_dir=raw_dir,
        )

        results["runs"][f"{wid}_baseline"] = res_baseline
        results["runs"][f"{wid}_instrumented"] = res_instrumented

        # Overhead evaluation
        dur_delta_pct = round(((res_instrumented["duration_seconds"] - res_baseline["duration_seconds"]) / res_baseline["duration_seconds"]) * 100.0, 2)
        tps_delta_pct = round(((res_instrumented["throughput_tps"] - res_baseline["throughput_tps"]) / res_baseline["throughput_tps"]) * 100.0, 2)
        exec_delta_pct = round(((res_instrumented["execution_overhead_p50_ms"] - res_baseline["execution_overhead_p50_ms"]) / res_baseline["execution_overhead_p50_ms"]) * 100.0, 2) if res_baseline["execution_overhead_p50_ms"] else 0.0

        results["overhead_comparison"][wid] = {
            "baseline_duration_s": res_baseline["duration_seconds"],
            "instrumented_duration_s": res_instrumented["duration_seconds"],
            "duration_delta_pct": dur_delta_pct,
            "baseline_throughput_tps": res_baseline["throughput_tps"],
            "instrumented_throughput_tps": res_instrumented["throughput_tps"],
            "throughput_delta_pct": tps_delta_pct,
            "baseline_exec_p50_ms": res_baseline["execution_overhead_p50_ms"],
            "instrumented_exec_p50_ms": res_instrumented["execution_overhead_p50_ms"],
            "exec_p50_delta_pct": exec_delta_pct,
        }

    # Clean up database
    admin_conn = psycopg.connect("postgresql://taskforge:taskforge_dev@localhost:5432/postgres", autocommit=True)
    with admin_conn.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {BENCHMARK_DB_NAME} WITH (FORCE)")
    admin_conn.close()

    report_path = REPO_ROOT / "benchmarks/scenarios/g93/stage1b_attribution_report.json"
    report_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\n[Stage 1B] Attribution run completed. Full report: {report_path}")


if __name__ == "__main__":
    main()

