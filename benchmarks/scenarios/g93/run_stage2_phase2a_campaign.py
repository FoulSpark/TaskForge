#!/usr/bin/env python3
"""Stage 2 Phase 2A — Dispatcher Scheduling Efficiency Diagnostic Campaign.

Executes a 3-paired interleaved diagnostic campaign (3 x Disabled, 3 x Enabled)
across W-01, W-02, and W-03 to measure internal dispatcher wake-up delay,
dequeue duration, replenishment lag, and queue-backed starvation ratio.
"""

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import statistics
import sys
import threading
import time
from typing import Any
import uuid

import psycopg
from sqlalchemy import text

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Disposable benchmark infrastructure
BENCHMARK_DB_NAME = "taskforge_g93_benchmark"
BENCHMARK_PSYCOPG_URL = f"postgresql://taskforge:taskforge_dev@localhost:5432/{BENCHMARK_DB_NAME}"
BENCHMARK_DB_URL = f"postgresql+psycopg://taskforge:taskforge_dev@localhost:5432/{BENCHMARK_DB_NAME}"
BENCHMARK_QUEUE_PREFIX = "taskforge:g93:s2a:"

print(f"[Stage 2 Phase 2A] Initializing benchmark database '{BENCHMARK_DB_NAME}'...")
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
        w = k - f
        return round(sorted_s[f] * (1.0 - w) + sorted_s[c] * w, 3)

    return {
        "p50_ms": _pct(0.50),
        "p95_ms": _pct(0.95),
        "p99_ms": _pct(0.99),
        "max_ms": round(sorted_s[-1], 3),
        "min_ms": round(sorted_s[0], 3),
        "mean_ms": round(statistics.mean(sorted_s), 3),
        "stddev_ms": round(statistics.stdev(sorted_s) if n > 1 else 0.0, 3),
        "count": n,
    }


def analyze_phase2a_events(events_path: Path, concurrency: int) -> dict[str, Any]:
    lines = [json.loads(line) for line in events_path.read_text(encoding="utf-8").strip().splitlines() if line.strip()]

    event_counts = defaultdict(int)
    all_events = []
    events_by_task = defaultdict(dict)
    sleep_intervals = [] # list of (start_mono, end_mono, sleep_id, loop_iter)
    sleep_starts = {} # sleep_id -> (start_mono, loop_iter)

    for entry in lines:
        ev = entry["event"]
        tid = entry.get("task_id")
        t_mono = entry["t_mono_ns"]
        t_epoch = entry["t_epoch_utc"]
        meta = entry.get("metadata") or {}

        event_counts[ev] += 1
        all_events.append(entry)

        if ev == "dispatcher_sleep_start":
            sid = meta.get("sleep_id", "")
            sleep_starts[sid] = (t_mono, meta.get("loop_iter", 0))
        elif ev == "dispatcher_sleep_end":
            sid = meta.get("sleep_id", "")
            if sid in sleep_starts:
                st, liter = sleep_starts[sid]
                sleep_intervals.append((st, t_mono, sid, liter))

        if tid:
            events_by_task[tid][ev] = {
                "t_mono_ns": t_mono,
                "t_epoch_utc": t_epoch,
                "metadata": meta,
            }

    # 1. State Invariant Verification & Queue Reconstruction
    enq_events = [e for e in all_events if e["event"] == "redis_enqueue_complete"]
    deq_events = [e for e in all_events if e["event"] == "redis_dequeue_complete"]
    ack_events = [e for e in all_events if e["event"] == "redis_ack_complete"]

    intervals_queue = [] # (enq_mono, deq_mono, task_id)
    intervals_worker = [] # (deq_mono, ack_mono, task_id)

    for tid, evs in events_by_task.items():
        if "redis_enqueue_complete" in evs and "redis_dequeue_complete" in evs:
            intervals_queue.append((evs["redis_enqueue_complete"]["t_mono_ns"], evs["redis_dequeue_complete"]["t_mono_ns"], tid))
        if "redis_dequeue_complete" in evs and "redis_ack_complete" in evs:
            intervals_worker.append((evs["redis_dequeue_complete"]["t_mono_ns"], evs["redis_ack_complete"]["t_mono_ns"], tid))

    all_times = sorted([t for inv in intervals_queue for t in inv[:2]] + [t for inv in intervals_worker for t in inv[:2]])
    t_start = all_times[0] if all_times else 0
    t_end = all_times[-1] if all_times else 1

    # Sample queue depth and worker occupancy across 1,000 points
    n_samples = 1000
    step = (t_end - t_start) / n_samples if t_end > t_start else 1
    q_depths = []
    w_occupancies = []
    starve_samples = 0
    q_pos_samples = 0
    w_saturated_when_q_pos = 0

    for i in range(n_samples):
        t = t_start + i * step
        qd = sum(1 for (enq, deq, _) in intervals_queue if enq <= t < deq)
        wa = sum(1 for (deq, ack, _) in intervals_worker if deq <= t < ack)
        q_depths.append(qd)
        w_occupancies.append(wa)

        if qd > 0:
            q_pos_samples += 1
            if wa >= concurrency:
                w_saturated_when_q_pos += 1
            # Check if dispatcher is asleep at time t
            is_asleep = any(st <= t < et for (st, et, _, _) in sleep_intervals)
            if wa < concurrency and is_asleep:
                starve_samples += 1

    starvation_ratio = round((starve_samples / q_pos_samples) * 100.0, 2) if q_pos_samples else 0.0
    saturation_ratio = round((w_saturated_when_q_pos / q_pos_samples) * 100.0, 2) if q_pos_samples else 0.0

    # 2. Wake-up Delay & Slot Release Analysis
    slot_releases = [e for e in all_events if e["event"] == "worker_slot_released"]
    sleep_lags = []
    releases_during_sleep = 0
    releases_while_awake = 0

    for rel in slot_releases:
        t_rel = rel["t_mono_ns"]
        matching_sleep = next(((st, et, sid, liter) for (st, et, sid, liter) in sleep_intervals if st <= t_rel <= et), None)
        if matching_sleep:
            releases_during_sleep += 1
            lag_ms = (matching_sleep[1] - t_rel) / 1_000_000.0
            sleep_lags.append(lag_ms)
        else:
            releases_while_awake += 1

    pct_releases_during_sleep = round((releases_during_sleep / len(slot_releases) * 100.0), 2) if slot_releases else 0.0

    # 3. Redis Dequeue Duration & Loop Overhead
    deq_starts = {e["metadata"].get("iter_id"): e["t_mono_ns"] for e in all_events if e["event"] == "dispatcher_dequeue_start" and e.get("metadata")}
    deq_completes = [e for e in all_events if e["event"] == "redis_dequeue_complete"]
    dequeue_durations = []

    for dc in deq_completes:
        iter_id = dc.get("metadata", {}).get("iter_id")
        if iter_id and iter_id in deq_starts:
            d_ms = (dc["t_mono_ns"] - deq_starts[iter_id]) / 1_000_000.0
            dequeue_durations.append(d_ms)

    # Loop overhead immediately after sleep ends
    loop_overheads = []
    for st, et, sid, liter in sleep_intervals:
        matching_deq_start = next((e["t_mono_ns"] for e in all_events if e["event"] == "dispatcher_dequeue_start" and e.get("metadata", {}).get("loop_iter") == liter + 1 and e["t_mono_ns"] >= et), None)
        if matching_deq_start:
            loop_overheads.append((matching_deq_start - et) / 1_000_000.0)

    # 4. FIFO Replenishment Lag Matching (worker_slot_released -> subsequent dispatcher_task_submitted)
    task_submits = [e for e in all_events if e["event"] == "dispatcher_task_submitted"]
    replenish_lags = []
    rel_idx = 0
    for sub in task_submits:
        t_sub = sub["t_mono_ns"]
        while rel_idx < len(slot_releases) and slot_releases[rel_idx]["t_mono_ns"] < t_sub:
            lag_ms = (t_sub - slot_releases[rel_idx]["t_mono_ns"]) / 1_000_000.0
            replenish_lags.append(lag_ms)
            rel_idx += 1
            break

    # 5. Direct Dispatcher State Comparison (_active_tasks vs. occupancy proxy)
    state_diffs = []
    for sub in task_submits:
        t_sub = sub["t_mono_ns"]
        internal_active = sub.get("metadata", {}).get("active_tasks")
        if internal_active is not None:
            proxy_active = sum(1 for (deq, ack, _) in intervals_worker if deq <= t_sub < ack)
            state_diffs.append(abs(internal_active - proxy_active))

    mean_state_drift = round(statistics.mean(state_diffs), 3) if state_diffs else 0.0

    return {
        "event_counts": dict(event_counts),
        "invariants": {
            "min_logical_queue_depth": min(q_depths) if q_depths else 0,
            "min_worker_occupancy": min(w_occupancies) if w_occupancies else 0,
            "enqueues_count": len(enq_events),
            "dequeues_count": len(deq_events),
            "acks_count": len(ack_events),
            "balance_verified": (len(enq_events) == len(deq_events) == len(ack_events)),
        },
        "starvation_analysis": {
            "starvation_ratio_pct": starvation_ratio,
            "saturation_ratio_pct": saturation_ratio,
            "max_queue_depth": max(q_depths) if q_depths else 0,
            "mean_queue_depth": round(statistics.mean(q_depths), 2) if q_depths else 0.0,
            "max_active_workers": max(w_occupancies) if w_occupancies else 0,
            "mean_active_workers": round(statistics.mean(w_occupancies), 2) if w_occupancies else 0.0,
        },
        "sleep_lag_analysis": {
            "releases_during_sleep_count": releases_during_sleep,
            "releases_while_awake_count": releases_while_awake,
            "pct_releases_during_sleep": pct_releases_during_sleep,
            "distribution": compute_distribution(sleep_lags),
        },
        "loop_overhead_analysis": {
            "distribution": compute_distribution(loop_overheads),
        },
        "dequeue_execution_analysis": {
            "distribution": compute_distribution(dequeue_durations),
        },
        "replenish_lag_analysis": {
            "distribution": compute_distribution(replenish_lags),
        },
        "state_comparison": {
            "mean_state_drift": mean_state_drift,
            "max_state_drift": max(state_diffs) if state_diffs else 0,
        },
    }


def run_trial(
    workload_cfg: dict[str, Any],
    trial_id: str,
    enable_profiling: bool,
    raw_dir: Path,
    redis_client: Any,
) -> dict[str, Any]:
    wid = workload_cfg["workload_id"]
    name = workload_cfg["name"]
    concurrency = workload_cfg["concurrency"]
    task_type = workload_cfg["task_type"]
    payload = workload_cfg["payload"]
    measured_count = workload_cfg["measured_tasks"]
    warmup_count = workload_cfg["warmup_tasks"]
    priority = workload_cfg["priority"]

    truncate_benchmark_tables()

    profiler = DiagnosticProfiler.get_instance()
    raw_events_path = raw_dir / f"events_s2a_{wid}_{trial_id}_{int(time.time())}.jsonl"

    if enable_profiling:
        profiler.start(raw_events_path)

    queue_name = f"{BENCHMARK_QUEUE_PREFIX}{wid}:{trial_id}:{uuid.uuid4().hex[:6]}"
    broker = RedisBroker(redis_client, queue_name=queue_name, visibility_timeout_seconds=30)

    outbox_pub = OutboxPublisherWorker(poll_interval=0.05, broker=broker)
    outbox_pub.start()

    dispatcher = WorkerDispatcher(
        worker_id=f"g93-s2a-worker-{wid}-{trial_id}",
        concurrency=concurrency,
        broker=broker,
        poll_interval=0.05,
    )
    dispatcher.start()

    client = TestClient(app)

    print(f"  --> [{wid}] {trial_id} | profiling={enable_profiling} | tasks={measured_count} (warmup={warmup_count}) | concurrency={concurrency}")

    # 1. Warmup Window
    warmup_ids = []
    for i in range(warmup_count):
        res = client.post(
            "/api/v1/tasks",
            json={
                "task_name": f"warmup-s2a-{wid}-{i}",
                "task_type": task_type,
                "payload": payload,
                "priority": priority,
            },
        )
        assert res.status_code == 201
        warmup_ids.append(res.json()["id"])

    w_start = time.time()
    while time.time() - w_start < 25.0:
        with SessionLocal() as db:
            completed_warmup = db.execute(
                text("SELECT COUNT(*) FROM tasks WHERE id = ANY(:ids) AND status = 'SUCCESS'"),
                {"ids": warmup_ids},
            ).scalar()
            if completed_warmup == warmup_count:
                break
        time.sleep(0.05)

    # 2. Measured Window
    task_ids = []
    submission_lock = threading.Lock()

    def submit_one(idx: int):
        res = client.post(
            "/api/v1/tasks",
            json={
                "task_name": f"s2a-{wid}-{trial_id}-{idx}",
                "task_type": task_type,
                "payload": payload,
                "priority": priority,
            },
        )
        if res.status_code == 201:
            with submission_lock:
                task_ids.append(res.json()["id"])

    start_wall = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(submit_one, i) for i in range(measured_count)]
        for f in as_completed(futures):
            f.result()

    assert len(task_ids) == measured_count, f"Submitted {len(task_ids)} != {measured_count}"

    deadline = time.time() + 90.0
    completed_ids = set()
    while time.time() < deadline:
        with SessionLocal() as db:
            rows = db.execute(
                text("SELECT id FROM tasks WHERE id = ANY(:ids) AND status IN ('SUCCESS', 'FAILURE', 'DEAD_LETTER')"),
                {"ids": task_ids},
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

    achieved_tps = round(len(completed_ids) / total_duration_sec, 2)

    trial_summary = {
        "trial_id": trial_id,
        "profiling_enabled": enable_profiling,
        "measured_tasks": measured_count,
        "completed_tasks": len(completed_ids),
        "duration_seconds": round(total_duration_sec, 3),
        "throughput_tps": achieved_tps,
        "flush_report": flush_report,
        "raw_events_path": str(raw_events_path) if enable_profiling else None,
        "raw_events_sha256": raw_hash if enable_profiling else None,
    }

    if enable_profiling and raw_events_path.exists():
        analysis = analyze_phase2a_events(raw_events_path, concurrency)
        trial_summary["diagnostic_analysis"] = analysis

    return trial_summary


def main():
    print("================================================================================")
    print("Gate G9.3 — Stage 2 Phase 2A: Dispatcher Scheduling Diagnostic Campaign")
    print("================================================================================")

    defs_path = REPO_ROOT / "benchmarks/scenarios/g90/workload_definitions.json"
    workload_configs = json.loads(defs_path.read_text(encoding="utf-8"))["workloads"]
    defs_hash = hashlib.sha256(defs_path.read_bytes()).hexdigest()

    raw_dir = REPO_ROOT / "benchmarks/raw/g93_s2a"
    raw_dir.mkdir(parents=True, exist_ok=True)
    redis_client = create_redis_client()

    campaign_results: dict[str, Any] = {
        "milestone": "G9.3_stage2_phase2a_diagnostic_baseline",
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "git_provenance": {
            "base_authorized_sha": "61f398f",
            "protocol_sha": "2533717",
            "frozen_baseline_tag": "v0.8.0-phase8-freeze",
            "frozen_baseline_commit": "da27047f56fe2eec75acdb8502f505542cf61202",
        },
        "workload_definitions_sha256": defs_hash,
        "workloads": {},
    }

    # 3 Paired Interleaved Trials: D1, E1, D2, E2, D3, E3
    trial_plan = [
        ("pair_1_disabled", False),
        ("pair_1_enabled", True),
        ("pair_2_disabled", False),
        ("pair_2_enabled", True),
        ("pair_3_disabled", False),
        ("pair_3_enabled", True),
    ]

    for wl in workload_configs:
        wid = wl["workload_id"]
        print(f"\n>>> Running 3-Paired Interleaved Campaign for {wid} ({wl['name']}, n={wl['measured_tasks']}, c={wl['concurrency']}) <<<")
        wl_trials = []

        for tid, prof_enabled in trial_plan:
            res = run_trial(wl, tid, prof_enabled, raw_dir, redis_client)
            wl_trials.append(res)

        # Aggregate disabled vs enabled
        d_trials = [t for t in wl_trials if not t["profiling_enabled"]]
        e_trials = [t for t in wl_trials if t["profiling_enabled"]]

        d_durations = [t["duration_seconds"] for t in d_trials]
        e_durations = [t["duration_seconds"] for t in e_trials]
        d_tps = [t["throughput_tps"] for t in d_trials]
        e_tps = [t["throughput_tps"] for t in e_trials]

        dur_delta_pct = round(((statistics.mean(e_durations) - statistics.mean(d_durations)) / statistics.mean(d_durations)) * 100.0, 2)
        tps_delta_pct = round(((statistics.mean(e_tps) - statistics.mean(d_tps)) / statistics.mean(d_tps)) * 100.0, 2)

        # Aggregate diagnostic metrics across enabled trials
        starve_ratios = [t["diagnostic_analysis"]["starvation_analysis"]["starvation_ratio_pct"] for t in e_trials]
        pct_releases_during_sleep = [t["diagnostic_analysis"]["sleep_lag_analysis"]["pct_releases_during_sleep"] for t in e_trials]
        sleep_lag_p50s = [t["diagnostic_analysis"]["sleep_lag_analysis"]["distribution"]["p50_ms"] for t in e_trials]
        replenish_p50s = [t["diagnostic_analysis"]["replenish_lag_analysis"]["distribution"]["p50_ms"] for t in e_trials]
        deq_p50s = [t["diagnostic_analysis"]["dequeue_execution_analysis"]["distribution"]["p50_ms"] for t in e_trials]
        loop_p50s = [t["diagnostic_analysis"]["loop_overhead_analysis"]["distribution"]["p50_ms"] for t in e_trials]

        campaign_results["workloads"][wid] = {
            "workload_definition": wl,
            "trials": wl_trials,
            "paired_overhead_comparison": {
                "disabled_duration_mean_s": round(statistics.mean(d_durations), 3),
                "enabled_duration_mean_s": round(statistics.mean(e_durations), 3),
                "duration_delta_pct": dur_delta_pct,
                "disabled_throughput_mean_tps": round(statistics.mean(d_tps), 2),
                "enabled_throughput_mean_tps": round(statistics.mean(e_tps), 2),
                "throughput_delta_pct": tps_delta_pct,
                "per_pair_duration_deltas_pct": [
                    round(((e_trials[i]["duration_seconds"] - d_trials[i]["duration_seconds"]) / d_trials[i]["duration_seconds"]) * 100.0, 2)
                    for i in range(3)
                ],
                "per_pair_throughput_deltas_pct": [
                    round(((e_trials[i]["throughput_tps"] - d_trials[i]["throughput_tps"]) / d_trials[i]["throughput_tps"]) * 100.0, 2)
                    for i in range(3)
                ],
            },
            "diagnostic_summary": {
                "mean_starvation_ratio_pct": round(statistics.mean(starve_ratios), 2),
                "mean_pct_releases_during_sleep": round(statistics.mean(pct_releases_during_sleep), 2),
                "mean_sleep_lag_p50_ms": round(statistics.mean(sleep_lag_p50s), 2),
                "mean_replenish_lag_p50_ms": round(statistics.mean(replenish_p50s), 2),
                "mean_dequeue_exec_p50_ms": round(statistics.mean(deq_p50s), 2),
                "mean_loop_overhead_p50_ms": round(statistics.mean(loop_p50s), 2),
            },
        }

    report_path = REPO_ROOT / "benchmarks/scenarios/g93/stage2_phase2a_report.json"
    report_path.write_text(json.dumps(campaign_results, indent=2), encoding="utf-8")
    print(f"\n[DONE] Stage 2 Phase 2A Campaign Complete. Report saved to: {report_path}")


if __name__ == "__main__":
    main()
