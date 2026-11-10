#!/usr/bin/env python3
"""Stage 2 Phase 2C — Post-Adoption Rebaselining & Sequential Dequeue Investigation Campaign.

Executes an 18-trial diagnostic campaign to rebaseline the production-adopted dispatcher
(enable_capacity_wakeup=True) and test hypotheses H2-A, H2-B, and H2-C:
  - Series 1: Post-adoption baseline across W-01 (noop, c=10), W-02 (prime, c=4), W-03 (io, c=10).
  - Series 2: Concurrency scaling across W-01 at c=4, c=10, c=20.
Captures Redis commandstats deltas, unambiguous 1-to-1 replenishment matching,
cohort replenishment latency, dispatcher active-duty ratio, and process CPU time.
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
BENCHMARK_QUEUE_PREFIX = "taskforge:g93:s2c:"

os.environ["DATABASE_URL"] = BENCHMARK_DB_URL
os.environ["TASKFORGE_TESTING"] = "1"
os.environ["APP_ENV"] = "development"

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


def analyze_phase2c_events(events_path: Path, concurrency: int) -> dict[str, Any]:
    lines = [json.loads(line) for line in events_path.read_text(encoding="utf-8").strip().splitlines() if line.strip()]

    event_counts = defaultdict(int)
    all_events = []
    events_by_task = defaultdict(dict)
    sleep_intervals = []
    sleep_starts = {}

    deq_durations_ms = []
    loop_overheads_ms = []

    iter_deq_start = {}
    iter_deq_complete = {}
    iter_task_submit = {}

    for entry in lines:
        ev = entry["event"]
        tid = entry.get("task_id")
        t_mono = entry["t_mono_ns"]
        t_epoch = entry["t_epoch_utc"]
        meta = entry.get("metadata") or {}
        iter_id = meta.get("iter_id")

        event_counts[ev] += 1
        all_events.append(entry)

        if ev == "dispatcher_sleep_start":
            sid = meta.get("sleep_id", "")
            sleep_starts[sid] = (t_mono, meta.get("loop_iter", 0), meta.get("wakeup_mode", "event_driven"))
        elif ev == "dispatcher_sleep_end":
            sid = meta.get("sleep_id", "")
            if sid in sleep_starts:
                st, liter, wmode = sleep_starts[sid]
                sleep_intervals.append((st, t_mono, sid, liter, wmode))

        elif ev == "dispatcher_dequeue_start" and iter_id:
            iter_deq_start[iter_id] = t_mono
        elif ev == "redis_dequeue_complete" and iter_id:
            iter_deq_complete[iter_id] = t_mono
            if iter_id in iter_deq_start:
                dur_ms = (t_mono - iter_deq_start[iter_id]) / 1_000_000.0
                deq_durations_ms.append(dur_ms)
        elif ev == "dispatcher_task_submitted" and iter_id:
            iter_task_submit[iter_id] = t_mono
            if iter_id in iter_deq_complete:
                loop_ms = (t_mono - iter_deq_complete[iter_id]) / 1_000_000.0
                loop_overheads_ms.append(loop_ms)

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

    intervals_queue = []
    intervals_worker = []

    for tid, evs in events_by_task.items():
        if "redis_enqueue_complete" in evs and "redis_dequeue_complete" in evs:
            intervals_queue.append((evs["redis_enqueue_complete"]["t_mono_ns"], evs["redis_dequeue_complete"]["t_mono_ns"], tid))
        if "redis_dequeue_complete" in evs and "redis_ack_complete" in evs:
            intervals_worker.append((evs["redis_dequeue_complete"]["t_mono_ns"], evs["redis_ack_complete"]["t_mono_ns"], tid))

    all_times = sorted([t for inv in intervals_queue for t in inv[:2]] + [t for inv in intervals_worker for t in inv[:2]])
    t_start = all_times[0] if all_times else 0
    t_end = all_times[-1] if all_times else 1

    # Merge overlapping [enq, deq] intervals for exact queue-positive wall clock
    sorted_q_invs = sorted(intervals_queue, key=lambda x: x[0])
    merged_qp = []
    for s, e, _ in sorted_q_invs:
        if not merged_qp:
            merged_qp.append([s, e])
        elif s <= merged_qp[-1][1]:
            merged_qp[-1][1] = max(merged_qp[-1][1], e)
        else:
            merged_qp.append([s, e])

    qp_wall_ns = sum(e - s for s, e in merged_qp)
    qp_wall_sec = qp_wall_ns / 1_000_000_000.0

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
            is_asleep = any(st <= t < et for (st, et, _, _, _) in sleep_intervals)
            if wa < concurrency and is_asleep:
                starve_samples += 1

    starvation_ratio = round((starve_samples / q_pos_samples) * 100.0, 2) if q_pos_samples else 0.0
    saturation_ratio = round((w_saturated_when_q_pos / q_pos_samples) * 100.0, 2) if q_pos_samples else 0.0

    # 2. Dispatcher Active-Duty Ratio Calculation (strictly inside QP windows)
    active_in_qp_ns = 0
    for iter_id, t_sub in iter_task_submit.items():
        t_d_start = iter_deq_start.get(iter_id)
        if not t_d_start or t_sub <= t_d_start:
            continue
        # Compute overlap with merged QP intervals
        for qp_s, qp_e in merged_qp:
            overlap_s = max(t_d_start, qp_s)
            overlap_e = min(t_sub, qp_e)
            if overlap_e > overlap_s:
                active_in_qp_ns += (overlap_e - overlap_s)

    active_in_qp_sec = active_in_qp_ns / 1_000_000_000.0
    dispatcher_active_duty_pct = round((active_in_qp_sec / qp_wall_sec * 100.0), 2) if qp_wall_sec > 0 else 0.0

    # 3. Unambiguous 1-to-1 Replenishment Matching & Per-Interval Dequeue Contribution Ratio
    slot_releases = [e for e in all_events if e["event"] == "worker_slot_released"]
    task_submits = [e for e in all_events if e["event"] == "dispatcher_task_submitted"]

    matched_replenish_lags_ms = []
    per_interval_deq_ratios = []

    rel_and_sub = sorted(
        [{"type": "rel", "t": e["t_mono_ns"], "tid": e.get("task_id"), "meta": e.get("metadata", {})} for e in slot_releases] +
        [{"type": "sub", "t": e["t_mono_ns"], "tid": e.get("task_id"), "meta": e.get("metadata", {})} for e in task_submits],
        key=lambda x: x["t"]
    )

    cohort_first_replenish_ms = []
    cohort_full_replenish_ms = []

    pending_releases = []
    for item in rel_and_sub:
        if item["type"] == "rel":
            pending_releases.append(item["t"])
        elif item["type"] == "sub":
            if pending_releases:
                if len(pending_releases) == 1:
                    t_rel = pending_releases.pop(0)
                    rep_ms = (item["t"] - t_rel) / 1_000_000.0
                    matched_replenish_lags_ms.append(rep_ms)

                    sub_iter = item["meta"].get("iter_id")
                    if sub_iter and sub_iter in iter_deq_start and sub_iter in iter_deq_complete:
                        deq_ms = (iter_deq_complete[sub_iter] - iter_deq_start[sub_iter]) / 1_000_000.0
                        if rep_ms > 0:
                            ratio_pct = min(100.0, round((deq_ms / rep_ms) * 100.0, 2))
                            per_interval_deq_ratios.append(ratio_pct)
                else:
                    t_first_rel = pending_releases[0]
                    t_first_rep = (item["t"] - t_first_rel) / 1_000_000.0
                    cohort_first_replenish_ms.append(t_first_rep)
                    pending_releases.pop(0)
                    if not pending_releases:
                        cohort_full_replenish_ms.append(t_first_rep)

    # 4. End-to-End Latency per Task
    e2e_latencies = []
    for tid, evs in events_by_task.items():
        if "api_request_received" in evs and "terminal_commit" in evs:
            e2e_ms = (evs["terminal_commit"]["t_mono_ns"] - evs["api_request_received"]["t_mono_ns"]) / 1_000_000.0
            e2e_latencies.append(e2e_ms)

    return {
        "event_counts": dict(event_counts),
        "invariants": {
            "min_logical_queue_depth": min(q_depths) if q_depths else 0,
            "min_worker_occupancy": min(w_occupancies) if w_occupancies else 0,
            "enqueues_count": len(enq_events),
            "dequeues_count": len(deq_events),
            "acks_count": len(ack_events),
            "balance_verified": (len(enq_events) == len(deq_events) == len(ack_events)),
            "duplicate_submissions_count": len(deq_events) - len(set(e["task_id"] for e in deq_events if e.get("task_id"))),
        },
        "starvation_and_occupancy": {
            "starvation_ratio_pct": starvation_ratio,
            "saturation_ratio_pct": saturation_ratio,
            "max_queue_depth": max(q_depths) if q_depths else 0,
            "mean_queue_depth": round(statistics.mean(q_depths), 2) if q_depths else 0.0,
            "max_active_workers": max(w_occupancies) if w_occupancies else 0,
            "mean_active_workers": round(statistics.mean(w_occupancies), 2) if w_occupancies else 0.0,
        },
        "dispatcher_active_duty": {
            "active_in_qp_seconds": round(active_in_qp_sec, 4),
            "qp_wall_seconds": round(qp_wall_sec, 4),
            "active_duty_ratio_pct": dispatcher_active_duty_pct,
        },
        "dequeue_timing": {
            "round_trip_dist": compute_distribution(deq_durations_ms),
            "loop_overhead_dist": compute_distribution(loop_overheads_ms),
        },
        "replenishment_matching": {
            "unambiguous_matched_count": len(matched_replenish_lags_ms),
            "matched_replenish_dist": compute_distribution(matched_replenish_lags_ms),
            "per_interval_deq_ratio_pct_dist": compute_distribution(per_interval_deq_ratios),
            "cohort_first_replenish_dist": compute_distribution(cohort_first_replenish_ms),
            "cohort_full_replenish_dist": compute_distribution(cohort_full_replenish_ms),
        },
        "e2e_latency": {
            "dist": compute_distribution(e2e_latencies),
        },
    }


def run_trial(
    workload_cfg: dict[str, Any],
    series_name: str,
    trial_num: int,
    raw_dir: Path,
    redis_client: Any,
) -> dict[str, Any]:
    wid = workload_cfg["workload_id"]
    concurrency = workload_cfg["concurrency"]
    measured_count = workload_cfg["measured_tasks"]
    task_type = workload_cfg["task_type"]
    payload = workload_cfg["payload"]
    priority = workload_cfg["priority"]
    trial_id = f"s2c_{wid}_c{concurrency}_t{trial_num}"

    print(f"\n=======================================================")
    print(f"[{series_name}] Executing Trial {trial_num}/3 for {wid} (Tasks={measured_count}, Concurrency={concurrency})")
    print(f"Trial ID: {trial_id}")
    print(f"=======================================================")

    # 1. Reset infrastructure
    truncate_benchmark_tables()
    redis_client.flushdb()

    # Capture initial Redis commandstats snapshot
    cmdstats_before = redis_client.info("commandstats")

    # 2. Configure Diagnostic Profiler
    profiler = DiagnosticProfiler.get_instance()
    events_file = raw_dir / f"events_{trial_id}.jsonl"
    if events_file.exists():
        events_file.unlink()
    profiler.start(events_file)

    queue_name = f"{BENCHMARK_QUEUE_PREFIX}{wid}:{trial_id}:{uuid.uuid4().hex[:6]}"
    broker = RedisBroker(redis_client, queue_name=queue_name, visibility_timeout_seconds=30)

    publisher = OutboxPublisherWorker(broker=broker, poll_interval=0.05)
    publisher.start()

    dispatcher = WorkerDispatcher(
        worker_id=f"worker-s2c-{trial_id}",
        concurrency=concurrency,
        broker=broker,
        poll_interval=0.05,
        enable_capacity_wakeup=True,
    )
    dispatcher.start()

    client = TestClient(app)

    task_ids = []
    submission_lock = threading.Lock()

    def submit_one(idx: int):
        res = client.post(
            "/api/v1/tasks",
            json={
                "task_name": f"{trial_id}-{idx}",
                "task_type": task_type,
                "payload": payload,
                "priority": priority,
            },
        )
        if res.status_code == 201:
            with submission_lock:
                task_ids.append(res.json()["id"])

    t_cpu_start = time.process_time()
    t_wall_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=min(10, concurrency)) as pool:
        futures = [pool.submit(submit_one, i) for i in range(measured_count)]
        for f in as_completed(futures):
            f.result()

    assert len(task_ids) == measured_count, f"Submitted {len(task_ids)} != {measured_count}"

    # Drain phase
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

    t_wall_end = time.perf_counter()
    t_cpu_end = time.process_time()

    assert len(completed_ids) == measured_count, f"Only {len(completed_ids)}/{measured_count} tasks completed within timeout!"

    # Clean shutdown
    publisher.stop()
    dispatcher.stop(grace_period_seconds=2.0)
    flush_report = profiler.stop(drain_timeout=5.0)

    # Capture final Redis commandstats snapshot
    cmdstats_after = redis_client.info("commandstats")

    def _get_stat(stats: dict, key: str, field: str) -> float:
        return stats.get(key, {}).get(field, 0.0)

    evalsha_calls = _get_stat(cmdstats_after, "cmdstat_evalsha", "calls") - _get_stat(cmdstats_before, "cmdstat_evalsha", "calls")
    evalsha_usec = _get_stat(cmdstats_after, "cmdstat_evalsha", "usec") - _get_stat(cmdstats_before, "cmdstat_evalsha", "usec")
    evalsha_usec_per_call = round(evalsha_usec / evalsha_calls, 2) if evalsha_calls > 0 else 0.0

    total_cmds_after = sum(d.get("calls", 0) for d in cmdstats_after.values())
    total_cmds_before = sum(d.get("calls", 0) for d in cmdstats_before.values())
    total_redis_cmds = total_cmds_after - total_cmds_before

    total_duration_sec = t_wall_end - t_wall_start
    total_cpu_sec = t_cpu_end - t_cpu_start
    throughput_tps = round(measured_count / total_duration_sec, 2)

    sha256_hash = hashlib.sha256(events_file.read_bytes()).hexdigest()
    analysis = analyze_phase2c_events(events_file, concurrency)

    trial_summary = {
        "trial_id": trial_id,
        "series": series_name,
        "workload_id": wid,
        "num_tasks": measured_count,
        "concurrency": concurrency,
        "duration_sec": round(total_duration_sec, 3),
        "total_cpu_sec": round(total_cpu_sec, 3),
        "throughput_tps": throughput_tps,
        "raw_events_path": str(events_file),
        "raw_events_sha256": sha256_hash,
        "unpersisted_records": flush_report.get("unpersisted_records", 0),
        "dropped_records": flush_report.get("dropped_records", 0),
        "redis_commandstats": {
            "evalsha_calls": evalsha_calls,
            "evalsha_total_usec": evalsha_usec,
            "evalsha_usec_per_call": evalsha_usec_per_call,
            "total_redis_commands": total_redis_cmds,
        },
        "analysis": analysis,
    }

    print(f"Results for {trial_id}:")
    print(f"  Duration: {trial_summary['duration_sec']}s, TPS: {throughput_tps}, CPU: {trial_summary['total_cpu_sec']}s")
    print(f"  Redis Dequeue Round-Trip (p50): {analysis['dequeue_timing']['round_trip_dist']['p50_ms']} ms")
    print(f"  Matched Replenish Lag (p50): {analysis['replenishment_matching']['matched_replenish_dist']['p50_ms']} ms")
    print(f"  Per-Interval Deq Ratio (p50): {analysis['replenishment_matching']['per_interval_deq_ratio_pct_dist']['p50_ms']}%")
    print(f"  Dispatcher Active-Duty: {analysis['dispatcher_active_duty']['active_duty_ratio_pct']}% (Active: {analysis['dispatcher_active_duty']['active_in_qp_seconds']}s / QP: {analysis['dispatcher_active_duty']['qp_wall_seconds']}s)")
    print(f"  Worker Saturation Ratio: {analysis['starvation_and_occupancy']['saturation_ratio_pct']}%")
    print(f"  Redis evalsha: {evalsha_calls} calls, {evalsha_usec_per_call} us/call")

    return trial_summary


def main() -> None:
    raw_dir = REPO_ROOT / "benchmarks" / "raw" / "g93_s2c"
    raw_dir.mkdir(parents=True, exist_ok=True)

    redis_client = create_redis_client()

    defs_path = REPO_ROOT / "benchmarks/scenarios/g90/workload_definitions.json"
    defs_json = json.loads(defs_path.read_text(encoding="utf-8"))
    workload_configs = {w["workload_id"]: w for w in defs_json["workloads"]}
    defs_hash = hashlib.sha256(defs_path.read_bytes()).hexdigest()

    w01 = workload_configs["W-01"]
    w02 = workload_configs["W-02"]
    w03 = workload_configs["W-03"]

    # Series 1: Post-Adoption Workload Baseline
    series_1_workloads = [w01, w02, w03]

    # Series 2: Concurrency Scaling on W-01
    w01_c4 = dict(w01, concurrency=4)
    w01_c10 = dict(w01, concurrency=10)
    w01_c20 = dict(w01, concurrency=20)
    series_2_workloads = [w01_c4, w01_c10, w01_c20]

    all_trials = []

    print("=================================================================")
    print("STARTING STAGE 2 PHASE 2C DIAGNOSTIC REBASELINING CAMPAIGN (18 TRIALS)")
    print("=================================================================")

    # Execute Series 1
    for w_cfg in series_1_workloads:
        for t_idx in range(1, 4):
            trial = run_trial(w_cfg, "Series-1-Post-Adoption-Baseline", t_idx, raw_dir, redis_client)
            all_trials.append(trial)

    # Execute Series 2
    for w_cfg in series_2_workloads:
        for t_idx in range(1, 4):
            trial = run_trial(w_cfg, "Series-2-Concurrency-Scaling", t_idx, raw_dir, redis_client)
            all_trials.append(trial)

    report = {
        "metadata": {
            "campaign": "Gate G9.3 Stage 2 Phase 2C Post-Adoption Rebaselining & Sequential Dequeue Investigation",
            "timestamp_utc": datetime.now(UTC).isoformat(),
            "git_commit": "a89858b73f8488e05cbcc2393a61f3080ffc7a38",
            "workload_definitions_sha256": defs_hash,
            "trials_count": len(all_trials),
        },
        "trials": all_trials,
    }

    report_path = REPO_ROOT / "benchmarks" / "scenarios" / "g93" / "stage2_phase2c_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\n=======================================================")
    print(f"CAMPAIGN COMPLETE: All 18 trials executed successfully.")
    print(f"Report written to: {report_path}")
    print(f"=======================================================")


if __name__ == "__main__":
    main()
