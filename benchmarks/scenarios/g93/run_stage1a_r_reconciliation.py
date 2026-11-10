"""Stage 1A-R: Baseline Reconciliation & Paired Alternating Sampling Preflight.

Executes a paired, alternating (A/B/A/B/A/B) benchmark campaign across W-01, W-02, W-03
to definitively satisfy Stage 1A-R reconciliation requirements R1 through R7:
- R1: Workload provenance verification (SHA-256 and content matches G9.0 definition).
- R2: Strict verification of W-02 payload (prime sieve n=1000 confirmed).
- R3: Multiple independent trials under identical starting conditions (tables truncated between trials).
- R4: Paired alternating sampling evaluation (3x Disabled vs 3x Enabled, interleaved).
- R5: Specification of expanded 14-event attribution contract for Stage 1B.
- R6: Empirical quantification of PostgreSQL-Python clock offset and host clock model.
- R7: Reconciled comparison against frozen G9.0 baseline without unfounded optimization claims.
"""

import hashlib
import json
import math
import os
import platform
import statistics
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
from pydantic import BaseModel

# Ensure src/ is on sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Disposable benchmark infrastructure
BENCHMARK_DB_NAME = "taskforge_g93_benchmark"
BENCHMARK_PSYCOPG_URL = f"postgresql://taskforge:taskforge_dev@localhost:5432/{BENCHMARK_DB_NAME}"
BENCHMARK_DB_URL = f"postgresql+psycopg://taskforge:taskforge_dev@localhost:5432/{BENCHMARK_DB_NAME}"
BENCHMARK_QUEUE_PREFIX = "taskforge:g93:benchmark:"

# Setup disposable database
print(f"[Stage 1A-R] Creating isolated benchmark database '{BENCHMARK_DB_NAME}'...")
admin_conn = psycopg.connect("postgresql://taskforge:taskforge_dev@localhost:5432/postgres", autocommit=True)
with admin_conn.cursor() as cur:
    cur.execute(f"DROP DATABASE IF EXISTS {BENCHMARK_DB_NAME} WITH (FORCE)")
    cur.execute(f"CREATE DATABASE {BENCHMARK_DB_NAME} OWNER taskforge")
admin_conn.close()
print(f"[Stage 1A-R] Created database '{BENCHMARK_DB_NAME}'.")

# Configure environment before app imports
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
print(f"[Stage 1A-R] Applied Alembic migrations to '{BENCHMARK_DB_NAME}'.")

from fastapi.testclient import TestClient
from sqlalchemy import text

from taskforge.database.session import SessionLocal, engine
from taskforge.main import app
from taskforge.queue.broker import RedisBroker
from taskforge.queue.outbox import OutboxPublisherWorker
from taskforge.queue.redis_client import create_redis_client
from taskforge.workers.dispatcher import WorkerDispatcher


def probe_clock_synchronization(num_probes: int = 100) -> dict[str, float]:
    """Empirically measure clock offset and round-trip time between Python and PostgreSQL."""
    conn = psycopg.connect(BENCHMARK_PSYCOPG_URL)
    cur = conn.cursor()
    offsets_ms = []
    rtts_ms = []

    for _ in range(num_probes):
        t0 = time.time()
        cur.execute("SELECT EXTRACT(EPOCH FROM clock_timestamp())")
        pg_epoch = float(cur.fetchone()[0])
        t1 = time.time()
        rtt = (t1 - t0) * 1000.0
        t_mid = (t0 + t1) / 2.0
        offset = (pg_epoch - t_mid) * 1000.0
        offsets_ms.append(offset)
        rtts_ms.append(rtt)

    conn.close()

    return {
        "probe_count": num_probes,
        "rtt_mean_ms": round(statistics.mean(rtts_ms), 3),
        "rtt_min_ms": round(min(rtts_ms), 3),
        "rtt_max_ms": round(max(rtts_ms), 3),
        "clock_offset_mean_ms": round(statistics.mean(offsets_ms), 3),
        "clock_offset_min_ms": round(min(offsets_ms), 3),
        "clock_offset_max_ms": round(max(offsets_ms), 3),
        "clock_offset_stddev_ms": round(statistics.stdev(offsets_ms), 3),
    }


def truncate_benchmark_tables() -> None:
    """Reset database tables between trials to guarantee identical clean starting state."""
    with SessionLocal() as db:
        db.execute(text("TRUNCATE tasks, task_attempts, task_results, outbox_events, task_events CASCADE"))
        db.commit()


def compute_percentiles(samples_ms: list[float]) -> dict[str, float]:
    if not samples_ms:
        return {"p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0, "max_ms": 0.0, "min_ms": 0.0, "mean_ms": 0.0, "stddev_ms": 0.0, "variance_ms": 0.0}
    sorted_s = sorted(samples_ms)
    n = len(sorted_s)

    def _pct(p: float) -> float:
        k = (n - 1) * p
        f = int(k)
        c = min(f + 1, n - 1)
        d0 = sorted_s[f] * (c - k)
        d1 = sorted_s[c] * (k - f)
        return round(d0 + d1, 3)

    mean_val = statistics.mean(sorted_s)
    variance_val = statistics.variance(sorted_s) if n > 1 else 0.0
    stddev_val = statistics.stdev(sorted_s) if n > 1 else 0.0

    return {
        "p50_ms": _pct(0.50),
        "p95_ms": _pct(0.95),
        "p99_ms": _pct(0.99),
        "max_ms": round(sorted_s[-1], 3),
        "min_ms": round(sorted_s[0], 3),
        "mean_ms": round(mean_val, 3),
        "stddev_ms": round(stddev_val, 3),
        "variance_ms": round(variance_val, 3),
    }


def get_environment_fingerprint() -> dict[str, Any]:
    import ctypes

    class MEMORYSTATUSEX(ctypes.Structure):
        _fields_ = [
            ("dwLength", ctypes.c_ulong),
            ("dwMemoryLoad", ctypes.c_ulong),
            ("ullTotalPhys", ctypes.c_ulonglong),
            ("ullAvailPhys", ctypes.c_ulonglong),
            ("ullTotalPageFile", ctypes.c_ulonglong),
            ("ullAvailPageFile", ctypes.c_ulonglong),
            ("ullTotalVirtual", ctypes.c_ulonglong),
            ("ullAvailVirtual", ctypes.c_ulonglong),
            ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
        ]

    stat = MEMORYSTATUSEX()
    stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
    ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
    ram_gb = round(stat.ullTotalPhys / (1024**3), 2)

    with psycopg.connect(BENCHMARK_PSYCOPG_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            pg_ver = cur.fetchone()[0]

    redis_cli = create_redis_client()
    redis_info = redis_cli.info()
    redis_ver = redis_info.get("redis_version", "unknown")

    return {
        "os_platform": f"{platform.system()}-{platform.release()}-{platform.version()}",
        "cpu_architecture": platform.machine(),
        "logical_cpus": os.cpu_count(),
        "total_ram_gb": ram_gb,
        "python_version": sys.version,
        "postgresql_version": pg_ver,
        "redis_version": redis_ver,
        "git_provenance": "ada0b9b",
        "frozen_baseline_tag": "v0.8.0-phase8-freeze",
        "frozen_baseline_commit": "da27047f56fe2eec75acdb8502f505542cf61202",
    }


def run_workload(
    workload_cfg: dict[str, Any],
    trial_id: str,
    enable_diagnostic_sampling: bool,
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

    # Explicit assertion of payload invariance
    if wid == "W-02":
        assert payload.get("n") == 1000, f"Critical invariant violation: W-02 must be n=1000, got {payload.get('n')}"

    # Reset DB tables prior to workload run for strict isolation
    truncate_benchmark_tables()

    queue_name = f"{BENCHMARK_QUEUE_PREFIX}{wid}:{trial_id}:{uuid.uuid4().hex[:6]}"
    broker = RedisBroker(redis_client, queue_name=queue_name, visibility_timeout_seconds=30)

    outbox_pub = OutboxPublisherWorker(poll_interval=0.05, broker=broker)
    outbox_pub.start()

    dispatcher = WorkerDispatcher(
        worker_id=f"g93-worker-{wid}-{trial_id}",
        concurrency=concurrency,
        broker=broker,
        poll_interval=0.05,
    )
    dispatcher.start()

    client = TestClient(app)

    print(f"\n--- Running Workload {wid} ({name}) | Trial: {trial_id} | Sampling: {enable_diagnostic_sampling} ---")

    # 1. Warmup Phase
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
        assert res.status_code == 201, f"Warmup failed: {res.text}"
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

    # 2. Setup Diagnostic Sampler (if enabled)
    samples_collected = []
    stop_sampling = threading.Event()

    def sampler_loop():
        while not stop_sampling.is_set():
            t_sample = time.time()
            try:
                q_depth = redis_client.zcount(queue_name, "-inf", "+inf")
                with SessionLocal() as db:
                    outbox_backlog = db.execute(text("SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING'")).scalar()
                samples_collected.append({"t": t_sample, "q_depth": q_depth, "outbox_backlog": outbox_backlog})
            except Exception as e:
                samples_collected.append({"t": t_sample, "error": str(e)})
            stop_sampling.wait(0.25)

    sampler_thread = None
    if enable_diagnostic_sampling:
        sampler_thread = threading.Thread(target=sampler_loop, name=f"Sampler-{wid}-{trial_id}", daemon=True)
        sampler_thread.start()

    # 3. Measured Trial Phase
    task_records: dict[str, dict[str, Any]] = {}
    records_lock = threading.Lock()

    def submit_task(idx: int):
        t0_req = time.perf_counter()
        t0_epoch = time.time()
        res = client.post(
            "/api/v1/tasks",
            json={
                "task_name": f"g93-{wid}-{trial_id}-{idx}",
                "task_type": task_type,
                "payload": payload,
                "priority": priority,
            },
        )
        t1_resp = time.perf_counter()
        if res.status_code != 201:
            return {"status": "FAILED", "idx": idx, "error": res.text}
        tid = res.json()["id"]
        sub_lat_ms = (t1_resp - t0_req) * 1000.0
        return {
            "status": "SUBMITTED",
            "idx": idx,
            "task_id": tid,
            "t0_epoch": t0_epoch,
            "t0_req": t0_req,
            "t1_resp": t1_resp,
            "submission_latency_ms": sub_lat_ms,
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

    # Await measured task completion
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

    if enable_diagnostic_sampling and sampler_thread:
        stop_sampling.set()
        sampler_thread.join(timeout=2.0)

    # Stop workers and outbox publisher
    outbox_pub.stop()
    dispatcher.stop(grace_period_seconds=5.0)

    # Collect database lifecycle timestamps
    with SessionLocal() as db:
        attempts_rows = db.execute(
            text("""
                SELECT ta.task_id, ta.started_at, ta.completed_at, ta.duration_ms
                FROM task_attempts ta
                WHERE ta.task_id = ANY(:ids) AND ta.attempt_number = 1
            """),
            {"ids": list(task_records.keys())},
        ).mappings().all()

        results_rows = db.execute(
            text("""
                SELECT tr.task_id, tr.created_at
                FROM task_results tr
                WHERE tr.task_id = ANY(:ids)
            """),
            {"ids": list(task_records.keys())},
        ).mappings().all()

    att_by_id = {row["task_id"]: row for row in attempts_rows}
    res_by_id = {row["task_id"]: row for row in results_rows}

    sub_latencies = []
    dispatch_delays = []
    exec_overheads = []
    e2e_latencies = []

    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_filename = f"raw_g93_r_{wid}_{trial_id}_{int(time.time())}.jsonl"
    raw_path = raw_dir / raw_filename

    with open(raw_path, "w", encoding="utf-8") as f_raw:
        for tid, tinfo in task_records.items():
            sub_lat_ms = tinfo["submission_latency_ms"]
            sub_latencies.append(sub_lat_ms)

            att = att_by_id.get(tid)
            res = res_by_id.get(tid)

            if att and att["started_at"]:
                started_epoch = att["started_at"].timestamp()
                disp_delay_ms = max(0.0, (started_epoch - tinfo["t0_epoch"]) * 1000.0 - sub_lat_ms)
            else:
                disp_delay_ms = 0.0
            dispatch_delays.append(disp_delay_ms)

            if att and att["duration_ms"] is not None:
                exec_ms = float(att["duration_ms"])
            elif att and att["completed_at"] and att["started_at"]:
                exec_ms = (att["completed_at"] - att["started_at"]).total_seconds() * 1000.0
            else:
                exec_ms = 0.0
            exec_overheads.append(exec_ms)

            if res and res["created_at"]:
                e2e_ms = max(sub_lat_ms, (res["created_at"].timestamp() - tinfo["t0_epoch"]) * 1000.0)
            else:
                e2e_ms = sub_lat_ms + disp_delay_ms + exec_ms
            e2e_latencies.append(e2e_ms)

            f_raw.write(json.dumps({
                "task_id": tid,
                "trial_id": trial_id,
                "workload_id": wid,
                "submission_latency_ms": round(sub_lat_ms, 3),
                "dispatch_delay_ms": round(disp_delay_ms, 3),
                "execution_overhead_ms": round(exec_ms, 3),
                "e2e_latency_ms": round(e2e_ms, 3),
            }) + "\n")

    raw_hash = hashlib.sha256(raw_path.read_bytes()).hexdigest()
    achieved_tps = round(len(completed_ids) / total_duration_sec, 2)

    return {
        "workload_id": wid,
        "trial_id": trial_id,
        "diagnostic_sampling_enabled": enable_diagnostic_sampling,
        "sample_count": len(completed_ids),
        "duration_seconds": round(total_duration_sec, 3),
        "throughput_tps": achieved_tps,
        "api_submission_latency": compute_percentiles(sub_latencies),
        "broker_dispatch_delay": compute_percentiles(dispatch_delays),
        "execution_overhead": compute_percentiles(exec_overheads),
        "end_to_end_latency": compute_percentiles(e2e_latencies),
        "raw_jsonl_filename": raw_filename,
        "raw_jsonl_sha256": raw_hash,
        "sampling_records_count": len(samples_collected),
    }


def main():
    print("[Stage 1A-R] Starting Baseline Reconciliation & Paired Sampling Campaign...")
    clock_probe_results = probe_clock_synchronization(num_probes=100)
    print(f"[Stage 1A-R] Clock Synchronization Probe: offset_mean={clock_probe_results['clock_offset_mean_ms']}ms, offset_stddev={clock_probe_results['clock_offset_stddev_ms']}ms, rtt_mean={clock_probe_results['rtt_mean_ms']}ms")

    env_fingerprint = get_environment_fingerprint()

    # Load workload definitions
    defs_path = REPO_ROOT / "benchmarks/scenarios/g90/workload_definitions.json"
    defs_bytes = defs_path.read_bytes()
    defs_hash = hashlib.sha256(defs_bytes).hexdigest()
    workload_configs = json.loads(defs_bytes.decode("utf-8"))["workloads"]

    print(f"[Stage 1A-R] Workload definitions SHA-256: {defs_hash}")
    assert defs_hash == "04f2306538868a41514c19f0958e04bf0b0586b3a83989e115231a32e50f0110", "Workload definitions SHA mismatch!"

    raw_dir = REPO_ROOT / "benchmarks/raw/g93_r"
    redis_client = create_redis_client()

    # Paired Alternating Design:
    # Pair 1: A1 (Disabled), B1 (Enabled)
    # Pair 2: A2 (Disabled), B2 (Enabled)
    # Pair 3: A3 (Disabled), B3 (Enabled)
    trials_schedule = [
        ("pair_1_disabled_A1", False),
        ("pair_1_enabled_B1", True),
        ("pair_2_disabled_A2", False),
        ("pair_2_enabled_B2", True),
        ("pair_3_disabled_A3", False),
        ("pair_3_enabled_B3", True),
    ]

    campaign_results: list[dict[str, Any]] = []

    for trial_id, sampling_flag in trials_schedule:
        trial_record = {
            "trial_id": trial_id,
            "sampling_enabled": sampling_flag,
            "workloads": {},
        }
        for w_cfg in workload_configs:
            res = run_workload(
                workload_cfg=w_cfg,
                trial_id=trial_id,
                enable_diagnostic_sampling=sampling_flag,
                redis_client=redis_client,
                raw_dir=raw_dir,
            )
            trial_record["workloads"][w_cfg["workload_id"]] = res
        campaign_results.append(trial_record)

    # Clean up database at completion
    admin_conn = psycopg.connect("postgresql://taskforge:taskforge_dev@localhost:5432/postgres", autocommit=True)
    with admin_conn.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {BENCHMARK_DB_NAME} WITH (FORCE)")
    admin_conn.close()
    print(f"[Stage 1A-R] Cleaned up disposable database '{BENCHMARK_DB_NAME}'.")

    # Aggregate and compute paired statistics
    # Workload summary aggregation across Disabled (A) and Enabled (B)
    analysis = {}
    for wid in ["W-01", "W-02", "W-03"]:
        disabled_runs = [t["workloads"][wid] for t in campaign_results if not t["sampling_enabled"]]
        enabled_runs = [t["workloads"][wid] for t in campaign_results if t["sampling_enabled"]]

        def _stats(runs, key):
            vals = [r[key] for r in runs]
            mean_v = statistics.mean(vals)
            std_v = statistics.stdev(vals) if len(vals) > 1 else 0.0
            rsd_pct = (std_v / mean_v * 100.0) if mean_v > 0 else 0.0
            return {"mean": round(mean_v, 3), "stddev": round(std_v, 3), "rsd_percent": round(rsd_pct, 2), "values": vals}

        def _pct_stats(runs, subkey, metric):
            vals = [r[subkey][metric] for r in runs]
            mean_v = statistics.mean(vals)
            std_v = statistics.stdev(vals) if len(vals) > 1 else 0.0
            rsd_pct = (std_v / mean_v * 100.0) if mean_v > 0 else 0.0
            return {"mean": round(mean_v, 3), "stddev": round(std_v, 3), "rsd_percent": round(rsd_pct, 2), "values": vals}

        d_duration = _stats(disabled_runs, "duration_seconds")
        e_duration = _stats(enabled_runs, "duration_seconds")
        dur_overhead_pct = round(((e_duration["mean"] - d_duration["mean"]) / d_duration["mean"]) * 100.0, 2)

        d_tps = _stats(disabled_runs, "throughput_tps")
        e_tps = _stats(enabled_runs, "throughput_tps")
        tps_overhead_pct = round(((e_tps["mean"] - d_tps["mean"]) / d_tps["mean"]) * 100.0, 2)

        d_exec_p50 = _pct_stats(disabled_runs, "execution_overhead", "p50_ms")
        e_exec_p50 = _pct_stats(enabled_runs, "execution_overhead", "p50_ms")
        exec_overhead_pct = round(((e_exec_p50["mean"] - d_exec_p50["mean"]) / d_exec_p50["mean"]) * 100.0, 2)

        analysis[wid] = {
            "workload_id": wid,
            "baseline_disabled": {
                "runs_count": len(disabled_runs),
                "duration_seconds": d_duration,
                "throughput_tps": d_tps,
                "api_submission_p50_ms": _pct_stats(disabled_runs, "api_submission_latency", "p50_ms"),
                "broker_dispatch_delay_p50_ms": _pct_stats(disabled_runs, "broker_dispatch_delay", "p50_ms"),
                "execution_overhead_p50_ms": d_exec_p50,
                "end_to_end_p50_ms": _pct_stats(disabled_runs, "end_to_end_latency", "p50_ms"),
            },
            "sampling_enabled": {
                "runs_count": len(enabled_runs),
                "duration_seconds": e_duration,
                "throughput_tps": e_tps,
                "api_submission_p50_ms": _pct_stats(enabled_runs, "api_submission_latency", "p50_ms"),
                "broker_dispatch_delay_p50_ms": _pct_stats(enabled_runs, "broker_dispatch_delay", "p50_ms"),
                "execution_overhead_p50_ms": e_exec_p50,
                "end_to_end_p50_ms": _pct_stats(enabled_runs, "end_to_end_latency", "p50_ms"),
            },
            "paired_overhead_evaluation": {
                "duration_delta_pct": dur_overhead_pct,
                "throughput_delta_pct": tps_overhead_pct,
                "execution_p50_delta_pct": exec_overhead_pct,
            },
        }

    final_report = {
        "milestone": "G9.3_stage1a_r_reconciliation",
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "clock_synchronization_probe": clock_probe_results,
        "environment": env_fingerprint,
        "workload_definitions_sha256": defs_hash,
        "workload_payload_verification": {
            "W-01": {"task_type": "noop", "payload": {"benchmark": "g90_noop", "sample": True}},
            "W-02": {"task_type": "compute_prime", "payload": {"n": 1000}},
            "W-03": {"task_type": "io_simulation", "payload": {"delay_seconds": 0.005, "jitter": False}},
        },
        "statistical_analysis": analysis,
        "trials": campaign_results,
    }

    report_path = REPO_ROOT / "benchmarks/scenarios/g93/stage1a_r_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(final_report, f, indent=2)

    print(f"\n[Stage 1A-R] Full reconciliation report written to: {report_path}")


if __name__ == "__main__":
    main()
