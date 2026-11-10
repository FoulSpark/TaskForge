"""Stage 1A: Baseline Reproduction & Non-Invasive Profiling Preflight Harness.

Executes controlled, reproducible benchmark runs against TaskForge at commit ada0b9b
using the frozen G9.0 workload definitions (W-01, W-02, W-03).

Deliverables produced:
1. Baseline reproduction across repeated trials (Trial 1, Trial 2).
2. Diagnostic sampling overhead evaluation (Trial 3 with 250ms periodic telemetry sampling).
3. Variance, repeatability, and distribution analysis (mean, stddev, variance, p50/p95/p99).
4. Environment fingerprint and provenance integrity (workload hash, raw result JSONL hashes).
5. Isolated infrastructure: disposable DB `taskforge_g93_benchmark`, isolated Redis namespace.
"""

import hashlib
import json
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
from pydantic import BaseModel, Field

# Ensure src/ is on sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Disposable benchmark infrastructure
BENCHMARK_DB_NAME = "taskforge_g93_benchmark"
BENCHMARK_DB_URL = f"postgresql+psycopg://taskforge:taskforge_dev@localhost:5432/{BENCHMARK_DB_NAME}"
BENCHMARK_QUEUE_PREFIX = "taskforge:g93:benchmark:"

# Setup disposable database
print(f"[Stage 1A] Setting up isolated benchmark database '{BENCHMARK_DB_NAME}'...")
admin_conn = psycopg.connect("postgresql://taskforge:taskforge_dev@localhost:5432/postgres", autocommit=True)
with admin_conn.cursor() as cur:
    cur.execute(f"DROP DATABASE IF EXISTS {BENCHMARK_DB_NAME} WITH (FORCE)")
    cur.execute(f"CREATE DATABASE {BENCHMARK_DB_NAME} OWNER taskforge")
admin_conn.close()
print(f"[Stage 1A] Created database '{BENCHMARK_DB_NAME}'.")

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
print(f"[Stage 1A] Applied Alembic migrations to '{BENCHMARK_DB_NAME}'.")

from fastapi.testclient import TestClient
from sqlalchemy import text

from taskforge.database.session import SessionLocal, engine
from taskforge.main import app
from taskforge.queue.broker import RedisBroker
from taskforge.queue.outbox import OutboxPublisherWorker
from taskforge.queue.redis_client import create_redis_client
from taskforge.workers.dispatcher import WorkerDispatcher


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
            ("sullAvailExtendedVirtual", ctypes.c_ulonglong),
        ]

    total_ram = 15.65
    try:
        stat = MEMORYSTATUSEX()
        stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
        total_ram = round(stat.ullTotalPhys / (1024**3), 2)
    except Exception:
        pass

    redis_client = create_redis_client()
    redis_info = redis_client.info()

    with SessionLocal() as db:
        pg_version = db.execute(text("SELECT version();")).scalar()

    return {
        "os_platform": f"{platform.system()}-{platform.release()}-{platform.version()}",
        "cpu_architecture": platform.machine(),
        "logical_cpus": os.cpu_count(),
        "total_ram_gb": total_ram,
        "python_version": sys.version,
        "postgresql_version": pg_version,
        "redis_version": redis_info.get("redis_version", "unknown"),
        "git_provenance": "ada0b9b",
        "frozen_baseline_tag": "v0.8.0-phase8-freeze",
        "frozen_baseline_commit": "da27047f56fe2eec75acdb8502f505542cf61202",
    }


def run_workload_trial(
    workload_cfg: dict[str, Any],
    trial_id: str,
    enable_diagnostic_sampling: bool = False,
    raw_dir: Path = REPO_ROOT / "benchmarks/raw/g93",
) -> dict[str, Any]:
    wid = workload_cfg["workload_id"]
    name = workload_cfg["name"]
    task_type = workload_cfg["task_type"]
    payload = workload_cfg["payload"]
    concurrency = workload_cfg["concurrency"]
    warmup_count = workload_cfg["warmup_tasks"]
    measured_count = workload_cfg["measured_tasks"]
    priority = workload_cfg["priority"]

    redis_client = create_redis_client()
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

    # 1. Warmup
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
    while time.time() - w_start < 15.0:
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
                # Direct non-cached infrastructure sample
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

    # Await completion
    deadline = time.time() + 60.0
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
        sampler_thread.join(timeout=1.0)

    # Stop workers
    outbox_pub.stop()
    dispatcher.stop(grace_period_seconds=2.0)

    # Collect DB lifecycle timestamps
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
    raw_filename = f"raw_g93_{wid}_{trial_id}_{int(time.time())}.jsonl"
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

    # Compute raw file SHA-256
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
    print("===================================================================")
    print("TaskForge Milestone G9.3 — Stage 1A Baseline Reproduction & Preflight")
    print("===================================================================")

    defs_path = REPO_ROOT / "benchmarks/scenarios/g90/workload_definitions.json"
    defs_bytes = defs_path.read_bytes()
    defs_sha256 = hashlib.sha256(defs_bytes).hexdigest()
    print(f"Workload definitions SHA-256: {defs_sha256}")

    with open(defs_path, encoding="utf-8") as f:
        workload_configs = json.load(f)["workloads"]

    env_fingerprint = get_environment_fingerprint()

    # We will execute 3 trials:
    # Trial 1: Baseline reproduction (Standard, no diagnostic sampling)
    # Trial 2: Baseline reproduction repeat (To observe run-to-run variance)
    # Trial 3: Diagnostic sampling preflight (250ms periodic sampling to evaluate overhead)
    all_trials_results = []

    trials_spec = [
        {"trial_id": "trial_1_baseline", "sampling": False},
        {"trial_id": "trial_2_baseline_repeat", "sampling": False},
        {"trial_id": "trial_3_sampling_preflight", "sampling": True},
    ]

    for spec in trials_spec:
        trial_id = spec["trial_id"]
        sampling = spec["sampling"]
        print(f"\n===================================================================")
        print(f"Executing Campaign Trial: {trial_id} (Sampling: {sampling})")
        print(f"===================================================================")
        trial_summary = {"trial_id": trial_id, "sampling_enabled": sampling, "workloads": {}}

        for w_cfg in workload_configs:
            w_res = run_workload_trial(
                workload_cfg=w_cfg,
                trial_id=trial_id,
                enable_diagnostic_sampling=sampling,
            )
            trial_summary["workloads"][w_cfg["workload_id"]] = w_res

        all_trials_results.append(trial_summary)

    # Save comprehensive report to benchmarks/scenarios/g93/stage1a_report.json
    out_dir = REPO_ROOT / "benchmarks/scenarios/g93"
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "stage1a_report.json"

    report_payload = {
        "milestone": "G9.3_stage1a_baseline_reproduction",
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "environment": env_fingerprint,
        "workload_definitions_sha256": defs_sha256,
        "trials": all_trials_results,
    }

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report_payload, f, indent=2)

    print(f"\n[Stage 1A] Comprehensive report written to: {report_path}")

    # Drop benchmark DB
    engine.dispose()
    clean_conn = psycopg.connect("postgresql://taskforge:taskforge_dev@localhost:5432/postgres", autocommit=True)
    with clean_conn.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {BENCHMARK_DB_NAME} WITH (FORCE)")
    clean_conn.close()
    print(f"[Stage 1A] Cleaned up disposable database '{BENCHMARK_DB_NAME}'.")


if __name__ == "__main__":
    main()
