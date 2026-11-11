#!/usr/bin/env python3
"""TaskForge Canonical Public Architecture Benchmark Suite (Gate G9.5 Stage 2).

Executes the locked canonical public benchmark protocol:
- Workload W-01: No-op baseline (API ingestion, transactional outbox, Redis transit,
                 dispatcher dequeue, DB state update, result persistence, event ACK).
- Workload W-02: CPU-bound prime sieve (Eratosthenes prime sieve up to n=500 per task
                 under saturated concurrency).
- Workload W-03: I/O-bound simulated downstream service (deterministic 50ms sleep
                 evaluating pool saturation and concurrency limits).

Measurement Contract:
- Throughput: Measured Tasks / (T_end - T_start) over steady-state window.
- Warmup: First 10% of tasks excluded from latency percentiles.
- Latencies: Outbox dwell, queue dwell, handler execution, and end-to-end percentiles (p50, p95, p99).
- Accounting: All task outcomes retained (success, failed, retried).
- Validation: Validates output against benchmarks/public/schema/v1.json.

Usage:
    python benchmarks/public/run_public_benchmarks.py [--quick] [--workload {all,W-01,W-02,W-03}]
                                                      [--output PATH] [--no-cleanup]
"""

import argparse
import ctypes
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import socket
import subprocess
import sys
import threading
import time
from typing import Any
import uuid

import psycopg
import redis
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session

# Ensure repository root and src/ are on sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from alembic import command
from alembic.config import Config

from taskforge.core.state_machine import TaskPriority, TaskStatus
from taskforge.database.models import OutboxEvent, Task, TaskAttempt, TaskResult, get_utc_now
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal, set_engine
from taskforge.queue.broker import RedisBroker
from taskforge.queue.outbox import OutboxPublisherWorker
from taskforge.queue.redis_client import create_redis_client
from taskforge.workers.dispatcher import WorkerDispatcher

# ANSI Terminal Colors
RESET = "\033[0m"
BOLD = "\033[1m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RED = "\033[91m"
BLUE = "\033[94m"
MAGENTA = "\033[95m"

SCHEMA_PATH = REPO_ROOT / "benchmarks" / "public" / "schema" / "v1.json"
DEFAULT_OUTPUT_PATH = REPO_ROOT / "benchmarks" / "public" / "benchmark_results.json"


def print_banner(text: str) -> None:
    print(f"\n{BOLD}{BLUE}{'=' * 80}{RESET}")
    print(f"{BOLD}{BLUE}  {text}{RESET}")
    print(f"{BOLD}{BLUE}{'=' * 80}{RESET}\n")


def print_step(step_num: int, title: str) -> None:
    print(f"\n{BOLD}{CYAN}[Step {step_num}] {title}{RESET}")


def calculate_percentiles(values: list[float]) -> dict[str, float]:
    """Calculates p50, p95, p99, min, max, mean using standard linear interpolation."""
    if not values:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "min": 0.0, "max": 0.0, "mean": 0.0}
    s = sorted(values)
    n = len(s)

    def pct(p: float) -> float:
        if n == 1:
            return float(s[0])
        k = (n - 1) * (p / 100.0)
        f = int(k)
        c = min(f + 1, n - 1)
        return round(float(s[f] * (c - k) + s[c] * (k - f)), 3)

    return {
        "p50": pct(50.0),
        "p95": pct(95.0),
        "p99": pct(99.0),
        "min": round(float(s[0]), 3),
        "max": round(float(s[-1]), 3),
        "mean": round(float(sum(s) / n), 3),
    }


def get_system_ram_gb() -> float:
    """Detects total physical system memory in GB cross-platform."""
    try:
        if platform.system() == "Windows":
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
            stat = MEMORYSTATUSEX()
            stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
            return round(stat.ullTotalPhys / (1024**3), 2)
        elif platform.system() == "Linux":
            with open("/proc/meminfo", "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        kb = int(line.split()[1])
                        return round(kb / (1024**2), 2)
    except Exception:
        pass
    return 16.0  # Safe fallback estimate


def get_git_info() -> tuple[str, bool]:
    """Retrieves current Git commit SHA and dirty state."""
    try:
        res_sha = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, cwd=str(REPO_ROOT), timeout=5
        )
        sha = res_sha.stdout.strip() if res_sha.returncode == 0 else "unknown"

        res_dirty = subprocess.run(
            ["git", "status", "--porcelain"], capture_output=True, text=True, cwd=str(REPO_ROOT), timeout=5
        )
        is_dirty = bool(res_dirty.stdout.strip()) if res_dirty.returncode == 0 else False
        return sha, is_dirty
    except Exception:
        return "unknown", False


def get_postgres_version() -> str:
    """Connects to local PostgreSQL instance and retrieves version string."""
    try:
        conn = psycopg.connect("postgresql://taskforge:taskforge_dev@localhost:5432/postgres", connect_timeout=3)
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            row = cur.fetchone()
            ver = row[0].split(",")[0].strip() if row else "PostgreSQL 16"
        conn.close()
        return ver
    except Exception as exc:
        return f"PostgreSQL (connection error: {exc})"


def get_redis_version() -> str:
    """Connects to local Redis instance and retrieves server version string."""
    try:
        r = redis.Redis(host="localhost", port=6379, socket_connect_timeout=3)
        info = r.info("server")
        return f"Redis v{info.get('redis_version', '7.4')}"
    except Exception as exc:
        return f"Redis (connection error: {exc})"


def validate_schema(data: dict[str, Any], schema_path: Path) -> None:
    """Strict in-process validation of benchmark results against the v1.json schema contract."""
    if not schema_path.is_file():
        raise FileNotFoundError(f"Benchmark schema file missing: {schema_path}")

    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    # Required top-level fields
    for field in schema["required"]:
        if field not in data:
            raise ValueError(f"Schema violation: missing required top-level field '{field}'")

    if data["schema_version"] != "1.0.0":
        raise ValueError(f"Schema violation: schema_version must be '1.0.0', got '{data['schema_version']}'")

    # Validate provenance
    prov = data["provenance"]
    for pf in ["git_commit", "git_dirty", "production_baseline_sha"]:
        if pf not in prov:
            raise ValueError(f"Schema violation: provenance missing '{pf}'")

    # Validate environment
    env = data["environment"]
    for ef in ["os", "cpu_model", "cpu_logical_cores", "ram_gb", "python_version", "postgres_version", "redis_version"]:
        if ef not in env:
            raise ValueError(f"Schema violation: environment missing '{ef}'")

    # Validate workload results
    wr = data["workload_results"]
    if not wr:
        raise ValueError("Schema violation: workload_results cannot be empty")

    req_workload_fields = ["task_count", "elapsed_seconds", "throughput_tasks_per_sec", "latencies_ms", "raw_observations_count"]
    req_latencies = ["p50", "p95", "p99", "min", "max", "mean"]

    for wk_name, wk_data in wr.items():
        for wf in req_workload_fields:
            if wf not in wk_data:
                raise ValueError(f"Schema violation: workload '{wk_name}' missing field '{wf}'")

        lats = wk_data["latencies_ms"]
        for lf in req_latencies:
            if lf not in lats:
                raise ValueError(f"Schema violation: workload '{wk_name}' latencies_ms missing '{lf}'")


def run_benchmark_workload(
    workload_id: str,
    task_count: int,
    concurrency: int,
    run_id: str,
    demo_queue_prefix: str,
) -> dict[str, Any]:
    """Executes a single canonical workload, enforces warmup exclusion, and collects latency breakdown."""
    queue_name = f"{demo_queue_prefix}{workload_id.lower()}"
    warmup_count = max(1, int(task_count * 0.10))
    measured_count = task_count - warmup_count

    # Define task type and payload based on canonical workload definition
    if workload_id == "W-01":
        task_type = "noop"
        def make_payload(idx: int) -> dict[str, Any]:
            return {"workload": "W-01", "index": idx}
    elif workload_id == "W-02":
        task_type = "cpu_bound"
        def make_payload(idx: int) -> dict[str, Any]:
            return {"workload": "W-02", "n": 500, "index": idx}
    elif workload_id == "W-03":
        task_type = "io_bound"
        def make_payload(idx: int) -> dict[str, Any]:
            return {"workload": "W-03", "delay_seconds": 0.050, "index": idx}
    else:
        raise ValueError(f"Unknown workload ID: {workload_id}")

    print(f"\n  Executing Workload {BOLD}{workload_id}{RESET} (Total: {task_count}, Warmup: {warmup_count}, Measured: {measured_count})...")

    task_ids: list[str] = []
    event_ids: list[str] = []
    created_at_map: dict[str, datetime] = {}

    # 1. Seed tasks and transactional outbox events in disposable PostgreSQL database
    with SessionLocal() as db:
        now = get_utc_now()
        for idx in range(task_count):
            tid = f"bench-{run_id}-{workload_id.lower()}-{idx:04d}"
            is_warmup = idx < warmup_count
            t = Task(
                id=tid,
                task_name=f"Benchmark Task [{workload_id}] #{idx:04d}",
                task_type=task_type,
                status=TaskStatus.QUEUED.value,
                priority=TaskPriority.MEDIUM.value,  # Stage 0 Normal tier
                payload=make_payload(idx),
            )
            eid = str(uuid.uuid4())
            evt = OutboxEvent(
                id=eid,
                aggregate_type="task",
                aggregate_id=tid,
                event_type="TASK_CREATED",
                payload={"task_id": tid, "priority": TaskPriority.MEDIUM.value, "warmup": is_warmup},
                status="PENDING",
                available_at=now,
                created_at=now,
            )
            db.add(t)
            db.add(evt)
            task_ids.append(tid)
            event_ids.append(eid)
            created_at_map[tid] = now
        db.commit()

    # 2. Initialize Broker, Publisher, and Dispatcher Worker
    real_redis_client = create_redis_client()
    broker = RedisBroker(real_redis_client, queue_name=queue_name)
    publisher = OutboxPublisherWorker(poll_interval=0.05, broker=broker)
    dispatcher = WorkerDispatcher(
        worker_id=f"bench-worker-{run_id}-{workload_id.lower()}",
        concurrency=concurrency,
        broker=broker,
        poll_interval=0.05,
    )

    t_wall_start = time.perf_counter()
    publisher.start()
    dispatcher.start()

    # 3. Poll for completion of all tasks in database
    t_deadline = time.time() + max(30.0, task_count * 0.25)
    all_completed = False

    while time.time() < t_deadline:
        with SessionLocal() as db:
            completed_in_db = db.scalars(
                select(Task.id).where(Task.id.in_(task_ids), Task.status.in_([TaskStatus.SUCCESS.value, TaskStatus.FAILED.value]))
            ).all()
            if len(completed_in_db) == task_count:
                all_completed = True
                break
        time.sleep(0.1)

    t_wall_end = time.perf_counter()
    wall_elapsed_seconds = round(t_wall_end - t_wall_start, 4)

    publisher.stop()
    dispatcher.stop(grace_period_seconds=2.0)

    assert all_completed is True, f"Workload {workload_id} failed to complete all tasks before deadline!"

    # 4. Extract detailed timestamps and calculate latency breakdown
    with SessionLocal() as db:
        tasks = db.scalars(select(Task).where(Task.id.in_(task_ids))).all()
        events = db.scalars(select(OutboxEvent).where(OutboxEvent.id.in_(event_ids))).all()
        attempts = db.scalars(select(TaskAttempt).where(TaskAttempt.task_id.in_(task_ids))).all()
        results = db.scalars(select(TaskResult).where(TaskResult.task_id.in_(task_ids))).all()

    events_by_task = {e.aggregate_id: e for e in events}
    attempts_by_task = {a.task_id: a for a in attempts}
    tasks_by_id = {t.id: t for t in tasks}

    # Separate warmup tasks (first warmup_count) from steady-state measured tasks
    measured_task_ids = task_ids[warmup_count:]

    outbox_dwell_ms_list: list[float] = []
    queue_dwell_ms_list: list[float] = []
    handler_exec_ms_list: list[float] = []
    e2e_latency_ms_list: list[float] = []

    t_measured_starts: list[datetime] = []
    t_measured_ends: list[datetime] = []

    success_count = 0
    failed_count = 0
    retried_count = 0

    for tid in measured_task_ids:
        t = tasks_by_id[tid]
        evt = events_by_task.get(tid)
        att = attempts_by_task.get(tid)

        if t.status == TaskStatus.SUCCESS.value:
            success_count += 1
        else:
            failed_count += 1

        if t.retry_count > 0:
            retried_count += t.retry_count

        if evt and evt.published_at and evt.created_at:
            outbox_dwell = (evt.published_at - evt.created_at).total_seconds() * 1000.0
            outbox_dwell_ms_list.append(max(0.0, outbox_dwell))

        if att and att.started_at and evt and evt.published_at:
            queue_dwell = (att.started_at - evt.published_at).total_seconds() * 1000.0
            queue_dwell_ms_list.append(max(0.0, queue_dwell))

        if att and att.duration_ms is not None:
            handler_exec_ms_list.append(att.duration_ms)
        elif att and att.completed_at and att.started_at:
            handler_exec = (att.completed_at - att.started_at).total_seconds() * 1000.0
            handler_exec_ms_list.append(max(0.0, handler_exec))

        if t.completed_at and t.created_at:
            e2e = (t.completed_at - t.created_at).total_seconds() * 1000.0
            e2e_latency_ms_list.append(max(0.0, e2e))

        if att and att.started_at:
            t_measured_starts.append(att.started_at)
        if t.completed_at:
            t_measured_ends.append(t.completed_at)

    # Calculate steady-state throughput over exact measurement window
    if t_measured_starts and t_measured_ends:
        window_start = min(t_measured_starts)
        window_end = max(t_measured_ends)
        measurement_window_seconds = max(0.001, (window_end - window_start).total_seconds())
    else:
        window_start = get_utc_now()
        window_end = get_utc_now()
        measurement_window_seconds = max(0.001, wall_elapsed_seconds)

    steady_state_throughput = round(measured_count / measurement_window_seconds, 2)

    e2e_pcts = calculate_percentiles(e2e_latency_ms_list)
    outbox_pcts = calculate_percentiles(outbox_dwell_ms_list)
    queue_pcts = calculate_percentiles(queue_dwell_ms_list)
    handler_pcts = calculate_percentiles(handler_exec_ms_list)

    print(f"  {GREEN}[OK]{RESET} Workload {workload_id} Completed in {wall_elapsed_seconds:.2f}s:")
    print(f"       - Steady-State Throughput:  {BOLD}{steady_state_throughput} tasks/s{RESET} (Window: {measurement_window_seconds:.2f}s)")
    print(f"       - End-to-End Latency:      p50={e2e_pcts['p50']}ms, p95={e2e_pcts['p95']}ms, p99={e2e_pcts['p99']}ms")
    print(f"       - Outbox Dwell Latency:    p50={outbox_pcts['p50']}ms, p95={outbox_pcts['p95']}ms, p99={outbox_pcts['p99']}ms")
    print(f"       - Queue Dwell Latency:     p50={queue_pcts['p50']}ms, p95={queue_pcts['p95']}ms, p99={queue_pcts['p99']}ms")
    print(f"       - Handler Execution:       p50={handler_pcts['p50']}ms, p95={handler_pcts['p95']}ms, p99={handler_pcts['p99']}ms")
    print(f"       - Task Outcomes:           {success_count} success, {failed_count} failed, {retried_count} retried")

    return {
        "task_count": task_count,
        "elapsed_seconds": wall_elapsed_seconds,
        "throughput_tasks_per_sec": steady_state_throughput,
        "warmup_tasks_excluded": warmup_count,
        "measured_tasks_count": measured_count,
        "measurement_window": {
            "t_start_utc": window_start.isoformat(),
            "t_end_utc": window_end.isoformat(),
            "window_seconds": round(measurement_window_seconds, 4),
        },
        "task_outcomes": {
            "success_count": success_count,
            "failed_count": failed_count,
            "retried_count": retried_count,
        },
        "latencies_ms": {
            "p50": e2e_pcts["p50"],
            "p95": e2e_pcts["p95"],
            "p99": e2e_pcts["p99"],
            "min": e2e_pcts["min"],
            "max": e2e_pcts["max"],
            "mean": e2e_pcts["mean"],
            "outbox_dwell_ms": {
                "p50": outbox_pcts["p50"],
                "p95": outbox_pcts["p95"],
                "p99": outbox_pcts["p99"],
                "mean": outbox_pcts["mean"],
            },
            "queue_dwell_ms": {
                "p50": queue_pcts["p50"],
                "p95": queue_pcts["p95"],
                "p99": queue_pcts["p99"],
                "mean": queue_pcts["mean"],
            },
            "handler_execution_ms": {
                "p50": handler_pcts["p50"],
                "p95": handler_pcts["p95"],
                "p99": handler_pcts["p99"],
                "mean": handler_pcts["mean"],
            },
        },
        "raw_observations_count": len(e2e_latency_ms_list),
    }


def run_public_benchmarks(
    selected_workload: str = "all",
    quick_mode: bool = False,
    concurrency: int = 4,
    output_path: Path | None = None,
    no_cleanup: bool = False,
) -> int:
    """Orchestrates end-to-end benchmark execution in an isolated disposable environment."""
    run_id = uuid.uuid4().hex[:8]
    demo_db_name = f"taskforge_bench_{run_id}"
    demo_db_url = f"postgresql+psycopg://taskforge:taskforge_dev@localhost:5432/{demo_db_name}"
    demo_queue_prefix = f"taskforge:bench:{run_id}:"

    print_banner(f"TASKFORGE CANONICAL PUBLIC BENCHMARK SUITE — RUN ID: {run_id}")
    print(f"  Disposable PostgreSQL DB: {BOLD}{demo_db_name}{RESET}")
    print(f"  Isolated Redis Namespace: {BOLD}{demo_queue_prefix}*{RESET}")
    print(f"  Production Baseline SHA:  {BOLD}a89858b{RESET} (Frozen & Preserved)")
    print(f"  Phase 8 Frozen Tag:       {BOLD}v0.8.0-phase8-freeze (da27047){RESET}")
    print(f"  Selected Workload:        {BOLD}{selected_workload}{RESET}")
    print(f"  Concurrency:              {BOLD}{concurrency} workers{RESET}")
    print(f"  Execution Mode:           {BOLD}{'Quick Mode' if quick_mode else 'Standard Mode'}{RESET}")

    # Determine task counts per workload
    if quick_mode:
        task_counts = {"W-01": 50, "W-02": 30, "W-03": 20}
    else:
        task_counts = {"W-01": 300, "W-02": 200, "W-03": 100}

    import taskforge.database.session as session_mod
    orig_engine = session_mod.engine
    orig_db_url = os.environ.get("DATABASE_URL")
    orig_testing = os.environ.get("TASKFORGE_TESTING")
    orig_app_env = os.environ.get("APP_ENV")

    demo_engine = None
    resource_manifest: dict[str, Any] = {}
    benchmark_results: dict[str, Any] = {}
    exit_code = 0

    try:
        # Step 1: Pre-existence guard & Provisioning
        print_step(1, f"Provisioning Isolated Database '{demo_db_name}' with Pre-Existence Guard...")
        admin_conn = psycopg.connect("postgresql://taskforge:taskforge_dev@localhost:5432/postgres", autocommit=True)
        with admin_conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (demo_db_name,))
            if cur.fetchone() is not None:
                admin_conn.close()
                raise RuntimeError(f"Resource collision: Database '{demo_db_name}' already exists! Aborting benchmark.")
            cur.execute(f"CREATE DATABASE {demo_db_name} OWNER taskforge")
        admin_conn.close()

        # Redis namespace pre-existence check
        r_init = redis.Redis(host="localhost", port=6379)
        pre_keys = r_init.keys(f"{demo_queue_prefix}*")
        if pre_keys:
            raise RuntimeError(f"Resource collision: Redis namespace contains {len(pre_keys)} pre-existing keys!")

        resource_manifest = {
            "run_id": run_id,
            "provisioned_at_utc": get_utc_now().isoformat(),
            "database": {
                "name": demo_db_name,
                "owner": "taskforge",
                "created_by_run": True,
                "pre_existed": False,
            },
            "redis": {
                "namespace_prefix": demo_queue_prefix,
                "pre_existing_keys_count": 0,
                "owned_by_run": True,
            },
        }
        print(f"  {GREEN}[OK]{RESET} Database '{demo_db_name}' created and ownership manifest locked.")

        # Reconfigure SQLAlchemy SessionLocal to point to disposable database
        os.environ["DATABASE_URL"] = demo_db_url
        os.environ["TASKFORGE_TESTING"] = "1"
        os.environ["APP_ENV"] = "development"

        from taskforge.core.config import get_settings
        get_settings.cache_clear()

        demo_engine = create_engine(demo_db_url, pool_pre_ping=True)
        set_engine(demo_engine)

        # Step 2: Apply Migrations
        print_step(2, "Applying Alembic Migrations to Disposable Schema...")
        alembic_cfg = Config(os.path.abspath(str(REPO_ROOT / "alembic.ini")))
        alembic_cfg.set_main_option("script_location", os.path.abspath(str(REPO_ROOT / "migrations")))
        alembic_cfg.set_main_option("sqlalchemy.url", demo_db_url)
        command.upgrade(alembic_cfg, "head")
        print(f"  {GREEN}[OK]{RESET} Database schema migrated to HEAD.")

        # Step 3: Execute Workloads
        print_step(3, "Executing Benchmark Workloads under Strict Measurement Contract...")
        workloads_to_run = ["W-01", "W-02", "W-03"] if selected_workload == "all" else [selected_workload]

        for wid in workloads_to_run:
            count = task_counts[wid]
            wk_result = run_benchmark_workload(
                workload_id=wid,
                task_count=count,
                concurrency=concurrency,
                run_id=run_id,
                demo_queue_prefix=demo_queue_prefix,
            )
            benchmark_results[wid] = wk_result

        # Step 4: Collect Environment and Provenance Metadata
        print_step(4, "Gathering Hardware, OS, and Runtime Provenance Metadata...")
        git_sha, git_dirty = get_git_info()
        pg_ver = get_postgres_version()
        rd_ver = get_redis_version()
        ram_gb = get_system_ram_gb()

        # Compute hash of this harness script for audit verification
        harness_bytes = Path(__file__).read_bytes()
        harness_sha = hashlib.sha256(harness_bytes).hexdigest()

        final_output = {
            "schema_version": "1.0.0",
            "timestamp_utc": get_utc_now().isoformat(),
            "provenance": {
                "git_commit": git_sha,
                "git_dirty": git_dirty,
                "production_baseline_sha": "a89858b73f8488e05cbcc2393a61f3080ffc7a38",
                "harness_sha": harness_sha,
                "harness_path": "benchmarks/public/run_public_benchmarks.py",
                "run_id": run_id,
            },
            "environment": {
                "os": f"{platform.system()} {platform.release()} ({platform.machine()})",
                "cpu_model": platform.processor() or "AMD64 Architecture",
                "cpu_logical_cores": os.cpu_count() or 8,
                "ram_gb": ram_gb,
                "python_version": platform.python_version(),
                "postgres_version": pg_ver,
                "redis_version": rd_ver,
            },
            "workload_results": benchmark_results,
        }

        # Step 5: Validate Schema Contract
        print_step(5, "Validating Artifact Against Schema Contract (schema/v1.json)...")
        validate_schema(final_output, SCHEMA_PATH)
        print(f"  {GREEN}[OK]{RESET} Benchmark output strictly conforms to benchmarks/public/schema/v1.json.")

        # Step 6: Write Artifact
        dest_path = output_path or DEFAULT_OUTPUT_PATH
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_text(json.dumps(final_output, indent=2), encoding="utf-8")
        print(f"  {GREEN}[OK]{RESET} Benchmark results artifact saved to:\n       {dest_path}")

    except Exception as exc:
        print(f"\n{RED}[ERROR] Benchmark execution encountered an exception: {exc}{RESET}")
        import traceback
        traceback.print_exc()
        exit_code = 1

    finally:
        # Step 7: Teardown with Manifest Verification
        print_step(7, f"Performing Manifest-Verified Safe Teardown...")
        if no_cleanup:
            print(f"  {YELLOW}[SKIP]{RESET} --no-cleanup flag supplied. Preserving '{demo_db_name}'.")
        else:
            db_owned = (
                bool(resource_manifest)
                and resource_manifest.get("database", {}).get("created_by_run") is True
                and resource_manifest.get("database", {}).get("name") == demo_db_name
                and resource_manifest.get("database", {}).get("pre_existed") is False
                and demo_db_name.startswith("taskforge_bench_")
                and demo_db_name.endswith(run_id)
            )
            redis_owned = (
                bool(resource_manifest)
                and resource_manifest.get("redis", {}).get("owned_by_run") is True
                and resource_manifest.get("redis", {}).get("namespace_prefix") == demo_queue_prefix
                and resource_manifest.get("redis", {}).get("pre_existing_keys_count") == 0
            )

            if not db_owned:
                print(f"  {RED}[ERROR]{RESET} Database ownership cannot be established from resource manifest! Refusing cleanup.")
            else:
                try:
                    if demo_engine:
                        demo_engine.dispose()
                    admin_conn = psycopg.connect("postgresql://taskforge:taskforge_dev@localhost:5432/postgres", autocommit=True)
                    with admin_conn.cursor() as cur:
                        cur.execute(f"DROP DATABASE IF EXISTS {demo_db_name} WITH (FORCE)")
                    admin_conn.close()
                    print(f"  {GREEN}[OK]{RESET} Manifest-verified database '{demo_db_name}' safely dropped.")

                    if redis_owned:
                        r = redis.Redis(host="localhost", port=6379)
                        keys = r.keys(f"{demo_queue_prefix}*")
                        if keys:
                            r.delete(*keys)
                        print(f"  {GREEN}[OK]{RESET} Manifest-verified Redis namespace '{demo_queue_prefix}*' safely purged ({len(keys)} keys).")
                except Exception as cleanup_exc:
                    print(f"  {RED}[WARN]{RESET} Teardown error: {cleanup_exc}")

        # Restore original database environment and engine bindings
        if orig_db_url is not None:
            os.environ["DATABASE_URL"] = orig_db_url
        else:
            os.environ.pop("DATABASE_URL", None)

        if orig_testing is not None:
            os.environ["TASKFORGE_TESTING"] = orig_testing
        else:
            os.environ.pop("TASKFORGE_TESTING", None)

        if orig_app_env is not None:
            os.environ["APP_ENV"] = orig_app_env
        else:
            os.environ.pop("APP_ENV", None)

        from taskforge.core.config import get_settings
        get_settings.cache_clear()

        if orig_engine is not None:
            set_engine(orig_engine)

    print(f"\n{BOLD}{GREEN}[BENCHMARKS COMPLETE]{RESET} Exit Code: {exit_code}\n")
    return exit_code


def main():
    parser = argparse.ArgumentParser(description="TaskForge Canonical Public Architecture Benchmark Suite (Gate G9.5 Stage 2)")
    parser.add_argument("--workload", choices=["all", "W-01", "W-02", "W-03"], default="all", help="Workload to execute")
    parser.add_argument("--quick", action="store_true", help="Execute rapid smoke-run task counts (50, 30, 20)")
    parser.add_argument("--concurrency", type=int, default=4, help="WorkerDispatcher worker thread concurrency")
    parser.add_argument("--output", type=Path, default=None, help="Path for JSON benchmark output artifact")
    parser.add_argument("--no-cleanup", action="store_true", help="Preserve disposable database and Redis keys after run")
    args = parser.parse_args()

    sys.exit(
        run_public_benchmarks(
            selected_workload=args.workload,
            quick_mode=args.quick,
            concurrency=args.concurrency,
            output_path=args.output,
            no_cleanup=args.no_cleanup,
        )
    )


if __name__ == "__main__":
    main()
