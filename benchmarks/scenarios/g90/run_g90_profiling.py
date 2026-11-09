# ruff: noqa: E402
"""G9.0 Baseline Performance Profiling Harness.

Executes standalone, reproducible benchmark workloads against TaskForge v0.8.0-phase8-freeze.
Operates strictly in an isolated benchmark environment:
- Database: taskforge_g90_benchmark
- Redis queue prefix: taskforge:g90:benchmark:
- Captures:
  * API submission latency (t_submit - t_req_start)
  * Broker dispatch delay (t_acquired - t_submit)
  * Execution overhead (t_completed - t_acquired)
  * End-to-end completion latency (t_terminal - t_req_start)
  * Total throughput, error counts, retry counts, failure rates
- Emits raw per-task JSONL measurements to benchmarks/raw/
- Emits machine-validated aggregated JSON metrics to benchmarks/scenarios/g90/baseline_performance.json
"""

import json
import os
import statistics
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import psycopg
from pydantic import BaseModel, model_validator

# Ensure src/ is on sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fastapi.testclient import TestClient
from sqlalchemy import text

# Target database and redis settings for isolated profiling
BENCHMARK_DB_NAME = "taskforge_g90_benchmark"
BENCHMARK_DB_URL = f"postgresql+psycopg://taskforge:taskforge_dev@localhost:5432/{BENCHMARK_DB_NAME}"
BENCHMARK_QUEUE_PREFIX = "taskforge:g90:benchmark:"
BENCHMARK_CAMPAIGN_ID = os.environ.get("TASKFORGE_G90_CAMPAIGN_ID", f"cmp-g90-{uuid.uuid4().hex[:8]}")

# 1. First ensure the database exists BEFORE importing taskforge.database.session
print(f"Ensuring disposable benchmark database '{BENCHMARK_DB_NAME}' exists...")
conn = psycopg.connect("postgresql://taskforge:taskforge_dev@localhost:5432/postgres", autocommit=True)
with conn.cursor() as cur:
    cur.execute(f"DROP DATABASE IF EXISTS {BENCHMARK_DB_NAME} WITH (FORCE)")
    cur.execute(f"CREATE DATABASE {BENCHMARK_DB_NAME} OWNER taskforge")
conn.close()
print(f"Created fresh '{BENCHMARK_DB_NAME}'.")

# 2. Setup environment variables before importing app
os.environ["DATABASE_URL"] = BENCHMARK_DB_URL
os.environ["TASKFORGE_TESTING"] = "1"  # Avoid duplicate default embedded dispatcher
os.environ["APP_ENV"] = "development"

# 3. Apply Alembic migrations on fresh DB
os.chdir(str(REPO_ROOT))
from alembic import command
from alembic.config import Config

alembic_cfg = Config(os.path.abspath(str(REPO_ROOT / "alembic.ini")))
alembic_cfg.set_main_option("script_location", os.path.abspath(str(REPO_ROOT / "migrations")))
alembic_cfg.set_main_option("sqlalchemy.url", BENCHMARK_DB_URL)
command.upgrade(alembic_cfg, "head")
print(f"Alembic migrations applied to '{BENCHMARK_DB_NAME}'.")

from taskforge.database.session import SessionLocal, engine
from taskforge.main import app
from taskforge.queue.broker import RedisBroker
from taskforge.queue.outbox import OutboxPublisherWorker
from taskforge.queue.redis_client import create_redis_client
from taskforge.workers.dispatcher import WorkerDispatcher

# --- Pydantic Schema for Aggregated Benchmark Validation ---

class PercentileMetricSchema(BaseModel, extra="forbid"):
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
    min_ms: float
    mean_ms: float


class WorkloadMetricSchema(BaseModel, extra="forbid"):
    workload_id: str
    name: str
    task_type: str
    concurrency: int
    warmup_tasks: int
    measured_tasks: int
    submitted_tasks: int
    completed_tasks: int
    failed_tasks: int
    timed_out_tasks: int
    measurement_duration_seconds: float
    achieved_throughput_tasks_per_sec: float
    api_submission_latency: PercentileMetricSchema
    broker_dispatch_delay: PercentileMetricSchema
    execution_overhead: PercentileMetricSchema
    end_to_end_latency: PercentileMetricSchema


class G90BaselineSchema(BaseModel, extra="forbid"):
    milestone: Literal["G9.0_baseline_performance"]
    schema_version: Literal["1"]
    campaign_id: str
    git_tag: str
    git_commit: str
    frozen_implementation_baseline: str
    measured_at: str
    environment: dict[str, Any]
    workloads: dict[str, WorkloadMetricSchema]
    overall_verdict: Literal["PASS", "FAIL"]

    @model_validator(mode="after")
    def validate_workload_completeness(self):
        expected = {"W-01", "W-02", "W-03"}
        if set(self.workloads.keys()) != expected:
            raise ValueError(f"Expected workloads {expected}, got {set(self.workloads.keys())}")
        for wid, wdata in self.workloads.items():
            if wdata.failed_tasks > 0:
                raise ValueError(f"Workload {wid} has {wdata.failed_tasks} failed tasks in baseline!")
            if wdata.completed_tasks < wdata.measured_tasks:
                raise ValueError(f"Workload {wid} completed {wdata.completed_tasks} < {wdata.measured_tasks}")
        return self


def compute_percentiles(samples_ms: list[float]) -> PercentileMetricSchema:
    if not samples_ms:
        return PercentileMetricSchema(p50_ms=0, p95_ms=0, p99_ms=0, max_ms=0, min_ms=0, mean_ms=0)
    sorted_s = sorted(samples_ms)
    n = len(sorted_s)
    
    def _pct(p: float) -> float:
        k = (n - 1) * p
        f = int(k)
        c = min(f + 1, n - 1)
        d0 = sorted_s[f] * (c - k)
        d1 = sorted_s[c] * (k - f)
        return round(d0 + d1, 3)

    return PercentileMetricSchema(
        p50_ms=_pct(0.50),
        p95_ms=_pct(0.95),
        p99_ms=_pct(0.99),
        max_ms=round(sorted_s[-1], 3),
        min_ms=round(sorted_s[0], 3),
        mean_ms=round(statistics.mean(sorted_s), 3),
    )


def setup_benchmark_database():
    """Disposable benchmark database already initialized on startup."""
    pass


def cleanup_benchmark_database():
    """Drop disposable benchmark database."""
    print(f"Cleaning up disposable benchmark database '{BENCHMARK_DB_NAME}'...")
    try:
        engine.dispose()
        conn = psycopg.connect("postgresql://taskforge:taskforge_dev@localhost:5432/postgres", autocommit=True)
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {BENCHMARK_DB_NAME} WITH (FORCE)")
        conn.close()
        print(f"Dropped '{BENCHMARK_DB_NAME}'.")
    except Exception as e:
        print(f"Warning during DB cleanup: {e}")


def run_benchmark_suite() -> dict[str, Any]:
    raw_timestamp = int(time.time())
    raw_jsonl_path = REPO_ROOT / f"benchmarks/raw/g90_baseline_raw_{raw_timestamp}.jsonl"
    raw_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    raw_file = open(raw_jsonl_path, "w", encoding="utf-8")

    setup_benchmark_database()
    redis_client = create_redis_client()
    
    # Load workload specifications
    defs_path = REPO_ROOT / "benchmarks/scenarios/g90/workload_definitions.json"
    with open(defs_path, encoding="utf-8") as f:
        workload_configs = json.load(f)["workloads"]

    workload_results: dict[str, WorkloadMetricSchema] = {}

    try:
        client = TestClient(app)

        for w_cfg in workload_configs:
            wid = w_cfg["workload_id"]
            name = w_cfg["name"]
            task_type = w_cfg["task_type"]
            payload = w_cfg["payload"]
            concurrency = w_cfg["concurrency"]
            warmup_count = w_cfg["warmup_tasks"]
            measured_count = w_cfg["measured_tasks"]
            priority = w_cfg["priority"]

            queue_name = f"{BENCHMARK_QUEUE_PREFIX}{wid}:{uuid.uuid4().hex[:6]}"
            broker = RedisBroker(redis_client, queue_name=queue_name, visibility_timeout_seconds=30)

            # Start isolated Outbox Publisher and Worker Dispatcher for this queue
            outbox_pub = OutboxPublisherWorker(poll_interval=0.05, broker=broker)
            outbox_pub.start()

            dispatcher = WorkerDispatcher(
                worker_id=f"g90-worker-{wid}",
                concurrency=concurrency,
                broker=broker,
                poll_interval=0.05,
            )
            dispatcher.start()

            print("\n========================================================")
            print(f"Starting Workload {wid}: {name} ({measured_count} tasks, concurrency={concurrency})")
            print("========================================================")

            # --- 1. WARM-UP PHASE ---
            print(f"Executing warm-up phase ({warmup_count} tasks)...")
            warmup_task_ids = []
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
                assert res.status_code == 201, f"Warmup submit failed: {res.text}"
                warmup_task_ids.append(res.json()["id"])

            # Await warm-up completion
            w_start = time.time()
            while time.time() - w_start < 15.0:
                with SessionLocal() as db:
                    completed_warmup = db.execute(
                        text("SELECT COUNT(*) FROM tasks WHERE id = ANY(:ids) AND status = 'SUCCESS'"),
                        {"ids": warmup_task_ids},
                    ).scalar()
                    if completed_warmup == warmup_count:
                        break
                time.sleep(0.05)
            print("Warm-up complete. Starting measured trial...")

            # --- 2. MEASURED TRIAL PHASE ---
            task_records: dict[str, dict[str, Any]] = {}
            records_lock = threading.Lock()

            def submit_single_task(idx: int, cur_wid=wid, cur_type=task_type, cur_payload=payload, cur_priority=priority) -> dict[str, Any]:
                t0_req = time.perf_counter()
                t0_epoch = time.time()
                resp = client.post(
                    "/api/v1/tasks",
                    json={
                        "task_name": f"g90-{cur_wid}-{idx}",
                        "task_type": cur_type,
                        "payload": cur_payload,
                        "priority": cur_priority,
                    },
                )
                t1_resp = time.perf_counter()
                if resp.status_code != 201:
                    return {"idx": idx, "status": "SUBMIT_FAILED", "error": resp.text}
                
                tid = resp.json()["id"]
                sub_lat_ms = (t1_resp - t0_req) * 1000.0
                return {
                    "idx": idx,
                    "task_id": tid,
                    "status": "SUBMITTED",
                    "t0_epoch": t0_epoch,
                    "t0_req": t0_req,
                    "t1_resp": t1_resp,
                    "submission_latency_ms": sub_lat_ms,
                }

            start_measured_wall = time.perf_counter()

            # Execute concurrent submissions
            submitted_results = []
            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                futures = [pool.submit(submit_single_task, i) for i in range(measured_count)]
                for f in as_completed(futures):
                    r = f.result()
                    submitted_results.append(r)
                    if r["status"] == "SUBMITTED":
                        with records_lock:
                            task_records[r["task_id"]] = r

            submitted_count = len(task_records)
            assert submitted_count == measured_count, f"Submitted {submitted_count} != {measured_count}"

            # Await all measured tasks reaching terminal state
            poll_deadline = time.time() + 60.0
            completed_set = set()
            while time.time() < poll_deadline:
                with SessionLocal() as db:
                    rows = db.execute(
                        text("""
                            SELECT id, status, created_at, updated_at
                            FROM tasks
                            WHERE id = ANY(:ids) AND status IN ('SUCCESS', 'FAILURE', 'DEAD_LETTER')
                        """),
                        {"ids": list(task_records.keys())},
                    ).fetchall()
                    for row in rows:
                        completed_set.add(row[0])
                    if len(completed_set) == measured_count:
                        break
                time.sleep(0.05)

            end_measured_wall = time.perf_counter()
            total_duration_sec = end_measured_wall - start_measured_wall
            achieved_tps = round(len(completed_set) / total_duration_sec, 2)

            # Query exact database lifecycle timestamps for dispatch delay and execution duration
            with SessionLocal() as db:
                attempts_data = db.execute(
                    text("""
                        SELECT ta.task_id, ta.attempt_number, ta.status, ta.started_at, ta.completed_at, ta.duration_ms
                        FROM task_attempts ta
                        WHERE ta.task_id = ANY(:ids) AND ta.attempt_number = 1
                    """),
                    {"ids": list(task_records.keys())},
                ).mappings().all()

                results_data = db.execute(
                    text("""
                        SELECT tr.task_id, tr.created_at
                        FROM task_results tr
                        WHERE tr.task_id = ANY(:ids)
                    """),
                    {"ids": list(task_records.keys())},
                ).mappings().all()

            att_by_id = {row["task_id"]: row for row in attempts_data}
            res_by_id = {row["task_id"]: row for row in results_data}

            sub_latencies_ms: list[float] = []
            dispatch_delays_ms: list[float] = []
            exec_overheads_ms: list[float] = []
            e2e_latencies_ms: list[float] = []

            for tid, tinfo in task_records.items():
                sub_lat_ms = tinfo["submission_latency_ms"]
                sub_latencies_ms.append(sub_lat_ms)

                att = att_by_id.get(tid)
                res = res_by_id.get(tid)

                if att and att["started_at"]:
                    # Dispatch delay = attempt started_at epoch - client response epoch
                    started_epoch = att["started_at"].timestamp()
                    disp_delay_ms = max(0.0, (started_epoch - tinfo["t0_epoch"]) * 1000.0 - sub_lat_ms)
                    dispatch_delays_ms.append(disp_delay_ms)
                else:
                    disp_delay_ms = 0.0

                if att and att["duration_ms"] is not None:
                    exec_overhead_ms = float(att["duration_ms"])
                    exec_overheads_ms.append(exec_overhead_ms)
                elif att and att["completed_at"] and att["started_at"]:
                    exec_overhead_ms = (att["completed_at"] - att["started_at"]).total_seconds() * 1000.0
                    exec_overheads_ms.append(exec_overhead_ms)
                else:
                    exec_overhead_ms = 0.0

                if res and res["created_at"]:
                    e2e_ms = max(sub_lat_ms, (res["created_at"].timestamp() - tinfo["t0_epoch"]) * 1000.0)
                    e2e_latencies_ms.append(e2e_ms)
                else:
                    e2e_ms = sub_lat_ms + disp_delay_ms + exec_overhead_ms
                    e2e_latencies_ms.append(e2e_ms)

                # Stream raw sample to JSONL
                sample_record = {
                    "workload_id": wid,
                    "task_id": tid,
                    "submission_latency_ms": round(sub_lat_ms, 3),
                    "dispatch_delay_ms": round(disp_delay_ms, 3),
                    "execution_overhead_ms": round(exec_overhead_ms, 3),
                    "end_to_end_latency_ms": round(e2e_ms, 3),
                }
                raw_file.write(json.dumps(sample_record) + "\n")

            raw_file.flush()

            # Compute percentiles
            sub_pct = compute_percentiles(sub_latencies_ms)
            disp_pct = compute_percentiles(dispatch_delays_ms)
            exec_pct = compute_percentiles(exec_overheads_ms)
            e2e_pct = compute_percentiles(e2e_latencies_ms)

            failed_tasks = measured_count - len(completed_set)

            workload_metric = WorkloadMetricSchema(
                workload_id=wid,
                name=name,
                task_type=task_type,
                concurrency=concurrency,
                warmup_tasks=warmup_count,
                measured_tasks=measured_count,
                submitted_tasks=submitted_count,
                completed_tasks=len(completed_set),
                failed_tasks=failed_tasks,
                timed_out_tasks=0,
                measurement_duration_seconds=round(total_duration_sec, 3),
                achieved_throughput_tasks_per_sec=achieved_tps,
                api_submission_latency=sub_pct,
                broker_dispatch_delay=disp_pct,
                execution_overhead=exec_pct,
                end_to_end_latency=e2e_pct,
            )
            workload_results[wid] = workload_metric

            print(f"Results for {wid}:")
            print(f"  Duration: {total_duration_sec:.2f}s | Throughput: {achieved_tps} tasks/sec")
            print(f"  Submission Latency (ms): p50={sub_pct.p50_ms}, p95={sub_pct.p95_ms}, p99={sub_pct.p99_ms}")
            print(f"  Dispatch Delay (ms):     p50={disp_pct.p50_ms}, p95={disp_pct.p95_ms}, p99={disp_pct.p99_ms}")
            print(f"  Execution Overhead (ms): p50={exec_pct.p50_ms}, p95={exec_pct.p95_ms}, p99={exec_pct.p99_ms}")
            print(f"  End-to-End Latency (ms): p50={e2e_pct.p50_ms}, p95={e2e_pct.p95_ms}, p99={e2e_pct.p99_ms}")

            # Stop worker and outbox publisher
            dispatcher.stop()
            outbox_pub.stop()

    finally:
        raw_file.close()
        cleanup_benchmark_database()
        if redis_client:
            redis_client.close()

    # Load environment metadata
    with open(REPO_ROOT / "benchmarks/scenarios/g90/environment.json", encoding="utf-8") as f:
        env_metadata = json.load(f)

    baseline_payload = {
        "milestone": "G9.0_baseline_performance",
        "schema_version": "1",
        "campaign_id": BENCHMARK_CAMPAIGN_ID,
        "git_tag": "v0.8.0-phase8-freeze",
        "git_commit": "da27047f56fe2eec75acdb8502f505542cf61202",
        "frozen_implementation_baseline": "9ab6f91",
        "measured_at": datetime.now(UTC).isoformat(),
        "environment": env_metadata,
        "workloads": {k: v.model_dump() for k, v in workload_results.items()},
        "overall_verdict": "PASS" if all(w.failed_tasks == 0 for w in workload_results.values()) else "FAIL",
    }

    # Validate against strict Pydantic model
    validated_model = G90BaselineSchema.model_validate(baseline_payload)
    out_path = REPO_ROOT / "benchmarks/scenarios/g90/baseline_performance.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(validated_model.model_dump(), f, indent=2)

    print(f"\nAggregated baseline performance successfully persisted to {out_path}")
    print(f"Raw sample logs preserved in {raw_jsonl_path}")
    return validated_model.model_dump()


if __name__ == "__main__":
    run_benchmark_suite()
