"""Gate G9.4.1: Outbox Publisher Diagnostic Profiling Campaign Harness.

Executes:
1. Campaign A: Existing Workload Baseline across W-01 (noop, n=500, c=10),
   W-02 (prime, n=200, c=4), and W-03 (io, n=200, c=10) with 3 trials each.
   Under baseline production dispatcher commit a89858b (enable_capacity_wakeup=True).
   Measures:
   - Eligible Outbox Lag (T_outbox_lag) p50/p95/p99
   - Batch Claim Duration (T_outbox_claim) p50/p95/p99
   - Pure Redis Enqueue Round-Trip (T_redis_enq) p50/p95/p99
   - Outbox Publication Commit (T_outbox_commit) p50/p95/p99
   - Client-Observed Broker Dwell (T_broker_dwell) p50/p95/p99
   - Worker Attempt and Handler Duration p50/p95/p99
   - Publisher active-duty ratio (U_pub) and publication throughput (R_outbox)
   - Publication failures and retries (must be 0)

2. Campaign B: Controlled Backlog Drain under 1,000 pre-inserted tasks across 3 trials.
   - Initial backlog: Exactly 1,000 eligible outbox events in PostgreSQL while publisher paused.
   - Quiescent arrival during drain (0 new tasks submitted).
   - Start publisher; drain until 0 pending records remain and all 1,000 events reach status='PUBLISHED'.
   - Measures:
     * Total drain time (Delta t_drain)
     * Drain rate (R_drain = 1000 / Delta t)
     * Publication rate per batch cycle (50 tasks/batch -> 20 batches)
     * Per-batch claim, enqueue, commit, and inter-batch sleep durations
     * Time-series backlog depletion curve
     * Verification that all 1,000 events reach durable PUBLISHED state.

All raw traces are stored in benchmarks/raw/g941/ with SHA-256 digests.
Summary JSON output to benchmarks/scenarios/g94/g941_outbox_report.json.
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
BENCHMARK_DB_NAME = "taskforge_g94_benchmark"
BENCHMARK_PSYCOPG_URL = f"postgresql://taskforge:taskforge_dev@localhost:5432/{BENCHMARK_DB_NAME}"
BENCHMARK_DB_URL = f"postgresql+psycopg://taskforge:taskforge_dev@localhost:5432/{BENCHMARK_DB_NAME}"
BENCHMARK_QUEUE_PREFIX = "taskforge:g941:"

print(f"[G9.4.1] Initializing benchmark database '{BENCHMARK_DB_NAME}'...")
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

from taskforge.database.models import OutboxEvent, Task, get_utc_now
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
        return {
            "p50_ms": 0.0,
            "p95_ms": 0.0,
            "p99_ms": 0.0,
            "max_ms": 0.0,
            "min_ms": 0.0,
            "mean_ms": 0.0,
            "stddev_ms": 0.0,
            "count": 0,
        }
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


def parse_campaign_a_events(events_path: Path, task_records: dict[str, dict[str, Any]]) -> dict[str, Any]:
    lines = [json.loads(line) for line in events_path.read_text(encoding="utf-8").strip().splitlines() if line.strip()]

    event_counts = defaultdict(int)
    events_by_task = defaultdict(dict)
    batch_claims = {}
    batch_tasks = defaultdict(list)

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
            batch_claims[bid][ev] = {
                "t_mono": t_mono,
                "t_epoch": t_epoch,
                "metadata": entry.get("metadata", {}),
            }

        if tid:
            events_by_task[tid][ev] = {
                "t_mono_ns": t_mono,
                "t_epoch_utc": t_epoch,
                "batch_id": bid,
            }
            if bid and ev == "redis_enqueue_complete":
                batch_tasks[bid].append(tid)

    # Per-task metric collections
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
    outbox_lag_races = 0
    outbox_unclaimed = 0

    for tid, tinfo in task_records.items():
        tev = events_by_task.get(tid, {})

        # End-to-end latency
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

        # 6. Broker Dwell
        if "redis_dequeue_complete" in tev and "redis_enqueue_complete" in tev:
            dwell_ms = (tev["redis_dequeue_complete"]["t_mono_ns"] - tev["redis_enqueue_complete"]["t_mono_ns"]) / 1_000_000.0
            if dwell_ms >= 0:
                t_broker_dwell.append(dwell_ms)

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
            tp_ms = (tev["terminal_commit"]["t_mono_ns"] - tev["terminal_transaction_start"]["t_mono_ns"]) / 1_000_000.0
            t_terminal_persist.append(tp_ms)

        # 10. Redis ACK
        if "redis_ack_complete" in tev and "terminal_commit" in tev:
            ack_ms = (tev["redis_ack_complete"]["t_mono_ns"] - tev["terminal_commit"]["t_mono_ns"]) / 1_000_000.0
            t_redis_ack.append(ack_ms)

    # Publisher active-duty computation
    # Active duty = sum of (claim duration + enqueue duration of batch tasks + commit duration of batch) across all batches
    total_publisher_active_ns = 0
    for bid, binfo in batch_claims.items():
        if "outbox_claim_start" in binfo and "outbox_claim_commit" in binfo:
            claim_ns = binfo["outbox_claim_commit"]["t_mono"] - binfo["outbox_claim_start"]["t_mono"]
            total_publisher_active_ns += claim_ns
            
        tids_in_b = batch_tasks.get(bid, [])
        for tid in tids_in_b:
            tev = events_by_task.get(tid, {})
            if "redis_enqueue_start" in tev and "redis_enqueue_complete" in tev:
                total_publisher_active_ns += (tev["redis_enqueue_complete"]["t_mono_ns"] - tev["redis_enqueue_start"]["t_mono_ns"])
            if "redis_enqueue_complete" in tev and "outbox_published_commit" in tev:
                # outbox_published_commit is emitted for each tid in batch, but commit happens once per batch
                pass

    return {
        "event_counts": dict(event_counts),
        "total_events_logged": len(lines),
        "outbox_lag_eligible": compute_distribution(t_outbox_lag_eligible),
        "outbox_lag_races": outbox_lag_races,
        "outbox_unclaimed": outbox_unclaimed,
        "outbox_claim": compute_distribution(t_outbox_claim),
        "redis_enqueue": compute_distribution(t_redis_enq),
        "outbox_commit": compute_distribution(t_outbox_commit),
        "broker_dwell": compute_distribution(t_broker_dwell),
        "worker_lease": compute_distribution(t_worker_lease),
        "handler_exec": compute_distribution(t_handler_exec),
        "terminal_persist": compute_distribution(t_terminal_persist),
        "redis_ack": compute_distribution(t_redis_ack),
        "end_to_end": compute_distribution(t_e2e),
        "total_publisher_active_ms": round(total_publisher_active_ns / 1_000_000.0, 3),
        "batches_processed": len(batch_claims),
    }


def run_campaign_a_workload_trial(
    workload_cfg: dict[str, Any],
    trial_id: str,
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

    truncate_benchmark_tables()

    profiler = DiagnosticProfiler.get_instance()
    ts_now = int(time.time())
    raw_events_path = raw_dir / f"events_g941_campaignA_{wid}_{trial_id}_{ts_now}.jsonl"

    queue_name = f"{BENCHMARK_QUEUE_PREFIX}{wid}:{trial_id}:{uuid.uuid4().hex[:6]}"
    broker = RedisBroker(redis_client, queue_name=queue_name, visibility_timeout_seconds=30)

    # Baseline production publisher configuration (poll_interval=0.1)
    publisher = OutboxPublisherWorker(broker=broker, poll_interval=0.1)
    publisher.start()

    # Production dispatcher configuration (concurrency=c, poll_interval=0.1, enable_capacity_wakeup=True)
    dispatcher = WorkerDispatcher(
        worker_id=f"g941-worker-{wid}-{trial_id}",
        concurrency=concurrency,
        broker=broker,
        poll_interval=0.1,
        enable_capacity_wakeup=True,
    )
    dispatcher.start()

    client = TestClient(app)

    print(f"\n[G9.4.1 Campaign A] Running {wid} ({name}) | Trial: {trial_id} (n={measured_count}, c={concurrency})")

    # 1. Warmup Phase (profiling inactive)
    warmup_ids = []
    for i in range(warmup_count):
        res = client.post(
            "/api/v1/tasks",
            json={
                "task_name": f"warmup-{wid}-{trial_id}-{i}",
                "task_type": task_type,
                "payload": payload,
                "priority": priority,
            },
        )
        assert res.status_code == 201
        warmup_ids.append(res.json()["id"])

    # Wait for warmup drain
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
    profiler.start(raw_events_path)

    task_records: dict[str, dict[str, Any]] = {}
    records_lock = threading.Lock()

    def submit_task(idx: int):
        t0_req = time.perf_counter()
        t0_epoch = time.time()
        res = client.post(
            "/api/v1/tasks",
            json={
                "task_name": f"g941-a-{wid}-{trial_id}-{idx}",
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
    start_mono_ns = time.perf_counter_ns()
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
    end_mono_ns = time.perf_counter_ns()
    total_duration_sec = end_wall - start_wall

    publisher.stop()
    dispatcher.stop(grace_period_seconds=5.0)

    flush_report = profiler.stop(drain_timeout=5.0)
    raw_hash = hashlib.sha256(raw_events_path.read_bytes()).hexdigest()

    # Query worker attempt duration from DB
    with SessionLocal() as db:
        attempts_rows = db.execute(
            text("""
                SELECT ta.duration_ms
                FROM task_attempts ta
                JOIN tasks t ON ta.task_id = t.id
                WHERE t.id = ANY(:ids) AND ta.duration_ms IS NOT NULL
            """),
            {"ids": list(task_records.keys())},
        ).fetchall()
        attempt_durations = [float(r[0]) for r in attempts_rows]

        # Verify outbox table final states
        outbox_states = db.execute(
            text("SELECT status, count(*) FROM outbox_events WHERE aggregate_id = ANY(:ids) GROUP BY status"),
            {"ids": list(task_records.keys())},
        ).fetchall()
        outbox_state_counts = {r[0]: int(r[1]) for r in outbox_states}

    parsed = parse_campaign_a_events(raw_events_path, task_records)
    attempt_dist = compute_distribution(attempt_durations)

    active_pub_ms = parsed["total_publisher_active_ms"]
    wall_ms = total_duration_sec * 1000.0
    duty_cycle = round((active_pub_ms / wall_ms) * 100.0, 2) if wall_ms > 0 else 0.0
    pub_throughput = round(measured_count / total_duration_sec, 2)

    trial_summary = {
        "workload_id": wid,
        "trial_id": trial_id,
        "concurrency": concurrency,
        "measured_count": measured_count,
        "wall_clock_duration_s": round(total_duration_sec, 3),
        "throughput_tps": pub_throughput,
        "publisher_active_duty_ratio_pct": duty_cycle,
        "publisher_total_active_ms": active_pub_ms,
        "outbox_state_counts": outbox_state_counts,
        "outbox_failures": outbox_state_counts.get("FAILED", 0),
        "raw_trace_path": str(raw_events_path),
        "raw_trace_sha256": raw_hash,
        "profiler_flush_report": flush_report,
        "metrics": {
            "eligible_outbox_lag": parsed["outbox_lag_eligible"],
            "batch_claim_duration": parsed["outbox_claim"],
            "redis_enqueue_round_trip": parsed["redis_enqueue"],
            "outbox_publication_commit": parsed["outbox_commit"],
            "broker_dwell": parsed["broker_dwell"],
            "worker_attempt_duration": attempt_dist,
            "handler_exec": parsed["handler_exec"],
            "end_to_end": parsed["end_to_end"],
        },
        "batches_processed": parsed["batches_processed"],
        "outbox_lag_races": parsed["outbox_lag_races"],
        "outbox_unclaimed": parsed["outbox_unclaimed"],
    }
    print(f"  -> Outbox Lag p50: {parsed['outbox_lag_eligible']['p50_ms']} ms | Redis Enq p50: {parsed['redis_enqueue']['p50_ms']} ms | Throughput: {pub_throughput} TPS")
    return trial_summary


def run_campaign_b_drain_trial(
    trial_id: str,
    redis_client: Any,
    raw_dir: Path,
    burst_count: int = 1000,
) -> dict[str, Any]:
    truncate_benchmark_tables()

    profiler = DiagnosticProfiler.get_instance()
    ts_now = int(time.time())
    raw_events_path = raw_dir / f"events_g941_campaignB_burst_{trial_id}_{ts_now}.jsonl"

    queue_name = f"{BENCHMARK_QUEUE_PREFIX}burst:{trial_id}:{uuid.uuid4().hex[:6]}"
    broker = RedisBroker(redis_client, queue_name=queue_name, visibility_timeout_seconds=30)

    # 1. Pre-insert exactly 1,000 tasks and outbox events while publisher is PAUSED
    print(f"\n[G9.4.1 Campaign B] Pre-inserting {burst_count} eligible outbox events for Trial {trial_id} (publisher paused)...")
    now_utc = get_utc_now()
    task_ids = []
    
    with SessionLocal() as db:
        tasks = []
        outbox_events = []
        for i in range(burst_count):
            tid = str(uuid.uuid4())
            task_ids.append(tid)
            t = Task(
                id=tid,
                task_name=f"burst-{trial_id}-{i}",
                task_type="noop",
                payload={"burst_idx": i, "trial": trial_id},
                status="QUEUED",
                priority=2,
                idempotency_key=f"burst-{trial_id}-{i}",
                tenant_id="default",
                timeout_seconds=60,
                max_retries=3,
                retry_count=0,
                scheduled_at=None,
                created_at=now_utc,
                updated_at=now_utc,
            )
            tasks.append(t)
            ev = OutboxEvent(
                id=str(uuid.uuid4()),
                aggregate_type="task",
                aggregate_id=tid,
                event_type="TASK_SUBMITTED",
                payload={
                    "task_id": tid,
                    "task_name": f"burst-{trial_id}-{i}",
                    "task_type": "noop",
                    "payload": {"burst_idx": i, "trial": trial_id},
                    "priority": 2,
                    "timeout_seconds": 60,
                    "max_retries": 3,
                    "scheduled_at": None,
                },
                status="PENDING",
                available_at=now_utc,
                created_at=now_utc,
            )
            outbox_events.append(ev)
        
        db.add_all(tasks)
        db.add_all(outbox_events)
        db.commit()

        # Invariant check
        pending_initial = db.execute(text("SELECT count(*) FROM outbox_events WHERE status = 'PENDING'")).scalar()
        published_initial = db.execute(text("SELECT count(*) FROM outbox_events WHERE status = 'PUBLISHED'")).scalar()
        assert pending_initial == burst_count, f"Pre-insertion invariant failed: {pending_initial} != {burst_count}"
        assert published_initial == 0, f"Published invariant failed: {published_initial} != 0"

    # Start profiler for Campaign B
    profiler.start(raw_events_path)

    # Initialize publisher with baseline configuration (poll_interval=0.1)
    publisher = OutboxPublisherWorker(broker=broker, poll_interval=0.1)

    print(f"[G9.4.1 Campaign B] Starting Outbox Publisher (poll_interval=0.1s, batch_size=50) for drain...")
    time_series = []
    
    t_start_wall = time.perf_counter()
    t_start_mono_ns = time.perf_counter_ns()
    publisher.start()

    deadline = time.time() + 60.0
    drain_complete_mono_ns = None
    
    while time.time() < deadline:
        t_sample_mono = time.perf_counter_ns()
        with SessionLocal() as db:
            rows = db.execute(text("SELECT status, count(*) FROM outbox_events GROUP BY status")).fetchall()
            status_map = {r[0]: int(r[1]) for r in rows}
            pending = status_map.get("PENDING", 0)
            published = status_map.get("PUBLISHED", 0)
            failed = status_map.get("FAILED", 0)
            
            elapsed_ms = (t_sample_mono - t_start_mono_ns) / 1_000_000.0
            time_series.append({
                "t_elapsed_ms": round(elapsed_ms, 2),
                "pending": pending,
                "published": published,
                "failed": failed,
            })
            
            if pending == 0 and published == burst_count:
                drain_complete_mono_ns = t_sample_mono
                break
        time.sleep(0.01)

    t_end_wall = time.perf_counter()
    if drain_complete_mono_ns is None:
        publisher.stop()
        profiler.stop(drain_timeout=2.0)
        raise TimeoutError("Campaign B timed out waiting for 1,000 tasks to publish")

    total_drain_wall_s = t_end_wall - t_start_wall
    drain_rate_tps = round(burst_count / total_drain_wall_s, 2)

    publisher.stop()
    flush_report = profiler.stop(drain_timeout=5.0)
    raw_hash = hashlib.sha256(raw_events_path.read_bytes()).hexdigest()

    # Verify final states in DB and Broker
    with SessionLocal() as db:
        final_pending = db.execute(text("SELECT count(*) FROM outbox_events WHERE status = 'PENDING'")).scalar()
        final_published = db.execute(text("SELECT count(*) FROM outbox_events WHERE status = 'PUBLISHED'")).scalar()
        final_failed = db.execute(text("SELECT count(*) FROM outbox_events WHERE status = 'FAILED'")).scalar()
        published_timestamps = db.execute(text("SELECT count(*) FROM outbox_events WHERE published_at IS NOT NULL")).scalar()

    broker_depth = broker.get_queue_depth()

    assert final_pending == 0, f"Invariant violation: final pending = {final_pending} (expected 0)"
    assert final_published == burst_count, f"Invariant violation: final published = {final_published} (expected {burst_count})"
    assert final_failed == 0, f"Invariant violation: final failed = {final_failed} (expected 0)"
    assert published_timestamps == burst_count, f"Invariant violation: published_at non-null = {published_timestamps}"
    assert broker_depth == burst_count, f"Invariant violation: broker depth = {broker_depth} (expected {burst_count})"

    # Parse Campaign B raw events for micro-architectural per-batch cadence
    lines = [json.loads(line) for line in raw_events_path.read_text(encoding="utf-8").strip().splitlines() if line.strip()]
    batch_claims = {}
    batch_enqueues = defaultdict(list)
    batch_commits = {}

    for entry in lines:
        ev = entry["event"]
        bid = entry.get("batch_id")
        t_mono = entry["t_mono_ns"]
        t_epoch = entry["t_epoch_utc"]
        tid = entry.get("task_id")

        if bid and ev in ("outbox_claim_start", "outbox_claim_commit"):
            if bid not in batch_claims:
                batch_claims[bid] = {}
            batch_claims[bid][ev] = t_mono

        if bid and ev in ("redis_enqueue_start", "redis_enqueue_complete"):
            batch_enqueues[bid].append((ev, t_mono, tid))

        if bid and ev == "outbox_published_commit":
            batch_commits[bid] = t_mono

    # Sort batches chronologically
    sorted_batches = sorted(
        [bid for bid, b in batch_claims.items() if "outbox_claim_start" in b],
        key=lambda b: batch_claims[b]["outbox_claim_start"],
    )

    batch_metrics = []
    claim_durations = []
    enqueue_durations = []
    commit_durations = []
    inter_batch_sleeps = []
    total_batch_active_ns = 0

    for idx, bid in enumerate(sorted_batches):
        binfo = batch_claims[bid]
        t_c_start = binfo.get("outbox_claim_start")
        t_c_commit = binfo.get("outbox_claim_commit")
        t_commit = batch_commits.get(bid, t_c_commit)

        claim_dur_ms = (t_c_commit - t_c_start) / 1_000_000.0 if (t_c_start and t_c_commit) else 0.0
        claim_durations.append(claim_dur_ms)

        # Calculate total enqueue duration for the batch
        enq_list = batch_enqueues.get(bid, [])
        enq_starts = [t for ev, t, _ in enq_list if ev == "redis_enqueue_start"]
        enq_ends = [t for ev, t, _ in enq_list if ev == "redis_enqueue_complete"]
        if enq_starts and enq_ends:
            batch_enq_ms = (max(enq_ends) - min(enq_starts)) / 1_000_000.0
        else:
            batch_enq_ms = 0.0
        enqueue_durations.append(batch_enq_ms)

        # Batch commit duration
        if t_commit and enq_ends:
            commit_dur_ms = max(0.0, (t_commit - max(enq_ends)) / 1_000_000.0)
        else:
            commit_dur_ms = 0.0
        commit_durations.append(commit_dur_ms)

        active_dur_ms = claim_dur_ms + batch_enq_ms + commit_dur_ms
        total_batch_active_ns += int(active_dur_ms * 1_000_000)

        # Inter-batch sleep duration
        sleep_dur_ms = 0.0
        if idx > 0:
            prev_bid = sorted_batches[idx - 1]
            prev_commit = batch_commits.get(prev_bid, batch_claims[prev_bid].get("outbox_claim_commit"))
            if prev_commit and t_c_start:
                sleep_dur_ms = max(0.0, (t_c_start - prev_commit) / 1_000_000.0)
                inter_batch_sleeps.append(sleep_dur_ms)

        batch_metrics.append({
            "batch_index": idx + 1,
            "batch_id": bid,
            "claim_duration_ms": round(claim_dur_ms, 3),
            "enqueue_duration_ms": round(batch_enq_ms, 3),
            "commit_duration_ms": round(commit_dur_ms, 3),
            "active_duration_ms": round(active_dur_ms, 3),
            "sleep_before_claim_ms": round(sleep_dur_ms, 3),
        })

    active_pub_ms = round(total_batch_active_ns / 1_000_000.0, 3)
    wall_ms = total_drain_wall_s * 1000.0
    duty_cycle = round((active_pub_ms / wall_ms) * 100.0, 2) if wall_ms > 0 else 0.0

    print(f"  -> Total Drain Time: {round(total_drain_wall_s, 3)} s | Drain Rate: {drain_rate_tps} tasks/s | Batches: {len(sorted_batches)} | Duty Cycle: {duty_cycle}%")

    return {
        "trial_id": trial_id,
        "burst_count": burst_count,
        "wall_clock_drain_duration_s": round(total_drain_wall_s, 3),
        "drain_rate_tasks_per_sec": drain_rate_tps,
        "batches_processed": len(sorted_batches),
        "publisher_active_duty_ratio_pct": duty_cycle,
        "publisher_active_ms": active_pub_ms,
        "raw_trace_path": str(raw_events_path),
        "raw_trace_sha256": raw_hash,
        "profiler_flush_report": flush_report,
        "cadence_distributions": {
            "claim_duration": compute_distribution(claim_durations),
            "batch_enqueue_duration": compute_distribution(enqueue_durations),
            "commit_duration": compute_distribution(commit_durations),
            "inter_batch_sleep": compute_distribution(inter_batch_sleeps),
        },
        "per_batch_cadence": batch_metrics,
        "time_series_depletion": time_series,
        "invariants": {
            "initial_backlog_pre_inserted": burst_count,
            "final_pending": final_pending,
            "final_published": final_published,
            "final_failed": final_failed,
            "broker_queue_depth": broker_depth,
            "durable_delivery_loss_count": 0,
        },
    }


def main():
    print("=" * 80)
    print("TASKFORGE PLATFORM HARDENING: GATE G9.4.1")
    print("Outbox Publisher Diagnostic Profiling Campaign (Campaign A & Campaign B)")
    print("=" * 80)

    # 1. Environment & Provenance Verification
    workload_defs_path = REPO_ROOT / "benchmarks" / "scenarios" / "g90" / "workload_definitions.json"
    defs_raw = workload_defs_path.read_bytes()
    defs_hash = hashlib.sha256(defs_raw).hexdigest()
    EXPECTED_HASH = "04f2306538868a41514c19f0958e04bf0b0586b3a83989e115231a32e50f0110"
    assert defs_hash == EXPECTED_HASH, f"Workload definitions hash mismatch: {defs_hash} != {EXPECTED_HASH}"
    print(f"[Provenance] Workload definitions SHA-256 verified: {defs_hash}")

    workload_data = json.loads(defs_raw.decode("utf-8"))["workloads"]
    workload_map = {w["workload_id"]: w for w in workload_data}

    raw_dir = REPO_ROOT / "benchmarks" / "raw" / "g941"
    raw_dir.mkdir(parents=True, exist_ok=True)

    redis_client = create_redis_client()
    redis_client.ping()
    print("[Provenance] Redis connection verified.")

    # 2. Execute Campaign A: Baseline Across W-01, W-02, W-03 (3 trials each)
    print("\n" + "=" * 80)
    print("STARTING CAMPAIGN A: EXISTING WORKLOAD BASELINE (9 TRIALS)")
    print("=" * 80)

    campaign_a_results = []
    for wid in ["W-01", "W-02", "W-03"]:
        wcfg = workload_map[wid]
        for t_idx in range(1, 4):
            trial_id = f"T{t_idx}"
            summary = run_campaign_a_workload_trial(
                workload_cfg=wcfg,
                trial_id=trial_id,
                redis_client=redis_client,
                raw_dir=raw_dir,
            )
            campaign_a_results.append(summary)

    # 3. Execute Campaign B: Controlled Backlog Drain (1,000 tasks, 3 trials)
    print("\n" + "=" * 80)
    print("STARTING CAMPAIGN B: CONTROLLED BACKLOG DRAIN (3 TRIALS)")
    print("=" * 80)

    campaign_b_results = []
    for t_idx in range(1, 4):
        trial_id = f"T{t_idx}"
        b_summary = run_campaign_b_drain_trial(
            trial_id=trial_id,
            redis_client=redis_client,
            raw_dir=raw_dir,
            burst_count=1000,
        )
        campaign_b_results.append(b_summary)

    # 4. Generate Final G9.4.1 Summary Report JSON
    report = {
        "milestone": "G9.4.1_outbox_publisher_diagnostic_profiling",
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "python_version": platform.python_version(),
        },
        "provenance": {
            "authorized_base_commit": "a89858b73f8488e05cbcc2393a61f3080ffc7a38",
            "frozen_tag": "v0.8.0-phase8-freeze (da27047f56fe2eec75acdb8502f505542cf61202)",
            "workload_definitions_sha256": defs_hash,
        },
        "configuration": {
            "outbox_poll_interval_s": 0.1,
            "outbox_batch_size": 50,
            "dispatcher_enable_capacity_wakeup": True,
            "database": BENCHMARK_DB_NAME,
        },
        "campaign_a_workload_baseline": campaign_a_results,
        "campaign_b_backlog_drain": campaign_b_results,
    }

    out_json_path = REPO_ROOT / "benchmarks" / "scenarios" / "g94" / "g941_outbox_report.json"
    out_json_path.parent.mkdir(parents=True, exist_ok=True)
    out_json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\n[Artifact] G9.4.1 report saved to: {out_json_path}")


if __name__ == "__main__":
    main()

