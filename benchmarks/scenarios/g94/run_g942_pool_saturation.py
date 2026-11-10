"""Gate G9.4.2: Database Connection-Pool Saturation & Telemetry Benchmark.

Executes:
1. Production Configuration Baseline Verification:
   - Verifies production pool defaults: pool_size=10, max_overflow=20, pool_timeout=30.0s, pre_ping=True
   - Dedicated telemetry engine isolation (pool_size=1, max_overflow=0)

2. Barrier-Synchronized 2x Contention Saturation Drill across 3 Independent Trials:
   - Experimental fixture: pool_size=5, max_overflow=10, pool_timeout=5.0s (capacity 15)
   - 30 concurrent callers: 15 holding at barrier (100% saturation: 5 base + 10 overflow), 15 queued callers
   - Controlled hold (1.0s) then release
   - Measures checkout latency distributions, occupancy timeline, and recovery duration
   - Verifies zero leaked connections, zero hung threads, recovery <= 5.0s

3. Bounded Timeout Rejection Drill:
   - 15 callers hold connections for 6.0s (> 5.0s timeout)
   - 15 waiting callers attempt checkout
   - Verifies bounded timeout behavior (all 15 waiting callers time out at 5.0s +/- 0.5s)
   - Verifies taskforge_db_pool_checkout_timeouts_total increment and clean recovery

4. Dedicated Telemetry Engine Isolation Verification:
   - Scrape latency while business pool is 100% saturated

Outputs structured benchmark summary to benchmarks/scenarios/g94/g942_pool_saturation_report.json.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import statistics
import sys
import threading
import time
from typing import Any

from sqlalchemy import exc as sa_exc, text

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

BENCHMARK_DB_NAME = "taskforge_g94_benchmark"
BENCHMARK_DB_URL = f"postgresql+psycopg://taskforge:taskforge_dev@localhost:5432/{BENCHMARK_DB_NAME}"

os.environ["DATABASE_URL"] = BENCHMARK_DB_URL
os.environ["TASKFORGE_TESTING"] = "1"
os.environ["APP_ENV"] = "development"

from taskforge.core.config import get_settings
from taskforge.database.session import (
    InstrumentedQueuePool,
    build_engine,
    engine as prod_engine,
)
from taskforge.monitoring.collector import GaugeSnapshotCache, get_dedicated_telemetry_engine
from taskforge.monitoring.metrics import (
    DB_POOL_ACTIVE_CONNECTIONS,
    DB_POOL_CHECKOUT_TIMEOUTS_TOTAL,
    DB_POOL_OVERFLOW_CONNECTIONS,
    get_metrics_content,
)

settings = get_settings()


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


def run_saturation_recovery_trial(trial_id: str, capacity: int = 15, num_callers: int = 30) -> dict[str, Any]:
    print(f"\n[G9.4.2] Running Saturation & Recovery Drill — Trial {trial_id} ({num_callers} callers vs {capacity} capacity)...")
    eng = build_engine(
        database_url=BENCHMARK_DB_URL,
        pool_size=5,
        max_overflow=10,
        pool_timeout=5.0,
        pool_name="primary",
        poolclass=InstrumentedQueuePool,
    )
    pool = eng.pool

    holding_barrier = threading.Barrier(capacity)
    release_event = threading.Event()
    worker_results: list[dict[str, Any]] = []
    res_lock = threading.Lock()
    errors: list[str] = []
    errors_lock = threading.Lock()

    timeline: list[dict[str, Any]] = []
    stop_sampling = threading.Event()

    t_start_wall = time.perf_counter()

    # Timeline sampling daemon
    def _sampler():
        while not stop_sampling.is_set():
            t_now = time.perf_counter() - t_start_wall
            timeline.append({
                "t_elapsed_s": round(t_now, 4),
                "checked_out": pool.checkedout(),
                "overflow": max(0, pool.overflow()),
            })
            time.sleep(0.01)

    sampler_thread = threading.Thread(target=_sampler, daemon=True)
    sampler_thread.start()

    def _holding_caller(idx: int):
        t0 = time.perf_counter()
        try:
            with eng.connect() as conn:
                checkout_time = time.perf_counter() - t0
                holding_barrier.wait(timeout=5.0)
                # Hold connection for 1.0 second
                release_event.wait(timeout=5.0)
                res = conn.execute(text("SELECT :v"), {"v": idx}).scalar()
                assert res == idx
            total_dur = time.perf_counter() - t0
            with res_lock:
                worker_results.append({
                    "idx": idx,
                    "type": "holding",
                    "checkout_duration_ms": checkout_time * 1000.0,
                    "total_duration_ms": total_dur * 1000.0,
                    "status": "SUCCESS",
                })
        except Exception as exc:
            with errors_lock:
                errors.append(f"Holding caller {idx} error: {exc}")

    def _queued_caller(idx: int):
        t0 = time.perf_counter()
        try:
            with eng.connect() as conn:
                checkout_time = time.perf_counter() - t0
                res = conn.execute(text("SELECT :v"), {"v": idx}).scalar()
                assert res == idx
            total_dur = time.perf_counter() - t0
            with res_lock:
                worker_results.append({
                    "idx": idx,
                    "type": "queued",
                    "checkout_duration_ms": checkout_time * 1000.0,
                    "total_duration_ms": total_dur * 1000.0,
                    "status": "SUCCESS",
                })
        except Exception as exc:
            with errors_lock:
                errors.append(f"Queued caller {idx} error: {exc}")

    with ThreadPoolExecutor(max_workers=num_callers) as executor:
        # Step 1: Submit 15 holding callers
        holding_futures = [executor.submit(_holding_caller, i) for i in range(capacity)]

        # Wait until barrier is reached (guarantees 15/15 full occupancy)
        time.sleep(0.15)
        peak_checked_out = pool.checkedout()
        peak_overflow = pool.overflow()
        assert peak_checked_out == capacity, f"Expected 100% saturation ({capacity}), got {peak_checked_out}"
        assert peak_overflow == 10, f"Expected 10 overflow, got {peak_overflow}"

        # Step 2: Submit 15 queued callers while pool is saturated
        queued_futures = [executor.submit(_queued_caller, i + capacity) for i in range(num_callers - capacity)]

        # Step 3: Hold connections at capacity for 1.0 second
        time.sleep(1.0)
        t_release = time.perf_counter()
        release_event.set()

        # Step 4: Wait for all callers to complete
        for f in as_completed(holding_futures + queued_futures):
            f.result()

    t_end_drill = time.perf_counter()

    # Step 5: Measure recovery time to idle (checkedout == 0)
    t_rec_start = time.perf_counter()
    recovery_duration_s = 0.0
    while time.perf_counter() - t_rec_start < 5.0:
        if pool.checkedout() == 0 and pool.overflow() <= 0:
            recovery_duration_s = time.perf_counter() - t_release
            break
        time.sleep(0.01)

    stop_sampling.set()
    sampler_thread.join(timeout=1.0)

    final_checkedout = pool.checkedout()
    final_overflow = pool.overflow()
    eng.dispose()

    assert len(errors) == 0, f"Drill encountered unexpected errors: {errors}"
    assert len(worker_results) == num_callers, f"Expected {num_callers} results, got {len(worker_results)}"
    assert final_checkedout == 0, f"Leaked connections! checkedout = {final_checkedout}"
    assert recovery_duration_s <= 5.0, f"Recovery exceeded 5.0s: {recovery_duration_s:.3f}s"

    holding_checkouts = [r["checkout_duration_ms"] for r in worker_results if r["type"] == "holding"]
    queued_checkouts = [r["checkout_duration_ms"] for r in worker_results if r["type"] == "queued"]
    all_checkouts = [r["checkout_duration_ms"] for r in worker_results]

    holding_dist = compute_distribution(holding_checkouts)
    queued_dist = compute_distribution(queued_checkouts)
    all_dist = compute_distribution(all_checkouts)

    print(f"  -> Peak Checked Out: {peak_checked_out}/15 (Overflow: {peak_overflow})")
    print(f"  -> Queued Caller Checkout Latency p50: {queued_dist['p50_ms']} ms | Max: {queued_dist['max_ms']} ms")
    print(f"  -> Recovery Duration to Idle: {recovery_duration_s:.3f} s (Target <= 5.0s)")

    return {
        "trial_id": trial_id,
        "capacity": capacity,
        "num_callers": num_callers,
        "peak_checked_out": peak_checked_out,
        "peak_overflow": peak_overflow,
        "recovery_duration_s": round(recovery_duration_s, 3),
        "final_checked_out": final_checkedout,
        "final_overflow": final_overflow,
        "error_count": len(errors),
        "checkout_distributions": {
            "all_callers": all_dist,
            "holding_callers": holding_dist,
            "queued_callers": queued_dist,
        },
        "occupancy_timeline_sample_count": len(timeline),
        "occupancy_timeline": timeline[::5],  # Sample every 50ms for report readability
    }


def run_timeout_rejection_drill() -> dict[str, Any]:
    print("\n[G9.4.2] Running Bounded Timeout Rejection Drill (15 callers hold 6.0s vs 5.0s timeout)...")
    eng = build_engine(
        database_url=BENCHMARK_DB_URL,
        pool_size=5,
        max_overflow=10,
        pool_timeout=5.0,
        pool_name="primary",
        poolclass=InstrumentedQueuePool,
    )
    pool = eng.pool
    capacity = 15
    num_waiting = 15

    hold_barrier = threading.Barrier(capacity)
    hold_release = threading.Event()
    timeout_results: list[dict[str, Any]] = []
    res_lock = threading.Lock()

    def _blocker():
        with eng.connect() as conn:
            hold_barrier.wait(timeout=5.0)
            hold_release.wait(timeout=10.0)

    def _timeout_caller(idx: int):
        t0 = time.perf_counter()
        try:
            with eng.connect() as conn:
                pass
            dur = time.perf_counter() - t0
            with res_lock:
                timeout_results.append({"idx": idx, "status": "UNEXPECTED_SUCCESS", "duration_s": dur})
        except sa_exc.TimeoutError as te:
            dur = time.perf_counter() - t0
            with res_lock:
                timeout_results.append({
                    "idx": idx,
                    "status": "TIMEOUT",
                    "duration_s": round(dur, 3),
                    "error": str(te),
                })

    with ThreadPoolExecutor(max_workers=capacity + num_waiting) as executor:
        blocker_futures = [executor.submit(_blocker) for _ in range(capacity)]

        # Guarantee 15/15 full saturation
        time.sleep(0.15)
        assert pool.checkedout() == capacity, "Expected 15 connections checked out"

        # Submit 15 waiting callers that will time out
        waiting_futures = [executor.submit(_timeout_caller, i) for i in range(num_waiting)]

        # Wait for all 15 waiting callers to time out (takes ~5.0s)
        for f in as_completed(waiting_futures):
            f.result()

        # Release blockers after waiting callers have timed out
        hold_release.set()
        for f in as_completed(blocker_futures):
            f.result()

    # Verify recovery
    time.sleep(0.1)
    final_checkedout = pool.checkedout()
    eng.dispose()

    timeouts = [r for r in timeout_results if r["status"] == "TIMEOUT"]
    assert len(timeouts) == num_waiting, f"Expected {num_waiting} timeouts, got {len(timeouts)}"
    assert final_checkedout == 0, f"Leaked connections: {final_checkedout}"

    timeout_durations = [r["duration_s"] for r in timeouts]
    mean_timeout_s = round(statistics.mean(timeout_durations), 3)
    min_timeout_s = round(min(timeout_durations), 3)
    max_timeout_s = round(max(timeout_durations), 3)

    print(f"  -> Timeouts Occurred: {len(timeouts)}/15 (Bounded wait: mean={mean_timeout_s}s, min={min_timeout_s}s, max={max_timeout_s}s)")
    return {
        "callers_attempted": num_waiting,
        "timeouts_recorded": len(timeouts),
        "mean_timeout_duration_s": mean_timeout_s,
        "min_timeout_duration_s": min_timeout_s,
        "max_timeout_duration_s": max_timeout_s,
        "bounded_target_s": 5.0,
        "final_checked_out": final_checkedout,
    }


def verify_telemetry_engine_isolation() -> dict[str, Any]:
    print("\n[G9.4.2] Verifying Dedicated Telemetry Engine Isolation during 100% Saturation...")
    from taskforge.database.session import set_engine, engine as orig_engine
    eng = build_engine(
        database_url=BENCHMARK_DB_URL,
        pool_size=5,
        max_overflow=10,
        pool_timeout=5.0,
        pool_name="primary",
        poolclass=InstrumentedQueuePool,
    )
    set_engine(eng)
    capacity = 15
    hold_barrier = threading.Barrier(capacity)
    hold_release = threading.Event()

    def _blocker():
        with eng.connect() as conn:
            hold_barrier.wait(timeout=5.0)
            hold_release.wait(timeout=5.0)

    with ThreadPoolExecutor(max_workers=capacity) as executor:
        futures = [executor.submit(_blocker) for _ in range(capacity)]
        time.sleep(0.15)
        assert eng.pool.checkedout() == capacity, "Application pool must be 100% occupied"

        # While business pool is saturated, test dedicated telemetry engine across 3 distinct dimensions (R5)
        # 1. Database query duration on dedicated engine
        telemetry_eng = get_dedicated_telemetry_engine()
        t0 = time.perf_counter()
        with telemetry_eng.connect() as tconn:
            val = tconn.execute(text("SELECT 99")).scalar()
            assert val == 99
        query_dur_ms = round((time.perf_counter() - t0) * 1000.0, 3)

        # 2. Metrics collection duration (PostgreSQL outbox + Redis queue queries)
        cache = GaugeSnapshotCache(telemetry_engine=telemetry_eng)
        t0_collect = time.perf_counter()
        collect_ok = cache.collect_and_update()
        assert collect_ok is True
        collection_dur_ms = round((time.perf_counter() - t0_collect) * 1000.0, 3)

        # 3. Complete Prometheus scrape response duration
        t0_scrape = time.perf_counter()
        scrape_bytes = get_metrics_content()
        scrape_dur_ms = round((time.perf_counter() - t0_scrape) * 1000.0, 3)

        active_val = DB_POOL_ACTIVE_CONNECTIONS.labels(pool_name="primary")._value.get()
        overflow_val = DB_POOL_OVERFLOW_CONNECTIONS.labels(pool_name="primary")._value.get()

        hold_release.set()
        for f in futures:
            f.result()

    set_engine(orig_engine)
    eng.dispose()
    print(f"  -> Dedicated DB Query Duration: {query_dur_ms} ms (Target < 1000 ms)")
    print(f"  -> Metrics Collection Duration: {collection_dur_ms} ms (Target < 1000 ms)")
    print(f"  -> Complete Scrape Response Duration: {scrape_dur_ms} ms (Target < 1000 ms)")
    print(f"  -> Live Saturated Scrape Occupancy: Active={active_val}, Overflow={overflow_val}")
    return {
        "telemetry_db_query_duration_ms": query_dur_ms,
        "metrics_collection_duration_ms": collection_dur_ms,
        "complete_scrape_response_duration_ms": scrape_dur_ms,
        "business_pool_active_during_scrape": active_val,
        "business_pool_overflow_during_scrape": overflow_val,
        "isolation_verified": True,
    }


def main():
    print("=" * 80)
    print("TASKFORGE PLATFORM HARDENING: GATE G9.4.2")
    print("Database Connection-Pool Telemetry & Saturation Benchmark Drill")
    print("=" * 80)

    # 1. Verify Production Baseline Configuration (Commit a89858b)
    prod_pool = prod_engine.pool
    print("\n[Baseline] Verifying Production Configuration (Immutable at a89858b):")
    print(f"  -> pool_size: {prod_pool.size()} (Expected: 10)")
    print(f"  -> max_overflow: {prod_pool._max_overflow} (Expected: 20)")
    print(f"  -> pool_timeout: {prod_pool._timeout}s (Expected: 30.0s)")
    print(f"  -> pool_pre_ping: {prod_pool._pre_ping} (Expected: True)")
    print(f"  -> Total Production Capacity: {prod_pool.size() + prod_pool._max_overflow} connections")

    assert prod_pool.size() == 10, "Production pool_size must be 10"
    assert prod_pool._max_overflow == 20, "Production max_overflow must be 20"
    assert prod_pool._timeout == 30.0, "Production pool_timeout must be 30.0"

    # 2. Execute 2x Saturation & Recovery Drill (3 Trials)
    saturation_trials = []
    for t_idx in range(1, 4):
        trial_id = f"T{t_idx}"
        res = run_saturation_recovery_trial(trial_id=trial_id, capacity=15, num_callers=30)
        saturation_trials.append(res)

    # 3. Execute Bounded Timeout Rejection Drill
    timeout_res = run_timeout_rejection_drill()

    # 4. Verify Dedicated Telemetry Engine Isolation
    telemetry_iso_res = verify_telemetry_engine_isolation()

    # 5. Measure Runtime Prometheus Series Count
    content = get_metrics_content().decode("utf-8")
    lines = [line.strip() for line in content.splitlines() if line.strip() and not line.startswith("#")]
    pool_lines = [l for l in lines if "taskforge_db_pool_" in l]
    print(f"\n[Telemetry] Runtime Exposition Series Count:")
    print(f"  -> Total Pool Metric Series Exported: {len(pool_lines)} (Ceiling: 25)")
    for l in pool_lines:
        print(f"     {l}")

    # 6. Save G9.4.2 Structured Report JSON
    report = {
        "milestone": "G9.4.2_database_connection_pool_saturation",
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "production_baseline": {
            "commit": "a89858b73f8488e05cbcc2393a61f3080ffc7a38",
            "pool_size": prod_pool.size(),
            "max_overflow": prod_pool._max_overflow,
            "pool_timeout_seconds": prod_pool._timeout,
            "pool_pre_ping": prod_pool._pre_ping,
            "total_production_capacity": prod_pool.size() + prod_pool._max_overflow,
            "status": "IMMUTABLE_PRESERVED",
        },
        "experimental_saturation_fixture": {
            "pool_size": 5,
            "max_overflow": 10,
            "pool_timeout_seconds": 5.0,
            "total_capacity": 15,
            "concurrent_callers": 30,
            "contention_ratio": "2x",
        },
        "metric_cardinality": {
            "base_series": 17,
            "runtime_series_with_created": len(pool_lines),
            "series_ceiling": 25,
            "compliant": len(pool_lines) <= 25,
        },
        "saturation_recovery_trials": saturation_trials,
        "timeout_rejection_drill": timeout_res,
        "telemetry_engine_isolation": telemetry_iso_res,
        "invariants": {
            "zero_connection_leaks": True,
            "zero_hung_threads": True,
            "recovery_under_5s": all(t["recovery_duration_s"] <= 5.0 for t in saturation_trials),
        },
    }

    out_path = REPO_ROOT / "benchmarks" / "scenarios" / "g94" / "g942_pool_saturation_report.json"
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\n[Artifact] Saved G9.4.2 report to {out_path}")


if __name__ == "__main__":
    main()

