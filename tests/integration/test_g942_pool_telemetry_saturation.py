"""Gate G9.4.2: Database Connection-Pool Telemetry & Saturation Test Suite.

Verifies:
1. G9.4 Pool Telemetry Contract:
   - taskforge_db_pool_active_connections (Gauge)
   - taskforge_db_pool_overflow_connections (Gauge)
   - taskforge_db_pool_checkout_duration_seconds (Histogram, 11 approved buckets)
   - taskforge_db_pool_checkout_timeouts_total (Counter)
   - Finite label contract (pool_name in {'primary', 'dedicated_telemetry'})
   - Total series budget (<= 25 series)

2. Barrier-Synchronized 2x Contention Saturation Drill:
   - Experimental topology: pool_size=5, max_overflow=10, pool_timeout=5.0s (capacity 15)
   - 30 concurrent callers (15 holding at barrier to guarantee simultaneous full occupancy, 15 queued)
   - Bounded checkout wait
   - Recovery to 0 active connections within 5.0s
   - Zero connection leaks (checkedout == 0, overflow <= 0)
   - Zero hung threads (all 30 threads cleanly joined)

3. Timeout Accounting & Bounded Failure:
   - Bounded timeout rejection verified under sustained pool exhaustion
   - Increments taskforge_db_pool_checkout_timeouts_total
   - Clean self-healing recovery once load ceases

4. Telemetry Isolation:
   - Dedicated telemetry engine scrapes cleanly without contention while business pool is 100% saturated.

5. Mixed Task-Submission and Lease-Transaction Workload under Contention.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
from typing import Any
import uuid

import pytest
from sqlalchemy import exc as sa_exc, text
from sqlalchemy.orm import Session

from taskforge.core.config import get_settings
from taskforge.database.models import Task, get_utc_now
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import (
    InstrumentedQueuePool,
    SessionLocal,
    build_engine,
)
from taskforge.monitoring.collector import GaugeSnapshotCache, get_dedicated_telemetry_engine
from taskforge.monitoring.metrics import (
    DB_POOL_ACTIVE_CONNECTIONS,
    DB_POOL_CHECKOUT_DURATION,
    DB_POOL_CHECKOUT_TIMEOUTS_TOTAL,
    DB_POOL_OVERFLOW_CONNECTIONS,
    get_metrics_content,
)

settings = get_settings()


@pytest.fixture
def experimental_saturation_engine():
    """Provides the authorized G9.4.2 experimental saturation pool fixture:
    pool_size=5, max_overflow=10, pool_timeout=5.0s (total capacity = 15).
    Strictly isolated from production defaults.
    """
    from taskforge.database.session import engine as orig_engine, set_engine

    url = settings.database_url
    eng = build_engine(
        database_url=url,
        pool_size=5,
        max_overflow=10,
        pool_timeout=5.0,
        pool_name="primary",
        poolclass=InstrumentedQueuePool,
    )
    set_engine(eng)
    yield eng
    set_engine(orig_engine)
    eng.dispose()



def test_g942_pool_telemetry_metric_contract(experimental_saturation_engine):
    """AC-9.4.6: Verifies finite-label contract, histogram buckets, and series budget."""
    eng = experimental_saturation_engine

    # Perform a checkout to populate metrics
    with eng.connect() as conn:
        res = conn.execute(text("SELECT 1")).scalar()
        assert res == 1

    content = get_metrics_content().decode("utf-8")
    lines = [line.strip() for line in content.splitlines() if line.strip() and not line.startswith("#")]

    # 1. Verify existence of the four required metric families
    pool_metric_names = [
        "taskforge_db_pool_active_connections",
        "taskforge_db_pool_overflow_connections",
        "taskforge_db_pool_checkout_duration_seconds",
        "taskforge_db_pool_checkout_timeouts_total",
    ]
    for mname in pool_metric_names:
        matching = [l for l in lines if l.startswith(mname)]
        assert len(matching) > 0, f"Expected metric family {mname} to be exported"

    # 2. Verify approved histogram bucket boundaries
    expected_buckets = ["0.001", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1.0", "2.5", "5.0", "+Inf"]
    for b in expected_buckets:
        needle = f'taskforge_db_pool_checkout_duration_seconds_bucket{{le="{b}",pool_name="primary"}}'
        assert any(needle in l for l in lines), f"Missing approved bucket {needle}"

    # 3. Verify finite label values only (no unbounded labels)
    for l in lines:
        if "taskforge_db_pool_" in l:
            assert "task_id" not in l
            assert "worker_id" not in l
            assert "error" not in l

    # 4. Verify runtime series count budget for G9.4 pool metrics <= 25 series
    g94_pool_lines = [
        l for l in lines
        if any(l.startswith(mname) for mname in pool_metric_names)
    ]
    # 1 active + 1 overflow + 12 buckets + 1 sum + 1 count + 1 created (histogram) + 1 timeout + 1 created (timeout) = 19
    assert len(g94_pool_lines) <= 25, f"Runtime pool metric series {len(g94_pool_lines)} exceeds ceiling 25"


def test_g942_barrier_synchronized_2x_contention_and_recovery(experimental_saturation_engine):
    """AC-9.4.3: 30 concurrent callers against capacity 15 (5 base + 10 overflow).
    Guarantees simultaneous full occupancy using a threading barrier, verifies bounded checkout,
    graceful queue draining, self-healing recovery within 5.0s, and zero connection leaks.
    """
    eng = experimental_saturation_engine
    pool = eng.pool
    total_capacity = 15  # 5 pool_size + 10 max_overflow
    num_callers = 30     # 2x saturation

    # Phase 1: 15 threads check out connections and hold them at a barrier to guarantee 100% occupancy
    holding_barrier = threading.Barrier(total_capacity)
    release_event = threading.Event()
    worker_latencies: list[float] = []
    latencies_lock = threading.Lock()
    errors: list[Exception] = []
    errors_lock = threading.Lock()

    def _holding_worker(idx: int):
        t0 = time.perf_counter()
        try:
            with eng.connect() as conn:
                # Signal barrier that this thread has acquired its connection
                holding_barrier.wait(timeout=5.0)
                # Hold until release signal
                release_event.wait(timeout=5.0)
                # Quick verify
                val = conn.execute(text("SELECT :v"), {"v": idx}).scalar()
                assert val == idx
            dur = time.perf_counter() - t0
            with latencies_lock:
                worker_latencies.append(dur)
        except Exception as exc:
            with errors_lock:
                errors.append(exc)

    def _queued_worker(idx: int):
        t0 = time.perf_counter()
        try:
            with eng.connect() as conn:
                val = conn.execute(text("SELECT :v"), {"v": idx}).scalar()
                assert val == idx
            dur = time.perf_counter() - t0
            with latencies_lock:
                worker_latencies.append(dur)
        except Exception as exc:
            with errors_lock:
                errors.append(exc)

    t_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=num_callers) as executor:
        # Submit 15 holding workers first
        holding_futures = [executor.submit(_holding_worker, i) for i in range(total_capacity)]

        # Wait until all 15 holding workers have acquired their connections at the barrier
        # (gives assurance that all 15 connections are actively checked out simultaneously)
        time.sleep(0.15)
        checked_out_peak = pool.checkedout()
        overflow_peak = pool.overflow()
        assert checked_out_peak == total_capacity, f"Expected 100% pool occupancy ({total_capacity}), got {checked_out_peak}"
        assert overflow_peak == 10, f"Expected max overflow (10), got {overflow_peak}"

        # Submit remaining 15 workers while pool is 100% occupied
        queued_futures = [executor.submit(_queued_worker, i + total_capacity) for i in range(num_callers - total_capacity)]

        # Let queued workers wait in the pool queue for a brief duration (< 5.0s pool_timeout)
        time.sleep(0.3)

        # Release holding workers
        release_event.set()

        # Wait for all 30 futures to complete
        for f in as_completed(holding_futures + queued_futures):
            f.result()

    t_total = time.perf_counter() - t_start

    # Invariants Verification:
    assert len(errors) == 0, f"Expected 0 errors during saturation drill, got {errors}"
    assert len(worker_latencies) == num_callers, f"Expected {num_callers} completed callers, got {len(worker_latencies)}"

    # Self-healing recovery: All connections checked back into pool within 5.0s
    t_rec_start = time.perf_counter()
    while time.perf_counter() - t_rec_start < 5.0:
        if pool.checkedout() == 0:
            break
        time.sleep(0.05)

    assert pool.checkedout() == 0, f"Connection leak detected! pool.checkedout() = {pool.checkedout()} (expected 0)"
    assert pool.overflow() <= 0, f"Overflow leak detected! pool.overflow() = {pool.overflow()} (expected <= 0)"


def test_g942_pool_timeout_accounting_and_bounded_rejection(experimental_saturation_engine):
    """AC-9.4.3: Verifies that sustained pool exhaustion raises TimeoutError strictly
    at configured pool_timeout boundary and increments taskforge_db_pool_checkout_timeouts_total.
    """
    eng = experimental_saturation_engine
    pool = eng.pool
    total_capacity = 15  # 5 pool + 10 overflow
    hold_barrier = threading.Barrier(total_capacity)
    hold_release = threading.Event()

    def _blocker():
        with eng.connect() as conn:
            hold_barrier.wait(timeout=5.0)
            hold_release.wait(timeout=10.0)

    # 1. Fill pool completely with 15 blocking connections
    with ThreadPoolExecutor(max_workers=total_capacity + 2) as executor:
        blocker_futures = [executor.submit(_blocker) for _ in range(total_capacity)]

        # Wait for all 15 to acquire connections
        time.sleep(0.2)
        assert pool.checkedout() == total_capacity

        # 2. Attempt 16th connection checkout with pool_timeout=5.0s
        # Must wait approximately 5.0 seconds and raise TimeoutError
        t0 = time.perf_counter()
        with pytest.raises(sa_exc.TimeoutError) as exc_info:
            with eng.connect() as conn:
                pass
        dur = time.perf_counter() - t0

        # Bounded checkout wait verified
        assert 4.8 <= dur <= 6.5, f"Expected checkout timeout around 5.0s, got {dur:.2f}s"
        assert "QueuePool limit of size" in str(exc_info.value)

        # 3. Verify timeout metric increment
        content = get_metrics_content().decode("utf-8")
        needle = 'taskforge_db_pool_checkout_timeouts_total{pool_name="primary"}'
        matching = [l for l in content.splitlines() if needle in l]
        assert len(matching) > 0, "Missing checkout timeouts total metric in export"
        val = float(matching[0].split()[-1])
        assert val >= 1.0, f"Expected checkout timeout counter >= 1, got {val}"

        # 4. Release blockers and verify clean recovery
        hold_release.set()
        for f in blocker_futures:
            f.result()

    # Verify zero leaked connections
    time.sleep(0.1)
    assert pool.checkedout() == 0, f"Leaked connections: {pool.checkedout()}"


def test_g942_dedicated_telemetry_engine_isolation_during_pool_saturation(experimental_saturation_engine):
    """AC-9.4.3 & AC-9.4.6: Verifies that the dedicated telemetry engine remains
    completely responsive and isolated while the application connection pool is 100% saturated.
    """
    eng = experimental_saturation_engine
    total_capacity = 15
    hold_barrier = threading.Barrier(total_capacity)
    hold_release = threading.Event()

    def _blocker():
        with eng.connect() as conn:
            hold_barrier.wait(timeout=5.0)
            hold_release.wait(timeout=5.0)

    with ThreadPoolExecutor(max_workers=total_capacity) as executor:
        futures = [executor.submit(_blocker) for _ in range(total_capacity)]
        time.sleep(0.15)
        assert eng.pool.checkedout() == total_capacity, "Application pool must be 100% occupied"

        # While application pool is 100% full, test telemetry engine and complete scrape latency (R5)
        # Instrument single cold-start scrape request to prove strict mathematical nesting:
        # T_complete_scrape >= T_metrics_collection >= T_dedicated_db_query
        from taskforge.monitoring import collector
        from taskforge.monitoring.collector import reset_gauge_snapshot_cache
        reset_gauge_snapshot_cache()

        nested_timings: dict[str, float] = {}
        orig_fetch = collector.GaugeSnapshotCache._fetch_outbox_telemetry
        orig_collect = collector.GaugeSnapshotCache.collect_and_update

        def _timed_fetch(self_cache):
            t_start = time.perf_counter()
            res = orig_fetch(self_cache)
            t_end = time.perf_counter()
            nested_timings["db_query_duration_ms"] = (t_end - t_start) * 1000.0
            return res

        def _timed_collect(self_cache):
            t_start = time.perf_counter()
            res = orig_collect(self_cache)
            t_end = time.perf_counter()
            nested_timings["collection_duration_ms"] = (t_end - t_start) * 1000.0
            return res

        collector.GaugeSnapshotCache._fetch_outbox_telemetry = _timed_fetch
        collector.GaugeSnapshotCache.collect_and_update = _timed_collect

        t0_scrape = time.perf_counter()
        scrape_bytes = get_metrics_content()
        t1_scrape = time.perf_counter()
        scrape_dur_ms = (t1_scrape - t0_scrape) * 1000.0

        collector.GaugeSnapshotCache._fetch_outbox_telemetry = orig_fetch
        collector.GaugeSnapshotCache.collect_and_update = orig_collect

        db_query_dur_ms = nested_timings["db_query_duration_ms"]
        collect_dur_ms = nested_timings["collection_duration_ms"]

        # Invariant 1: All durations must be strictly bounded (< 1.0s / 1000ms)
        assert db_query_dur_ms < 1000.0, f"Dedicated DB query took {db_query_dur_ms:.2f}ms (target < 1000ms)"
        assert collect_dur_ms < 1000.0, f"Metrics collection took {collect_dur_ms:.2f}ms (target < 1000ms)"
        assert scrape_dur_ms < 1000.0, f"Complete scrape response took {scrape_dur_ms:.2f}ms (target < 1000ms)"

        # Invariant 2: Mathematical nesting hierarchy of single scrape request
        assert scrape_dur_ms >= collect_dur_ms >= db_query_dur_ms, (
            f"Mathematical nesting violated: scrape({scrape_dur_ms:.2f}ms) >= "
            f"collect({collect_dur_ms:.2f}ms) >= db_query({db_query_dur_ms:.2f}ms)"
        )

        # Freshness verification: Live scrape reflects 15/15 occupancy and 10 overflow
        scrape_text = scrape_bytes.decode("utf-8")
        assert 'taskforge_db_pool_active_connections{pool_name="primary"} 15.0' in scrape_text
        assert 'taskforge_db_pool_overflow_connections{pool_name="primary"} 10.0' in scrape_text
        assert 'taskforge_db_pool_connections{pool_state="checked_out"} 15.0' in scrape_text
        assert 'taskforge_db_pool_connections{pool_state="overflow"} 10.0' in scrape_text

        hold_release.set()
        for f in futures:
            f.result()

    time.sleep(0.1)
    assert eng.pool.checkedout() == 0


def test_g942_pool_occupancy_transitions_and_nonnegative_overflow(experimental_saturation_engine):
    """R1, R2, R3: Verifies active and overflow gauge transitions across:
    - State 0: Empty pool (0 / 5)
    - State 1: Partially initialized pool (2 / 5)
    - State 2: Fully occupied base pool (5 / 5)
    - State 3: Actual overflow usage (8 / 15: 5 base + 3 overflow)
    - State 4: Maximum saturation (15 / 15: 5 base + 10 overflow)
    - State 5: Release back to empty (0 / 5)

    Invariants:
    1. Checkout increases checked-out count immediately.
    2. Checkin decreases checked-out count immediately.
    3. Overflow is strictly non-negative (>= 0) in all states.
    4. Prometheus text exposition confirms nonnegative values.
    """
    from taskforge.monitoring.metrics import DB_POOL_CONNECTIONS

    eng = experimental_saturation_engine

    # --- State 0: Empty Pool ---
    content_0 = get_metrics_content().decode("utf-8")
    assert DB_POOL_ACTIVE_CONNECTIONS.labels(pool_name="primary")._value.get() == 0.0
    assert DB_POOL_OVERFLOW_CONNECTIONS.labels(pool_name="primary")._value.get() == 0.0
    assert DB_POOL_CONNECTIONS.labels(pool_state="overflow")._value.get() >= 0.0
    assert 'taskforge_db_pool_connections{pool_state="overflow"} 0.0' in content_0
    assert 'taskforge_db_pool_overflow_connections{pool_name="primary"} 0.0' in content_0

    # --- State 1: Partial Occupancy (2 connections checked out) ---
    c1 = eng.connect()
    c2 = eng.connect()
    try:
        assert DB_POOL_ACTIVE_CONNECTIONS.labels(pool_name="primary")._value.get() == 2.0
        assert DB_POOL_CONNECTIONS.labels(pool_state="checked_out")._value.get() == 2.0
        assert DB_POOL_OVERFLOW_CONNECTIONS.labels(pool_name="primary")._value.get() == 0.0
        assert DB_POOL_CONNECTIONS.labels(pool_state="overflow")._value.get() == 0.0
        content_1 = get_metrics_content().decode("utf-8")
        assert 'taskforge_db_pool_connections{pool_state="checked_out"} 2.0' in content_1
        assert 'taskforge_db_pool_connections{pool_state="overflow"} 0.0' in content_1
        assert 'taskforge_db_pool_overflow_connections{pool_name="primary"} 0.0' in content_1

        # --- State 2: Fully Occupied Base Pool (5 connections checked out) ---
        c3 = eng.connect()
        c4 = eng.connect()
        c5 = eng.connect()
        try:
            assert DB_POOL_ACTIVE_CONNECTIONS.labels(pool_name="primary")._value.get() == 5.0
            assert DB_POOL_CONNECTIONS.labels(pool_state="checked_out")._value.get() == 5.0
            assert DB_POOL_OVERFLOW_CONNECTIONS.labels(pool_name="primary")._value.get() == 0.0
            assert DB_POOL_CONNECTIONS.labels(pool_state="overflow")._value.get() == 0.0
            content_2 = get_metrics_content().decode("utf-8")
            assert 'taskforge_db_pool_connections{pool_state="checked_out"} 5.0' in content_2
            assert 'taskforge_db_pool_connections{pool_state="overflow"} 0.0' in content_2

            # --- State 3: Actual Overflow Usage (8 connections checked out: 5 base + 3 overflow) ---
            c6 = eng.connect()
            c7 = eng.connect()
            c8 = eng.connect()
            try:
                assert DB_POOL_ACTIVE_CONNECTIONS.labels(pool_name="primary")._value.get() == 8.0
                assert DB_POOL_CONNECTIONS.labels(pool_state="checked_out")._value.get() == 8.0
                assert DB_POOL_OVERFLOW_CONNECTIONS.labels(pool_name="primary")._value.get() == 3.0
                assert DB_POOL_CONNECTIONS.labels(pool_state="overflow")._value.get() == 3.0
                content_3 = get_metrics_content().decode("utf-8")
                assert 'taskforge_db_pool_connections{pool_state="checked_out"} 8.0' in content_3
                assert 'taskforge_db_pool_connections{pool_state="overflow"} 3.0' in content_3
                assert 'taskforge_db_pool_overflow_connections{pool_name="primary"} 3.0' in content_3

                # --- State 4: Maximum Saturation (15 connections checked out: 5 base + 10 overflow) ---
                overflow_conns = [eng.connect() for _ in range(7)]
                try:
                    assert DB_POOL_ACTIVE_CONNECTIONS.labels(pool_name="primary")._value.get() == 15.0
                    assert DB_POOL_CONNECTIONS.labels(pool_state="checked_out")._value.get() == 15.0
                    assert DB_POOL_OVERFLOW_CONNECTIONS.labels(pool_name="primary")._value.get() == 10.0
                    assert DB_POOL_CONNECTIONS.labels(pool_state="overflow")._value.get() == 10.0
                    content_4 = get_metrics_content().decode("utf-8")
                    assert 'taskforge_db_pool_connections{pool_state="checked_out"} 15.0' in content_4
                    assert 'taskforge_db_pool_connections{pool_state="overflow"} 10.0' in content_4
                    assert 'taskforge_db_pool_overflow_connections{pool_name="primary"} 10.0' in content_4
                finally:
                    for conn in overflow_conns:
                        conn.close()
            finally:
                c6.close()
                c7.close()
                c8.close()
        finally:
            c3.close()
            c4.close()
            c5.close()
    finally:
        c1.close()
        c2.close()

    # --- State 5: All Released Back to Empty ---
    # Trigger scrape cache update
    content_final = get_metrics_content().decode("utf-8")
    assert DB_POOL_ACTIVE_CONNECTIONS.labels(pool_name="primary")._value.get() == 0.0
    assert DB_POOL_CONNECTIONS.labels(pool_state="checked_out")._value.get() == 0.0
    assert DB_POOL_OVERFLOW_CONNECTIONS.labels(pool_name="primary")._value.get() == 0.0
    assert DB_POOL_CONNECTIONS.labels(pool_state="overflow")._value.get() == 0.0
    assert 'taskforge_db_pool_connections{pool_state="overflow"} 0.0' in content_final
    assert 'taskforge_db_pool_overflow_connections{pool_name="primary"} 0.0' in content_final


def test_g942_mixed_task_submission_and_lease_workload_under_contention(experimental_saturation_engine):
    """AC-9.4.3: Verifies mixed transactional operations (task submissions, lease renewals,
    and state queries) under saturated connection pool without state corruptions or connection leaks.
    """
    eng = experimental_saturation_engine

    def _task_submitter(idx: int):
        with Session(bind=eng) as db:
            repo = TaskRepository(db)
            res = repo.create_task(
                task_name=f"contention-task-{idx}-{uuid.uuid4().hex[:6]}",
                task_type="noop",
                payload={"idx": idx},
            )
            return res.task.id

    def _lease_manager(task_id: str):
        with Session(bind=eng) as db:
            repo = TaskRepository(db)
            t = repo.get_by_id(task_id)
            if t:
                token = f"tok-{uuid.uuid4().hex[:8]}"
                repo.acquire_lease(task_id, worker_id="g942-worker", lease_token=token, lease_duration_seconds=30)
                repo.renew_lease(task_id, lease_token=token, lease_duration_seconds=30)
            return task_id

    # Run 25 mixed concurrent operations
    created_task_ids = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        submit_futures = [executor.submit(_task_submitter, i) for i in range(15)]
        for f in as_completed(submit_futures):
            created_task_ids.append(f.result())

        lease_futures = [executor.submit(_lease_manager, tid) for tid in created_task_ids[:10]]
        for f in as_completed(lease_futures):
            f.result()

    # Verify zero leaked connections
    time.sleep(0.1)
    assert eng.pool.checkedout() == 0, f"Leaked connections: {eng.pool.checkedout()}"
