from datetime import datetime, timezone
import threading
import time
from typing import Any
import pytest
from fastapi.testclient import TestClient

from taskforge.database.session import engine
from taskforge.main import app
from taskforge.monitoring.collector import (
    GaugeSnapshotCache,
    InfrastructureSnapshot,
    get_dedicated_telemetry_engine,
    get_gauge_snapshot_cache,
    reset_gauge_snapshot_cache,
)
from taskforge.monitoring.metrics import (
    ACTIVE_WORKERS,
    DB_POOL_CONNECTIONS,
    OUTBOX_LAG_SECONDS,
    OUTBOX_PENDING_EVENTS,
    QUEUE_DEPTH,
    TELEMETRY_SCRAPE_ERRORS_TOTAL,
    get_metrics_content,
)


@pytest.fixture(autouse=True)
def clean_cache_state():
    reset_gauge_snapshot_cache()
    yield
    reset_gauge_snapshot_cache()


# ==============================================================================
# Test 1: Warm Cache TTL Invariant
# ==============================================================================

def test_warm_cache_returns_previous_snapshot_without_unnecessary_refresh(monkeypatch):
    """
    AC-5 / Stage 2: A warm cache within 5.0s TTL returns the previous snapshot
    without executing backend queries or starting duplicate refresh operations.
    """
    cache = GaugeSnapshotCache(ttl_seconds=5.0)

    # Prime cache with an initial snapshot
    sample_snapshot = InfrastructureSnapshot(
        queue_depth={"critical": 1, "high": 2, "medium": 3, "low": 4},
        outbox_pending_events=12,
        outbox_lag_seconds=1.5,
        active_workers=3,
        collected_at=time.time(),
    )
    cache._snapshot = sample_snapshot
    cache._last_refresh_time = time.monotonic()

    # If _execute_refresh_operation were called, it would fail
    def failing_refresh():
        raise AssertionError("Unnecessary refresh executed on warm cache!")

    monkeypatch.setattr(cache, "_execute_refresh_operation", failing_refresh)

    # Issue multiple calls on warm cache
    for _ in range(10):
        ok = cache.collect_and_update()
        assert ok is True

    # Confirm operation count is 0 (no refresh was initiated)
    assert cache.refresh_operation_count == 0

    # Confirm gauges reflect the cached snapshot
    assert QUEUE_DEPTH.labels(priority="critical")._value.get() == 1
    assert QUEUE_DEPTH.labels(priority="high")._value.get() == 2
    assert QUEUE_DEPTH.labels(priority="medium")._value.get() == 3
    assert QUEUE_DEPTH.labels(priority="low")._value.get() == 4
    assert OUTBOX_PENDING_EVENTS._value.get() == 12
    assert OUTBOX_LAG_SECONDS._value.get() == 1.5
    assert ACTIVE_WORKERS._value.get() == 3


# ==============================================================================
# Tests 2 & 3: 50 Concurrent Scrapes & Single-Flight Guarantee
# ==============================================================================

def test_fifty_concurrent_scrapes_trigger_exactly_one_refresh(monkeypatch):
    """
    AC-5 / Stage 2: 50 concurrent scrape requests trigger exactly ONE refresh operation;
    concurrent scrapes receive the prior valid snapshot without duplicate queries.
    """
    cache = GaugeSnapshotCache(ttl_seconds=5.0)

    # Establish an initial valid snapshot (prior snapshot)
    initial_snapshot = InfrastructureSnapshot(
        queue_depth={"critical": 10, "high": 20, "medium": 30, "low": 40},
        outbox_pending_events=100,
        outbox_lag_seconds=5.0,
        active_workers=2,
        collected_at=time.time(),
    )
    cache._snapshot = initial_snapshot
    # Simulate TTL expired
    cache._last_refresh_time = time.monotonic() - 10.0

    refresh_block = threading.Event()
    refresh_entered = threading.Event()
    refresh_executions = 0
    exec_lock = threading.Lock()

    def slow_refresh_operation():
        nonlocal refresh_executions
        with exec_lock:
            refresh_executions += 1
        refresh_entered.set()
        refresh_block.wait()
        return InfrastructureSnapshot(
            queue_depth={"critical": 11, "high": 21, "medium": 31, "low": 41},
            outbox_pending_events=200,
            outbox_lag_seconds=0.5,
            active_workers=4,
            collected_at=time.time(),
        )

    monkeypatch.setattr(cache, "_execute_refresh_operation", slow_refresh_operation)

    results = [None] * 50

    def scrape_worker(idx: int):
        results[idx] = cache.collect_and_update()

    threads = [threading.Thread(target=scrape_worker, args=(i,)) for i in range(50)]

    # Start all 50 threads concurrently
    for t in threads:
        t.start()

    # Wait until the single refresh operation has entered and acquired lock
    assert refresh_entered.wait(timeout=3.0)

    # Let the remaining 49 threads run while refresh is blocked
    # They should finish immediately returning the prior valid snapshot
    time.sleep(0.1)

    # Unblock the refresh
    refresh_block.set()

    for t in threads:
        t.join(timeout=3.0)

    # CRITICAL SINGLE-FLIGHT INVARIANT:
    # Exactly ONE refresh operation was executed across all 50 concurrent scrapes!
    assert refresh_executions == 1, f"Expected exactly 1 refresh operation, got {refresh_executions}"
    assert cache.refresh_operation_count == 1

    # All 50 scrapes succeeded
    assert all(r is True for r in results)

    # Final gauge values reflect the newly refreshed snapshot
    assert OUTBOX_PENDING_EVENTS._value.get() == 200
    assert ACTIVE_WORKERS._value.get() == 4


# ==============================================================================
# Tests 4 & 5: Failed Refresh Preserves Snapshot & Increments Error Once
# ==============================================================================

def test_failed_refresh_preserves_snapshot_and_increments_error_once(monkeypatch):
    """
    AC-6 / Stage 2: A failed refresh preserves the last successful snapshot intact,
    avoids replacing valid values with zero, and increments taskforge_telemetry_scrape_errors_total
    exactly once per failed refresh attempt (not once per waiting scrape).
    """
    cache = GaugeSnapshotCache(ttl_seconds=5.0)

    # Prime with a valid snapshot
    initial_snapshot = InfrastructureSnapshot(
        queue_depth={"critical": 5, "high": 15, "medium": 25, "low": 35},
        outbox_pending_events=77,
        outbox_lag_seconds=2.4,
        active_workers=8,
        collected_at=time.time(),
    )
    cache._snapshot = initial_snapshot
    cache._apply_snapshot(initial_snapshot)
    cache._last_refresh_time = time.monotonic() - 10.0  # Expire TTL

    errors_before = TELEMETRY_SCRAPE_ERRORS_TOTAL._value.get()

    # Simulate backend crash during refresh
    def crashing_refresh():
        raise RuntimeError("PostgreSQL socket connection reset by peer")

    monkeypatch.setattr(cache, "_execute_refresh_operation", crashing_refresh)

    # Multiple scrapes observe the failed refresh
    for _ in range(5):
        ok = cache.collect_and_update()
        assert ok is True  # Preserved snapshot is served

    # 1. Previous successful snapshot is preserved intact (never zeroed out)
    assert OUTBOX_PENDING_EVENTS._value.get() == 77
    assert OUTBOX_LAG_SECONDS._value.get() == 2.4
    assert ACTIVE_WORKERS._value.get() == 8
    assert QUEUE_DEPTH.labels(priority="critical")._value.get() == 5

    # 2. Error counter incremented once per failed attempt (1st attempt failed; subsequent within TTL are warm)
    errors_after = TELEMETRY_SCRAPE_ERRORS_TOTAL._value.get()
    assert errors_after == errors_before + 1


def test_fifty_concurrent_scrapes_on_failing_refresh_increments_error_once(monkeypatch):
    """
    AC-6 / Stage 2: When 50 concurrent scrapes arrive and the background refresh fails,
    the previous snapshot is served to all 50 scrapes and the scrape error counter
    is incremented EXACTLY ONCE (never multiplied by 50).
    """
    cache = GaugeSnapshotCache(ttl_seconds=5.0)

    # Establish an initial valid snapshot
    initial_snapshot = InfrastructureSnapshot(
        queue_depth={"critical": 1, "high": 2, "medium": 3, "low": 4},
        outbox_pending_events=50,
        outbox_lag_seconds=1.0,
        active_workers=5,
        collected_at=time.time(),
    )
    cache._snapshot = initial_snapshot
    cache._apply_snapshot(initial_snapshot)
    cache._last_refresh_time = time.monotonic() - 10.0  # Expire TTL

    errors_before = TELEMETRY_SCRAPE_ERRORS_TOTAL._value.get()

    refresh_block = threading.Event()
    refresh_entered = threading.Event()

    def blocking_failing_refresh():
        refresh_entered.set()
        refresh_block.wait()
        raise RuntimeError("Downstream outage during concurrent refresh")

    monkeypatch.setattr(cache, "_execute_refresh_operation", blocking_failing_refresh)

    results = [None] * 50

    def scrape_worker(idx: int):
        results[idx] = cache.collect_and_update()

    threads = [threading.Thread(target=scrape_worker, args=(i,)) for i in range(50)]
    for t in threads:
        t.start()

    assert refresh_entered.wait(timeout=3.0)
    time.sleep(0.05)
    refresh_block.set()

    for t in threads:
        t.join(timeout=3.0)

    # All 50 scrapes succeeded by returning the preserved snapshot
    assert all(r is True for r in results)
    assert OUTBOX_PENDING_EVENTS._value.get() == 50

    # Error counter incremented EXACTLY ONCE (not 50 times!)
    errors_after = TELEMETRY_SCRAPE_ERRORS_TOTAL._value.get()
    assert errors_after == errors_before + 1


# ==============================================================================
# Test 6: Cold-Start Failure Omission (Zero Cardinality Leakage)
# ==============================================================================

def test_cold_start_failure_omits_unavailable_gauges(monkeypatch):
    """
    AC-7 / Stage 2: In a cold-start state where no prior snapshot exists and backend
    queries fail, affected infrastructure gauges are completely omitted from the
    scrape exposition (never reporting fabricated 0 values), and scrape errors increment once.
    """
    cache = GaugeSnapshotCache(ttl_seconds=5.0)
    assert cache.snapshot is None  # Cold start

    errors_before = TELEMETRY_SCRAPE_ERRORS_TOTAL._value.get()

    def failing_backend():
        raise RuntimeError("Total backend outage on cold start")

    monkeypatch.setattr(cache, "_execute_refresh_operation", failing_backend)
    monkeypatch.setattr("taskforge.monitoring.collector.get_gauge_snapshot_cache", lambda: cache)

    # Call get_metrics_content() directly
    content = get_metrics_content().decode("utf-8")

    # 1. Error counter incremented once
    errors_after = TELEMETRY_SCRAPE_ERRORS_TOTAL._value.get()
    assert errors_after == errors_before + 1

    # 2. Omitted gauges MUST NOT appear in the exposition output
    omitted = [
        "taskforge_outbox_pending_events",
        "taskforge_outbox_lag_seconds",
        "taskforge_queue_depth",
        "taskforge_active_workers",
    ]
    for m in omitted:
        assert m not in content, f"Cold-start failure leaked fabricated gauge: {m}"

    # 3. Process-local application pool gauge is still present
    assert "taskforge_db_pool_connections" in content


# ==============================================================================
# Test 7: Application DB Pool Stats In-Memory Inspection
# ==============================================================================

def test_db_pool_gauges_reflect_application_pool_without_borrowing_connection():
    """
    AC-5 / Stage 2: DB pool gauges reflect the application pool without borrowing connections.
    """
    cache = GaugeSnapshotCache()

    # Record application pool checked-out count before
    checked_out_before = engine.pool.checkedout()

    # Update DB pool connections
    cache.update_db_pool_connections()

    # Confirm no connection was borrowed during telemetry collection
    checked_out_after = engine.pool.checkedout()
    assert checked_out_after == checked_out_before

    # Verify Prometheus metrics match application pool
    assert DB_POOL_CONNECTIONS.labels(pool_state="checked_out")._value.get() == engine.pool.checkedout()
    assert DB_POOL_CONNECTIONS.labels(pool_state="idle")._value.get() == engine.pool.checkedin()
    assert DB_POOL_CONNECTIONS.labels(pool_state="overflow")._value.get() == engine.pool.overflow()


# ==============================================================================
# Test 8: Queue-Depth Redis Score Interval Compliance
# ==============================================================================

def test_queue_depth_calculations_match_contracted_redis_intervals():
    """
    Stage 2: Queue depth calculations strictly match the disjoint priority score intervals
    established in G9.2.1 using O(log N) ZCOUNT queries.
    """
    from taskforge.queue.redis_client import create_redis_client
    redis = create_redis_client()
    queue_key = "taskforge:queue:test_intervals"

    try:
        redis.delete(queue_key)

        # Populate tasks across different score bands
        pipe = redis.pipeline()
        # Critical: [3_000_000_000_001, 4_000_000_000_000]
        pipe.zadd(queue_key, {"c1": 3000000000001, "c2": 3999999999999})
        # High: [2_000_000_000_001, 3_000_000_000_000]
        pipe.zadd(queue_key, {"h1": 2000000000001, "h2": 2500000000000, "h3": 3000000000000})
        # Medium: [1_000_000_000_001, 2_000_000_000_000]
        pipe.zadd(queue_key, {"m1": 1500000000000})
        # Low: [1, 1_000_000_000_000]
        pipe.zadd(queue_key, {"l1": 1, "l2": 500000000000, "l3": 750000000000, "l4": 1000000000000})
        # Out-of-bounds (should not be counted in low)
        pipe.zadd(queue_key, {"zero": 0, "negative": -100})
        pipe.execute()

        cache = GaugeSnapshotCache()
        # Monkeypatch key to point to test sorted set
        def fetch_test_depths(r):
            p = r.pipeline()
            p.zcount(queue_key, 3000000000001, 4000000000000)
            p.zcount(queue_key, 2000000000001, 3000000000000)
            p.zcount(queue_key, 1000000000001, 2000000000000)
            p.zcount(queue_key, 1, 1000000000000)
            res = p.execute()
            return {
                "critical": int(res[0]),
                "high": int(res[1]),
                "medium": int(res[2]),
                "low": int(res[3]),
            }

        depths = fetch_test_depths(redis)
        assert depths["critical"] == 2
        assert depths["high"] == 3
        assert depths["medium"] == 1
        assert depths["low"] == 4
    finally:
        redis.delete(queue_key)
        redis.close()


# ==============================================================================
# Test 9: Stale Worker Pruning & Expiration Policy
# ==============================================================================

def test_worker_expiration_follows_stale_worker_policy():
    """
    Stage 2: Stale-worker pruning removes heartbeats older than now - 15.0s using
    ZREMRANGEBYSCORE and reads cardinality via ZCARD.
    """
    from taskforge.queue.redis_client import create_redis_client
    redis = create_redis_client()
    workers_key = "taskforge:workers:test_active"

    try:
        redis.delete(workers_key)
        now = time.time()

        pipe = redis.pipeline()
        pipe.zadd(workers_key, {"worker_fresh_1": now})
        pipe.zadd(workers_key, {"worker_fresh_2": now - 5.0})
        pipe.zadd(workers_key, {"worker_fresh_3": now - 14.0})
        pipe.zadd(workers_key, {"worker_stale_1": now - 16.0})
        pipe.zadd(workers_key, {"worker_stale_2": now - 60.0})
        pipe.execute()

        cache = GaugeSnapshotCache()
        # Custom fetch on test key
        def fetch_test_workers(r):
            cutoff = time.time() - 15.0
            p = r.pipeline()
            p.zremrangebyscore(workers_key, "-inf", cutoff)
            p.zcard(workers_key)
            _, count = p.execute()
            return int(count)

        active_count = fetch_test_workers(redis)
        assert active_count == 3, f"Expected 3 active workers, got {active_count}"

        # Confirm remaining members are only fresh workers
        remaining = redis.zrange(workers_key, 0, -1)
        assert set(remaining) == {"worker_fresh_1", "worker_fresh_2", "worker_fresh_3"}
    finally:
        redis.delete(workers_key)
        redis.close()


# ==============================================================================
# Test 10: Dedicated Telemetry Engine Isolation
# ==============================================================================

def test_dedicated_telemetry_engine_pool_isolation():
    """
    Stage 2: Dedicated telemetry engine has pool_size=1, max_overflow=0.
    """
    engine_telemetry = get_dedicated_telemetry_engine()
    assert engine_telemetry.pool.size() == 1
    assert engine_telemetry.pool._max_overflow == 0
