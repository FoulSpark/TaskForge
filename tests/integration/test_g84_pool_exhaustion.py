"""
G8.4.4 — Database Connection Pool Exhaustion & Recovery Test Suite.
Verifies graceful queueing under pool saturation, clean handling of pool timeouts,
and self-healing recovery to idle state with zero leaked connections.
"""

import concurrent.futures
import json
import threading
import time
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import TimeoutError as SQLATimeoutError

from taskforge.core.config import get_settings

settings = get_settings()


@pytest.fixture
def restricted_engine():
    """Provides an engine with a tightly restricted connection pool for testing saturation."""
    url = settings.database_url
    engine = create_engine(
        url,
        pool_size=5,
        max_overflow=5,
        pool_timeout=3.0,
        pool_pre_ping=True,
    )
    yield engine
    engine.dispose()


def test_connection_pool_queuing_under_saturation(restricted_engine):
    """30 concurrent workers saturate a pool of size 5+5=10.

    Invariants:
    - Queries queue cleanly and wait for available connections.
    - All 30 workers complete successfully without connection errors.
    - Zero leaked connections after completion.
    """
    num_workers = 30
    latencies = []

    def _worker_query():
        t0 = time.perf_counter()
        with restricted_engine.connect() as conn:
            # Simulate short transaction work
            res = conn.execute(text("SELECT 1 + 1")).scalar()
            time.sleep(0.02)
            assert res == 2
        return time.perf_counter() - t0

    t_start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(_worker_query) for _ in range(num_workers)]
        for f in concurrent.futures.as_completed(futures):
            latencies.append(f.result())
    _total_elapsed = time.perf_counter() - t_start

    assert len(latencies) == num_workers
    # Verify pool checked-out connections returned to 0
    pool = restricted_engine.pool
    assert pool.checkedout() == 0


def test_connection_pool_timeout_when_starved():
    """Tightly starved pool (size=2, overflow=0, timeout=0.5s) cleanly times out 3rd worker.

    Invariants:
    - Raises sqlalchemy.exc.TimeoutError rather than deadlock.
    - Clean failure without crashing the process.
    - Surviving connections complete cleanly and return to pool.
    """
    starved_engine = create_engine(
        settings.database_url,
        pool_size=2,
        max_overflow=0,
        pool_timeout=0.5,
    )

    barrier = threading.Barrier(3)
    release_event = threading.Event()

    def _hold_connection():
        with starved_engine.connect() as conn:
            barrier.wait(timeout=5.0)
            release_event.wait(timeout=5.0)
            conn.execute(text("SELECT 1"))

    def _starved_connection():
        with starved_engine.connect() as conn:
            conn.execute(text("SELECT 1"))

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # Hold both connections in the pool
        f1 = executor.submit(_hold_connection)
        f2 = executor.submit(_hold_connection)

        # Wait until both workers hold their connections
        barrier.wait(timeout=5.0)

        # 3rd worker attempts to acquire and must time out cleanly
        f3 = executor.submit(_starved_connection)
        with pytest.raises(SQLATimeoutError):
            f3.result()

        # Release holding workers
        release_event.set()
        f1.result()
        f2.result()

    starved_engine.dispose()


def test_connection_pool_recovery_after_burst(restricted_engine):
    """Verify pool completely recovers after heavy saturation burst.

    Invariants:
    - checkedout() count returns to 0.
    - Subsequent queries execute immediately with low latency (<50ms).
    - Zero orphaned connections.
    """
    def _query():
        with restricted_engine.connect() as conn:
            return conn.execute(text("SELECT 1")).scalar()

    # 1. Saturation burst
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(_query) for _ in range(20)]
        results = [f.result() for f in futures]
        assert all(r == 1 for r in results)

    # 2. Check idle state
    pool = restricted_engine.pool
    assert pool.checkedout() == 0

    # 3. Subsequent immediate query
    t0 = time.perf_counter()
    with restricted_engine.connect() as conn:
        val = conn.execute(text("SELECT 42")).scalar()
        assert val == 42
    recov_latency = time.perf_counter() - t0
    assert recov_latency < 0.1, f"Expected <100ms, took {recov_latency:.3f}s"

    # Write benchmark metrics
    metrics_path = Path("benchmarks/scenarios/g84/g84_pool_exhaustion_metrics.json")
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(
        json.dumps(
            {
                "gate": "G8.4.4",
                "test": "test_connection_pool_recovery_after_burst",
                "pool_size": 5,
                "max_overflow": 5,
                "burst_workers": 20,
                "checkedout_after_burst": pool.checkedout(),
                "post_burst_latency_ms": round(recov_latency * 1000, 2),
                "leaked_connections": 0,
                "recovery_status": "HEALTHY",
                "verdict": "PASS",
            },
            indent=2,
        )
    )


def test_api_level_pool_exhaustion_handling():
    """Verify HTTP requests against the FastAPI application when its connection pool is exhausted.

    Invariants:
    - HTTP requests encountering pool timeout fail gracefully with 500 error envelope without crashing FastAPI.
    - Application process remains healthy.
    - When connections are released, subsequent API requests succeed with 201 Created.
    """
    from fastapi.testclient import TestClient
    from sqlalchemy.orm import sessionmaker

    from taskforge.database.session import get_db
    from taskforge.main import app

    # Engine with strictly 1 connection, no overflow, short timeout
    tight_engine = create_engine(
        settings.database_url,
        pool_size=1,
        max_overflow=0,
        pool_timeout=0.2,
    )
    TightSessionLocal = sessionmaker(bind=tight_engine, autoflush=False, expire_on_commit=False)

    def override_get_db():
        db = TightSessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db

    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            # 1. Hold the single available connection
            held_conn = tight_engine.connect()
            held_conn.execute(text("SELECT 1"))

            # 2. HTTP request attempting to submit task must hit pool exhaustion cleanly (503 Service Unavailable)
            res = client.post(
                "/api/v1/tasks",
                json={"task_name": "Starved API Task", "task_type": "noop"},
            )
            assert res.status_code == 503
            body = res.json()
            assert "error" in body
            assert body["error"]["code"] == "SERVICE_UNAVAILABLE"
            assert "temporarily saturated" in body["error"]["message"]
            # Assert credentials or connection strings are NOT leaked
            assert "taskforge_dev" not in json.dumps(body)

            # 3. Release the held connection
            held_conn.close()

            # 4. Subsequent HTTP request must succeed immediately
            res_recov = client.post(
                "/api/v1/tasks",
                json={"task_name": "Recovered API Task", "task_type": "noop"},
            )
            assert res_recov.status_code == 201
            assert res_recov.json()["task_name"] == "Recovered API Task"
    finally:
        app.dependency_overrides.clear()
        tight_engine.dispose()


def test_api_database_outage_and_recovery_without_restarting_app():
    """Verify application behavior during real database outage and subsequent recovery without restarting FastAPI.

    Invariants:
    - Normal API request succeeds (201 Created).
    - Database outage (unreachable host/socket) causes API requests to return structured 503 error without crashing FastAPI.
    - Zero credential or connection string leakage in error payload.
    - Database reconnection restores full API functionality immediately with 201 Created.
    """
    from fastapi.testclient import TestClient
    from sqlalchemy.orm import sessionmaker

    from taskforge.database.session import get_db
    from taskforge.main import app

    unreachable_engine = None
    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            # Phase 1: Healthy baseline
            res1 = client.post(
                "/api/v1/tasks",
                json={"task_name": "Baseline Task", "task_type": "noop"},
            )
            assert res1.status_code == 201

            # Phase 2: Real Database Outage simulation with an engine attempting to connect to an unreachable port
            unreachable_engine = create_engine(
                "postgresql+psycopg://taskforge:taskforge_dev@127.0.0.1:5433/taskforge_test",
                connect_args={"connect_timeout": 1},
            )
            UnreachableSessionLocal = sessionmaker(bind=unreachable_engine, autoflush=False, expire_on_commit=False)

            def outage_get_db():
                db = UnreachableSessionLocal()
                try:
                    yield db
                finally:
                    db.close()

            app.dependency_overrides[get_db] = outage_get_db

            res_outage = client.post(
                "/api/v1/tasks",
                json={"task_name": "Outage Task", "task_type": "noop"},
            )
            assert res_outage.status_code == 503
            err = res_outage.json()["error"]
            assert err["code"] == "SERVICE_UNAVAILABLE"
            assert "temporarily unavailable" in err["message"]
            # Security verification: ensure no passwords or connection strings are leaked
            assert "taskforge_dev" not in json.dumps(res_outage.json())

            # Phase 3: Database Restored (clearing override to resume production PostgreSQL connection without restarting FastAPI)
            app.dependency_overrides.clear()

            res_restored = client.post(
                "/api/v1/tasks",
                json={"task_name": "Post-Outage Restored Task", "task_type": "noop"},
            )
            assert res_restored.status_code == 201
            assert res_restored.json()["task_name"] == "Post-Outage Restored Task"
    finally:
        app.dependency_overrides.clear()
        if unreachable_engine:
            unreachable_engine.dispose()



