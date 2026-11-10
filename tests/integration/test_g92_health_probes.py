import asyncio
import concurrent.futures
from datetime import datetime
import threading
from typing import Any
import pytest
from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy.exc import OperationalError

from taskforge.main import app
from taskforge.monitoring.health import (
    ReadinessProbeCoordinator,
    ReadinessProbeSaturationError,
    ReadinessProbeShuttingDownError,
    perform_readiness_checks,
)


class FakeRedisWithException:
    def __init__(self, exc: Exception):
        self._exc = exc

    def ping(self):
        raise self._exc

    def close(self):
        pass


# ==============================================================================
# Category 1: Liveness Independence & Dual Route Aliases (Mocked Outages)
# ==============================================================================

def test_liveness_both_aliases_independent_of_dependencies(monkeypatch):
    """
    AC-1 / Stage 1: Verify that both /health/live and /healthz/live return HTTP 200
    without executing database or Redis queries, even during total downstream outages.
    """
    # Force total dependency failure
    monkeypatch.setattr(
        "taskforge.main.check_database",
        lambda: (_ for _ in ()).throw(RuntimeError("FATAL: database completely down")),
    )
    monkeypatch.setattr(
        "taskforge.main.check_redis",
        lambda client=None: (_ for _ in ()).throw(RuntimeError("FATAL: redis completely down")),
    )

    with TestClient(app) as client:
        # 1. Primary alias: /health/live
        resp_live = client.get("/health/live")
        assert resp_live.status_code == 200
        data_live = resp_live.json()
        assert data_live["status"] == "alive"
        assert data_live["application"] == "TaskForge"

        # 2. Secondary alias: /healthz/live
        resp_healthz = client.get("/healthz/live")
        assert resp_healthz.status_code == 200
        data_healthz = resp_healthz.json()
        assert data_healthz["status"] == "alive"
        assert data_healthz["application"] == "TaskForge"


# ==============================================================================
# Category 2: Real Backing Store Readiness (PostgreSQL + Redis Integration)
# ==============================================================================

def test_readiness_both_aliases_success_against_real_services():
    """
    Stage 1: Verify that both /health/ready and /healthz/ready return HTTP 200
    against the actual running PostgreSQL and Redis containers.
    """
    with TestClient(app) as client:
        # 1. Primary alias: /health/ready
        resp_ready = client.get("/health/ready")
        assert resp_ready.status_code == 200
        data_ready = resp_ready.json()
        assert data_ready["status"] == "ready"
        assert data_ready["application"] == "TaskForge"
        assert data_ready["checks"]["database"] == "healthy"
        assert data_ready["checks"]["redis"] == "healthy"

        # 2. Secondary alias: /healthz/ready
        resp_healthz = client.get("/healthz/ready")
        assert resp_healthz.status_code == 200
        data_healthz = resp_healthz.json()
        assert data_healthz["status"] == "ready"
        assert data_healthz["application"] == "TaskForge"
        assert data_healthz["checks"]["database"] == "healthy"
        assert data_healthz["checks"]["redis"] == "healthy"


# ==============================================================================
# Category 3: Deterministic Concurrency Tests (threading.Event Synchronization)
# ==============================================================================

def test_readiness_executor_saturation_fast_fails_third_request(monkeypatch):
    """
    AC-2 / Stage 1: Verify that the dedicated readiness executor strictly enforces
    0 <= inflight probe futures <= 2. When two probe operations are actively executing,
    a 3rd concurrent request immediately fast-fails with HTTP 503 without submitting.
    """
    probe_block = threading.Event()
    entered_count = 0
    entered_lock = threading.Lock()
    two_entered = threading.Event()

    def blocking_probe(client=None):
        nonlocal entered_count
        with entered_lock:
            entered_count += 1
            if entered_count == 2:
                two_entered.set()
        probe_block.wait()
        return True, {"database": "healthy", "redis": "healthy"}

    with TestClient(app) as client:
        coordinator: ReadinessProbeCoordinator = client.app.state.readiness_coordinator
        assert coordinator.inflight_count == 0

        # Monkeypatch perform_readiness_checks to block inside the worker threads
        monkeypatch.setattr("taskforge.main.perform_readiness_checks", blocking_probe)

        # Launch 2 background requests to fill both execution slots
        results = [None, None]

        def client_request(index):
            results[index] = client.get("/health/ready")

        t1 = threading.Thread(target=client_request, args=(0,))
        t2 = threading.Thread(target=client_request, args=(1,))
        t1.start()
        t2.start()

        # Wait deterministically until both futures are executing in threads
        assert two_entered.wait(timeout=3.0), "Timed out waiting for two probe threads to enter"
        assert coordinator.inflight_count == 2, f"Expected 2 inflight futures, got {coordinator.inflight_count}"

        # 3rd concurrent request arrives while slots are saturated
        resp_saturated = client.get("/health/ready")
        assert resp_saturated.status_code == 503
        data_sat = resp_saturated.json()
        assert data_sat["status"] == "not_ready"
        assert data_sat["application"] == "TaskForge"
        assert data_sat["checks"]["database"] == "unavailable"
        assert data_sat["checks"]["redis"] == "unavailable"
        assert "timestamp" in data_sat

        # Inflight count MUST still be 2 (3rd request was rejected without taking a slot)
        assert coordinator.inflight_count == 2

        # Unblock running probes
        probe_block.set()
        t1.join(timeout=3.0)
        t2.join(timeout=3.0)

        assert results[0].status_code == 200
        assert results[1].status_code == 200

        # After completion, all slots are freed
        assert coordinator.inflight_count == 0


def test_readiness_cancellation_preserves_slot_until_future_completion():
    """
    AC-3 / Stage 1: Verify that cancelling the awaiting readiness coroutine does NOT
    release the concurrency slot; the slot remains occupied until the underlying
    worker thread's future actually completes.
    """
    probe_block = threading.Event()
    thread_entered = threading.Event()

    def slow_blocking_worker():
        thread_entered.set()
        probe_block.wait()
        return True, {"database": "healthy"}

    coordinator = ReadinessProbeCoordinator(max_workers=2)
    try:
        assert coordinator.inflight_count == 0

        # Submit probe to coordinator
        future = coordinator.submit_probe(slow_blocking_worker)
        assert coordinator.inflight_count == 1

        # Wait deterministically until worker thread enters
        assert thread_entered.wait(timeout=3.0)

        # Coroutine awaits the future with a short timeout and gets cancelled
        async def cancel_probe():
            asyncio_future = asyncio.wrap_future(future)
            # Cancel coroutine explicitly
            asyncio_future.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio_future

        asyncio.run(cancel_probe())

        # CRITICAL CONTRACT ASSERTION:
        # Coroutine is cancelled, BUT the underlying thread is still blocked on probe_block.
        # Inflight slot MUST remain occupied!
        assert not future.done()
        assert coordinator.inflight_count == 1, (
            f"Permit was prematurely released! Expected inflight=1, got {coordinator.inflight_count}"
        )

        # Release the worker thread
        probe_block.set()

        # Wait for the future to finish
        future.result(timeout=3.0)
        assert future.done()

        # NOW the slot must be released via future.add_done_callback
        assert coordinator.inflight_count == 0
    finally:
        probe_block.set()
        coordinator.shutdown(wait=True)


def test_readiness_shutdown_race_fast_fails_without_exception():
    """
    AC-11 / Stage 1: Trigger application shutdown while readiness requests arrive.
    Verify that no new probe is submitted after shutdown begins (fast-failing with 503),
    preventing RuntimeError: cannot schedule new futures after shutdown, and verify
    that already-running probe futures retain their slots until completion.
    """
    probe_block = threading.Event()
    thread_entered = threading.Event()

    def running_probe():
        thread_entered.set()
        probe_block.wait()
        return True, {"database": "healthy", "redis": "healthy"}

    with TestClient(app) as client:
        coordinator: ReadinessProbeCoordinator = client.app.state.readiness_coordinator

        # Submit one running probe before shutdown
        future = coordinator.submit_probe(running_probe)
        assert thread_entered.wait(timeout=3.0)
        assert coordinator.inflight_count == 1

        # Initiate application shutdown
        coordinator.shutdown(wait=False)
        assert coordinator.is_shutting_down is True

        # Now issue readiness requests through the API endpoint
        # They MUST fast-fail with HTTP 503 without raising unhandled RuntimeError
        resp1 = client.get("/health/ready")
        assert resp1.status_code == 503
        data1 = resp1.json()
        assert data1["status"] == "not_ready"

        resp2 = client.get("/healthz/ready")
        assert resp2.status_code == 503
        data2 = resp2.json()
        assert data2["status"] == "not_ready"

        # The running future from before shutdown is still occupying its slot
        assert coordinator.inflight_count == 1

        # Unblock running future
        probe_block.set()
        future.result(timeout=3.0)

        # Slot decrements to 0
        assert coordinator.inflight_count == 0

        # Further submissions still fast-fail cleanly
        with pytest.raises(ReadinessProbeShuttingDownError):
            coordinator.submit_probe(lambda: (True, {}))


# ==============================================================================
# Category 4: Sanitization & Zero Credential Leakage
# ==============================================================================

def test_readiness_sanitized_problem_details_schema_and_zero_leakage(monkeypatch):
    """
    AC-4 / Stage 1: Readiness failure emits HTTP 503 with standardized schema
    and asserts total absence of connection strings, hostnames, ports, credentials,
    and Python exception tracebacks.
    """
    sensitive_db_url = "postgresql+psycopg://taskforge_admin:super_secret_pw@192.168.1.100:5432/taskforge_secret_db"
    sensitive_redis_url = "redis://:super_redis_auth@10.0.0.50:6379/0"

    # Inject mock failures containing sensitive network details and credentials
    def failing_db():
        raise OperationalError(
            f"Connection refused to {sensitive_db_url}",
            params={},
            orig=Exception("FATAL: password authentication failed for user 'taskforge_admin'"),
        )

    def failing_redis(client=None):
        raise RedisConnectionError(f"Error 111 connecting to {sensitive_redis_url}. Connection refused.")

    monkeypatch.setattr("taskforge.main.check_database", failing_db)
    monkeypatch.setattr("taskforge.main.check_redis", failing_redis)

    with TestClient(app) as client:
        resp = client.get("/health/ready")
        assert resp.status_code == 503
        raw_text = resp.text
        data = resp.json()

        # 1. Assert Schema Conformance
        assert data["status"] == "not_ready"
        assert data["application"] == "TaskForge"
        assert data["checks"] == {
            "database": "unavailable",
            "redis": "unavailable",
        }
        assert "timestamp" in data
        # Validate timestamp is ISO 8601
        parsed_ts = datetime.fromisoformat(data["timestamp"])
        assert parsed_ts is not None

        # 2. Assert Zero Information Leakage
        forbidden_strings = [
            "super_secret_pw",
            "super_redis_auth",
            "taskforge_admin",
            "192.168.1.100",
            "10.0.0.50",
            "5432",
            "6379",
            "taskforge_secret_db",
            "postgresql",
            "Traceback",
            "OperationalError",
            "RedisConnectionError",
            'File "',
            "line ",
        ]
        for forbidden in forbidden_strings:
            assert forbidden not in raw_text, (
                f"LEAK DETECTED: Sensitive token '{forbidden}' leaked in readiness response: {raw_text}"
            )


# ==============================================================================
# Category 5: Sequential Execution Order & Driver Deadlines
# ==============================================================================

def test_readiness_sequential_execution_order(monkeypatch):
    """
    Stage 1: Verify PostgreSQL check is executed sequentially BEFORE Redis check.
    """
    call_order = []

    def tracking_db():
        call_order.append("database")
        return True

    def tracking_redis(client=None):
        call_order.append("redis")
        return True

    monkeypatch.setattr("taskforge.main.check_database", tracking_db)
    monkeypatch.setattr("taskforge.main.check_redis", tracking_redis)

    with TestClient(app) as client:
        resp = client.get("/health/ready")
        assert resp.status_code == 200
        assert call_order == ["database", "redis"]


def test_readiness_caller_timeout_returns_503_and_retains_slot(monkeypatch):
    """
    Stage 1: Verify that when underlying probe execution exceeds the 1.0s coroutine
    deadline, the caller receives HTTP 503, while the underlying future continues
    occupying its execution slot until completion.
    """
    probe_block = threading.Event()
    thread_entered = threading.Event()

    def slow_probe(client=None):
        thread_entered.set()
        probe_block.wait()
        return True, {"database": "healthy", "redis": "healthy"}

    monkeypatch.setattr("taskforge.main.perform_readiness_checks", slow_probe)

    with TestClient(app) as client:
        coordinator: ReadinessProbeCoordinator = client.app.state.readiness_coordinator

        result = [None]

        def client_call():
            result[0] = client.get("/health/ready")

        t = threading.Thread(target=client_call)
        t.start()

        # Wait until probe thread is executing
        assert thread_entered.wait(timeout=3.0)
        assert coordinator.inflight_count == 1

        # The request times out at 1.0s and returns 503 to the caller
        t.join(timeout=3.0)
        assert result[0] is not None
        assert result[0].status_code == 503
        assert result[0].json()["status"] == "not_ready"

        # Underlying thread is STILL BLOCKED on probe_block; slot remains occupied
        assert coordinator.inflight_count == 1

        # Release thread
        probe_block.set()

        # Give completion callback a moment to run
        for _ in range(50):
            if coordinator.inflight_count == 0:
                break
            threading.Event().wait(0.02)

        assert coordinator.inflight_count == 0

