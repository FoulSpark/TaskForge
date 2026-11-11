"""Backend contract, tenant isolation, and authorization tests for Phase 10 F1-A."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from prometheus_client import generate_latest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from taskforge.core.auth import ApiKeyService
from taskforge.core.state_machine import TaskPriority, TaskStatus
from taskforge.database.models import OutboxEvent, Task, TaskAttempt
from taskforge.database.session import Base, get_db
from taskforge.main import app

TEST_DATABASE_URL = "sqlite:///:memory:"
test_engine = create_engine(
    TEST_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)


class _FakeRedis:
    def __init__(self, healthy: bool = True):
        self.healthy = healthy

    def ping(self) -> bool:
        return self.healthy

    def close(self) -> None:
        pass


def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture(autouse=True)
def setup_test_env(monkeypatch):
    Base.metadata.create_all(bind=test_engine)
    app.dependency_overrides[get_db] = override_get_db
    monkeypatch.setenv("TASKFORGE_STRICT_AUTH", "1")
    monkeypatch.setattr("taskforge.main.check_database", lambda: True)
    monkeypatch.setattr("taskforge.main.check_redis", lambda _client=None: True)
    yield
    Base.metadata.drop_all(bind=test_engine)
    app.dependency_overrides.clear()


def _create_key(
    tenant_id: str,
    name: str,
    scopes: list[str],
    expires_at: datetime | None = None,
) -> tuple[str, str]:
    with TestingSessionLocal() as db:
        svc = ApiKeyService(db)
        plaintext, record = svc.create_api_key(
            tenant_id=tenant_id,
            name=name,
            scopes=scopes,
            expires_at=expires_at,
        )
        return plaintext, record.key_id


def _seed_completed_task(
    tenant_id: str,
    ingested_at: datetime,
    published_at: datetime,
    started_at: datetime,
    completed_at: datetime,
    handler_duration_ms: float,
) -> str:
    tid = str(uuid.uuid4())
    with TestingSessionLocal() as db:
        t = Task(
            id=tid,
            task_name=f"Task-{tenant_id}",
            task_type="noop",
            payload={},
            status=TaskStatus.SUCCESS.value,
            priority=TaskPriority.MEDIUM.value,
            tenant_id=tenant_id,
            created_at=ingested_at,
            updated_at=completed_at,
            started_at=started_at,
            completed_at=completed_at,
        )
        evt = OutboxEvent(
            id=str(uuid.uuid4()),
            aggregate_type="task",
            aggregate_id=tid,
            event_type="TASK_SUBMITTED",
            payload={"task_id": tid, "priority": 2},
            status="PUBLISHED",
            created_at=ingested_at,
            available_at=ingested_at,
            published_at=published_at,
        )
        att = TaskAttempt(
            task_id=tid,
            attempt_number=1,
            worker_id="worker-1",
            status="SUCCESS",
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=handler_duration_ms,
        )
        db.add_all([t, evt, att])
        db.commit()
    return tid


def test_auth_me_and_key_lifecycle():
    """Verify /api/v1/auth/me handles missing, invalid, expired, revoked, and valid API keys."""
    valid_key, key_id = _create_key("tenant-a", "Tenant A Key", ["tasks:read", "tasks:write"])
    expired_key, _ = _create_key(
        "tenant-a",
        "Expired Key",
        ["tasks:read"],
        expires_at=datetime.now(UTC) - timedelta(minutes=5),
    )

    with TestClient(app) as client:
        app.state.redis = _FakeRedis()

        assert client.get("/api/v1/auth/me").status_code == 401
        assert (
            client.get(
                "/api/v1/auth/me",
                headers={"Authorization": "Bearer tf_live_bad_key"},
            ).status_code
            == 401
        )
        assert (
            client.get(
                "/api/v1/auth/me",
                headers={"Authorization": f"Bearer {expired_key}"},
            ).status_code
            == 401
        )

        res = client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {valid_key}"})
        assert res.status_code == 200
        body = res.json()
        assert body == {
            "key_id": key_id,
            "tenant_id": "tenant-a",
            "name": "Tenant A Key",
            "scopes": ["tasks:read", "tasks:write"],
        }
        assert valid_key not in res.text

        with TestingSessionLocal() as db:
            assert ApiKeyService(db).revoke_api_key(key_id) is True

        assert (
            client.get(
                "/api/v1/auth/me",
                headers={"Authorization": f"Bearer {valid_key}"},
            ).status_code
            == 401
        )


def test_stats_and_operator_scope_isolation():
    """Prove /api/v1/stats is strictly tenant-scoped and operator endpoints reject ordinary users."""
    now = datetime.now(UTC)
    for _ in range(2):
        _seed_completed_task("tenant-a", now - timedelta(seconds=10), now - timedelta(seconds=9), now - timedelta(seconds=6), now - timedelta(seconds=5), 200.0)
    for _ in range(3):
        _seed_completed_task("tenant-b", now - timedelta(seconds=10), now - timedelta(seconds=9), now - timedelta(seconds=6), now - timedelta(seconds=5), 900.0)

    key_a, _ = _create_key("tenant-a", "Reader A", ["tasks:read"])
    key_op, _ = _create_key("tenant-op", "Admin Op", ["tasks:read", "admin:operator"])

    with TestClient(app) as client:
        app.state.redis = _FakeRedis()

        # Unauthenticated requests rejected
        assert client.get("/api/v1/stats").status_code == 401
        assert client.get("/api/v1/workers").status_code == 401
        assert client.get("/api/v1/operator/system-stats").status_code == 401

        # Tenant A receives only Tenant A stats even if passing tenant_id=tenant-b
        res_a = client.get(
            "/api/v1/stats?tenant_id=tenant-b",
            headers={"Authorization": f"Bearer {key_a}"},
        )
        assert res_a.status_code == 200
        data_a = res_a.json()
        assert data_a["tenant_id"] == "tenant-a"
        assert data_a["scope"] == "tenant"
        assert data_a["tasks"]["total"] == 2
        assert data_a["tasks"]["succeeded"] == 2
        assert data_a["performance"]["avg_execution_ms"] == 200.0
        assert data_a["performance"]["queue_depth"] is None
        assert data_a["workers"] is None

        # Tenant A rejected from operator-only endpoints
        assert (
            client.get(
                "/api/v1/operator/system-stats",
                headers={"Authorization": f"Bearer {key_a}"},
            ).status_code
            == 403
        )
        assert (
            client.get(
                "/api/v1/workers",
                headers={"Authorization": f"Bearer {key_a}"},
            ).status_code
            == 403
        )

        # Operator sees global counts (2 + 3 = 5) on /operator/system-stats
        res_op = client.get(
            "/api/v1/operator/system-stats",
            headers={"Authorization": f"Bearer {key_op}"},
        )
        assert res_op.status_code == 200
        data_op = res_op.json()
        assert data_op["scope"] == "system"
        assert data_op["tasks"]["total"] == 5
        assert data_op["tasks"]["succeeded"] == 5
        assert "queue_depth_by_priority" in data_op["performance"]
        assert "active_count" in data_op["workers"]

        assert (
            client.get(
                "/api/v1/workers",
                headers={"Authorization": f"Bearer {key_op}"},
            ).status_code
            == 200
        )


def test_monitoring_summary_r1_lifecycle_separation_and_scope():
    """Verify exact non-conflated lifecycle metrics, population filtering, empty null behavior, and R2 scope."""
    now = datetime.now(UTC)
    # Tenant A completed task with exact causal timestamps:
    # outbox_dwell = 1000ms, queue_dwell = 3000ms, submission_to_start = 4000ms,
    # handler_duration = 250ms, worker_lease_tenure = 500ms, end_to_end = 4500ms
    _seed_completed_task(
        tenant_id="tenant-a",
        ingested_at=now - timedelta(seconds=10),
        published_at=now - timedelta(seconds=9),
        started_at=now - timedelta(seconds=6),
        completed_at=now - timedelta(milliseconds=5500),
        handler_duration_ms=250.0,
    )
    # Seed an active RUNNING task for Tenant A — must NOT pollute completed percentiles
    with TestingSessionLocal() as db:
        running_tid = str(uuid.uuid4())
        db.add(
            Task(
                id=running_tid,
                task_name="Running Task",
                task_type="noop",
                payload={},
                status=TaskStatus.RUNNING.value,
                priority=TaskPriority.MEDIUM.value,
                tenant_id="tenant-a",
                created_at=now - timedelta(seconds=120),
                started_at=now - timedelta(seconds=1),
                completed_at=None,
            )
        )
        db.add(
            TaskAttempt(
                task_id=running_tid,
                attempt_number=1,
                worker_id="worker-1",
                status="RUNNING",
                started_at=now - timedelta(seconds=1),
                completed_at=None,
                duration_ms=None,
            )
        )
        db.commit()

    # Tenant B task must not bleed into Tenant A monitoring
    _seed_completed_task(
        tenant_id="tenant-b",
        ingested_at=now - timedelta(seconds=20),
        published_at=now - timedelta(seconds=15),
        started_at=now - timedelta(seconds=10),
        completed_at=now - timedelta(seconds=5),
        handler_duration_ms=1200.0,
    )

    key_a, _ = _create_key("tenant-a", "Tenant A", ["tasks:read"])
    key_empty, _ = _create_key("tenant-empty", "Empty Tenant", ["tasks:read"])
    key_op, _ = _create_key("tenant-a", "Op A", ["tasks:read", "admin:operator"])

    with TestClient(app) as client:
        app.state.redis = _FakeRedis()

        res_a = client.get(
            "/api/v1/monitoring/summary?window_minutes=5&tenant_id=tenant-b",
            headers={"Authorization": f"Bearer {key_a}"},
        )
        assert res_a.status_code == 200
        body = res_a.json()
        assert body["tenant_id"] == "tenant-a"
        assert body["window_minutes"] == 5
        assert body["window_seconds"] == 300
        assert body["cluster"] is None
        assert body["health"] == {
            "overall": "healthy",
            "database": "healthy",
            "redis": "healthy",
        }
        assert body["throughput"]["observation_count"] == 1

        lats = body["latencies_ms"]
        assert lats["outbox_dwell"]["p50_ms"] == 1000.0
        assert lats["queue_dwell"]["p50_ms"] == 3000.0
        assert lats["submission_to_start"]["p50_ms"] == 4000.0
        assert lats["handler_duration"]["p50_ms"] == 250.0
        assert lats["worker_lease_tenure"]["p50_ms"] == 500.0
        assert lats["end_to_end"]["p50_ms"] == 4500.0
        assert lats["end_to_end"]["sample_count"] == 1
        assert lats["end_to_end"]["sufficient_sample_size"] is False
        assert lats["end_to_end"]["minimum_recommended_samples"] == 20

        # Empty tenant must return null (not 0.0)
        res_empty = client.get(
            "/api/v1/monitoring/summary",
            headers={"Authorization": f"Bearer {key_empty}"},
        )
        assert res_empty.status_code == 200
        empty_lats = res_empty.json()["latencies_ms"]
        for metric_name in (
            "outbox_dwell",
            "queue_dwell",
            "submission_to_start",
            "handler_duration",
            "worker_lease_tenure",
            "end_to_end",
        ):
            assert empty_lats[metric_name]["sample_count"] == 0
            assert empty_lats[metric_name]["p50_ms"] is None
            assert empty_lats[metric_name]["p95_ms"] is None
            assert empty_lats[metric_name]["p99_ms"] is None

        # Operator receives cluster block
        res_op = client.get(
            "/api/v1/monitoring/summary",
            headers={"Authorization": f"Bearer {key_op}"},
        )
        assert res_op.status_code == 200
        cluster = res_op.json()["cluster"]
        assert cluster is not None
        assert "queue_depth_by_priority" in cluster
        assert "outbox_pending_events" in cluster
        assert "workers" in cluster
        assert "db_pool" in cluster


def test_task_types_catalog_and_zero_prometheus_cardinality_delta(monkeypatch):
    """Verify /api/v1/task-types exposes schemas and unavailable distinction without adding Prometheus series."""
    key_a, _ = _create_key("tenant-a", "Reader A", ["tasks:read"])

    before_series = len(
        [
            line
            for line in generate_latest().decode("utf-8").splitlines()
            if line and not line.startswith("#")
        ]
    )

    with TestClient(app) as client:
        app.state.redis = _FakeRedis()

        assert client.get("/api/v1/task-types").status_code == 401

        res = client.get("/api/v1/task-types", headers={"Authorization": f"Bearer {key_a}"})
        assert res.status_code == 200
        data = res.json()
        items_by_type = {i["task_type"]: i for i in data["items"]}
        assert "json_transform" in items_by_type
        assert "compute_prime" in items_by_type
        assert items_by_type["json_transform"]["available"] is True
        assert "payload_schema" in items_by_type["compute_prime"]
        assert "default_payload" in items_by_type["compute_prime"]
        assert data["submission_constraints"]["priority"]["max"] == 4

        # Simulate unregistered handler -> available=False rather than omitting or raising
        monkeypatch.setattr(
            "taskforge.tasks.catalog.list_registered_handlers",
            lambda: ["json_transform"],
        )
        res_unavail = client.get(
            "/api/v1/task-types",
            headers={"Authorization": f"Bearer {key_a}"},
        )
        assert res_unavail.status_code == 200
        unavail_map = {i["task_type"]: i for i in res_unavail.json()["items"]}
        assert unavail_map["json_transform"]["available"] is True
        assert unavail_map["compute_prime"]["available"] is False

    after_series = len(
        [
            line
            for line in generate_latest().decode("utf-8").splitlines()
            if line and not line.startswith("#")
        ]
    )
    assert after_series == before_series
