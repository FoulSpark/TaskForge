from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
import pytest

from taskforge.database.session import Base, get_db
from taskforge.main import app

TEST_DATABASE_URL = "sqlite:///:memory:"
test_engine = create_engine(
    TEST_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)


def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture(autouse=True)
def setup_test_db(monkeypatch):
    Base.metadata.create_all(bind=test_engine)
    app.dependency_overrides[get_db] = override_get_db
    monkeypatch.setattr("taskforge.main.check_database", lambda: True)
    yield
    Base.metadata.drop_all(bind=test_engine)
    app.dependency_overrides.clear()


def test_metrics_endpoint():
    with TestClient(app) as client:
        res = client.get("/metrics")
        assert res.status_code == 200
        text = res.text
        assert "taskforge_tasks_submitted_total" in text
        assert "taskforge_tasks_completed_total" in text
        assert "taskforge_active_workers" in text


def test_stats_endpoint():
    with TestClient(app) as client:
        res = client.get("/api/v1/stats")
        assert res.status_code == 200
        data = res.json()
        assert "tasks" in data
        assert "performance" in data
        assert "workers" in data
        assert "total" in data["tasks"]


def test_chaos_batch_and_idempotency_storm_endpoints():
    with TestClient(app) as client:
        # Submit batch of 5
        b_res = client.post("/api/v1/chaos/batch?count=5")
        assert b_res.status_code == 200
        assert b_res.json()["batch_size"] == 5
        assert len(b_res.json()["task_ids"]) == 5

        # Idempotency storm of 20 requests with same key
        storm_res = client.post("/api/v1/chaos/idempotency-storm?count=20")
        assert storm_res.status_code == 200
        storm_data = storm_res.json()
        assert storm_data["tasks_created"] == 1
        assert storm_data["duplicate_requests_absorbed"] == 19
        assert storm_data["verified"] is True
