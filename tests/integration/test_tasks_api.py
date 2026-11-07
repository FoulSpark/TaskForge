import uuid
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from taskforge.api.schemas import TaskResponse
from taskforge.database.session import Base, get_db
from taskforge.main import app

# Test database setup (isolated in-memory SQLite with StaticPool so all connections share the in-memory DB)
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


def test_submit_single_task_and_retrieve():
    with TestClient(app) as client:
        payload = {
            "task_name": "Test Transformation",
            "task_type": "json_transform",
            "payload": {"records": [1, 2, 3]},
            "priority": 3,
            "timeout_seconds": 30,
            "max_retries": 2,
        }
        res = client.post("/api/v1/tasks", json=payload)
        assert res.status_code == 201
        data = res.json()
        task_id = data["id"]
        assert data["task_name"] == "Test Transformation"
        assert data["status"] == "QUEUED"
        assert data["priority"] == 3
        assert "X-Correlation-ID" in res.headers

        # Retrieve task
        get_res = client.get(f"/api/v1/tasks/{task_id}")
        assert get_res.status_code == 200
        get_data = get_res.json()
        assert get_data["id"] == task_id
        assert len(get_data["events"]) >= 1


def test_batch_submission_100_tasks_persisted():
    """Verify Acceptance Criterion P1-01 & P1-02: 100/100 valid submissions persisted and retrievable."""
    with TestClient(app) as client:
        task_ids = []
        for i in range(100):
            payload = {
                "task_name": f"Batch Task {i}",
                "task_type": "compute_prime",
                "payload": {"n": 1000 + i},
                "priority": (i % 4) + 1,
            }
            res = client.post("/api/v1/tasks", json=payload)
            assert res.status_code == 201
            task_ids.append(res.json()["id"])

        assert len(task_ids) == 100

        # Verify all 100 are retrievable
        for tid in task_ids:
            res = client.get(f"/api/v1/tasks/{tid}")
            assert res.status_code == 200
            assert res.json()["id"] == tid


def test_invalid_payload_validation():
    """Verify Acceptance Criterion P1-03: Invalid payloads rejected with 422."""
    with TestClient(app) as client:
        # Missing task_name
        res = client.post("/api/v1/tasks", json={"task_type": "json_transform"})
        assert res.status_code == 422
        assert res.json()["error"]["code"] == "VALIDATION_ERROR"

        # Invalid priority (out of range 1..4)
        res = client.post(
            "/api/v1/tasks",
            json={"task_name": "T", "task_type": "prime", "priority": 99},
        )
        assert res.status_code == 422

        # Invalid timeout (must be >= 1)
        res = client.post(
            "/api/v1/tasks",
            json={"task_name": "T", "task_type": "prime", "timeout_seconds": 0},
        )
        assert res.status_code == 422


def test_idempotent_task_submission():
    """Verify idempotency key prevents duplicate logical task creation."""
    with TestClient(app) as client:
        key = f"idemp-{uuid.uuid4()}"
        payload = {
            "task_name": "Idempotent Task",
            "task_type": "io_simulation",
            "payload": {"delay": 1.0},
            "idempotency_key": key,
        }

        # First submission -> 201 Created
        res1 = client.post("/api/v1/tasks", json=payload)
        assert res1.status_code == 201
        task_id_1 = res1.json()["id"]

        # Second submission with same key -> 200 OK, returns same task ID
        res2 = client.post("/api/v1/tasks", json=payload)
        assert res2.status_code == 200
        task_id_2 = res2.json()["id"]

        assert task_id_1 == task_id_2


def test_pagination_and_filtering():
    """Verify Acceptance Criterion P1-06: Stable pagination and filtering."""
    with TestClient(app) as client:
        # Create 15 tasks with varying priority
        for i in range(15):
            client.post(
                "/api/v1/tasks",
                json={
                    "task_name": f"Paging Task {i}",
                    "task_type": "db_process" if i % 2 == 0 else "json_transform",
                    "priority": 4 if i < 5 else 2,
                },
            )

        # Page 1: limit 5, offset 0
        p1 = client.get("/api/v1/tasks?limit=5&offset=0").json()
        assert len(p1["items"]) == 5
        assert p1["total"] == 15
        assert p1["has_more"] is True

        # Page 2: limit 5, offset 5
        p2 = client.get("/api/v1/tasks?limit=5&offset=5").json()
        assert len(p2["items"]) == 5
        assert p2["total"] == 15
        assert p2["has_more"] is True

        # Ensure no overlap between page 1 and page 2
        p1_ids = {t["id"] for t in p1["items"]}
        p2_ids = {t["id"] for t in p2["items"]}
        assert len(p1_ids.intersection(p2_ids)) == 0

        # Filter by task_type
        filtered = client.get("/api/v1/tasks?task_type=db_process").json()
        for t in filtered["items"]:
            assert t["task_type"] == "db_process"


def test_task_cancellation():
    with TestClient(app) as client:
        res = client.post(
            "/api/v1/tasks",
            json={"task_name": "To Cancel", "task_type": "long_running"},
        )
        task_id = res.json()["id"]

        cancel_res = client.post(f"/api/v1/tasks/{task_id}/cancel")
        assert cancel_res.status_code == 200
        assert cancel_res.json()["status"] == "CANCELLED"
