"""
G8.4.1 — API Contract & Edge-Case Resilience Test Suite.
Verifies exhaustive API contract compliance, structured error formats, boundary limits,
pagination, filtering, and lifecycle state transition guards (404, 409, 422).
"""

import uuid

import pytest
from fastapi.testclient import TestClient

from taskforge.core.state_machine import TaskStatus
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal
from taskforge.main import app


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client


def test_g84_api_submission_success(client: TestClient):
    """Test valid task submission returns HTTP 201 with complete response schema."""
    payload = {
        "task_name": "G8.4 Contract Task",
        "task_type": "noop",
        "payload": {"param": "value"},
        "priority": 3,
        "timeout_seconds": 60,
        "max_retries": 5,
    }
    res = client.post("/api/v1/tasks", json=payload)
    assert res.status_code == 201
    body = res.json()
    assert body["task_name"] == "G8.4 Contract Task"
    assert body["task_type"] == "noop"
    assert body["status"] == "QUEUED"
    assert body["priority"] == 3
    assert body["timeout_seconds"] == 60
    assert body["max_retries"] == 5
    assert "id" in body
    assert "created_at" in body


def test_g84_api_validation_errors(client: TestClient):
    """Test malformed request payloads return HTTP 422 with structured API error details."""
    # 1. Missing required field 'task_name'
    res = client.post("/api/v1/tasks", json={"task_type": "noop"})
    assert res.status_code == 422
    assert res.json()["error"]["code"] == "VALIDATION_ERROR"

    # 2. Priority below minimum (priority = 0, must be >= 1)
    res = client.post(
        "/api/v1/tasks",
        json={"task_name": "Bad Priority", "task_type": "noop", "priority": 0},
    )
    assert res.status_code == 422
    assert res.json()["error"]["code"] == "VALIDATION_ERROR"

    # 3. Priority above maximum (priority = 5, must be <= 4)
    res = client.post(
        "/api/v1/tasks",
        json={"task_name": "Bad Priority", "task_type": "noop", "priority": 5},
    )
    assert res.status_code == 422
    assert res.json()["error"]["code"] == "VALIDATION_ERROR"

    # 4. Negative timeout_seconds (must be > 0)
    res = client.post(
        "/api/v1/tasks",
        json={"task_name": "Bad Timeout", "task_type": "noop", "timeout_seconds": -5},
    )
    assert res.status_code == 422

    # 5. Negative max_retries (must be >= 0)
    res = client.post(
        "/api/v1/tasks",
        json={"task_name": "Bad Retries", "task_type": "noop", "max_retries": -1},
    )
    assert res.status_code == 422

    # 6. Malformed JSON / raw non-JSON body
    res = client.post(
        "/api/v1/tasks",
        content="THIS IS NOT VALID JSON",
        headers={"Content-Type": "application/json"},
    )
    assert res.status_code == 422


def test_g84_api_task_retrieval_and_not_found(client: TestClient):
    """Test retrieving existing task returns 200, non-existent returns 404."""
    # Create task
    create_res = client.post(
        "/api/v1/tasks",
        json={"task_name": "Retrieval Task", "task_type": "noop"},
    )
    task_id = create_res.json()["id"]

    # Retrieve existing
    get_res = client.get(f"/api/v1/tasks/{task_id}")
    assert get_res.status_code == 200
    assert get_res.json()["id"] == task_id

    # Retrieve non-existent UUID
    fake_id = str(uuid.uuid4())
    fake_res = client.get(f"/api/v1/tasks/{fake_id}")
    assert fake_res.status_code == 404
    assert fake_res.json()["error"]["code"] == "HTTP_404"


def test_g84_api_pagination_and_filtering(client: TestClient):
    """Test pagination limits, offset boundaries, and query filters."""
    # Create 25 tasks for pagination testing
    batch_prefix = f"page-test-{uuid.uuid4().hex[:6]}"
    for i in range(25):
        client.post(
            "/api/v1/tasks",
            json={
                "task_name": f"{batch_prefix}-{i}",
                "task_type": "db_process" if i % 2 == 0 else "json_transform",
                "priority": (i % 4) + 1,
            },
        )

    # Page 1: limit 10, offset 0
    p1 = client.get("/api/v1/tasks?limit=10&offset=0").json()
    assert len(p1["items"]) == 10
    assert p1["total"] >= 25
    assert p1["has_more"] is True

    # Page 2: limit 10, offset 10
    p2 = client.get("/api/v1/tasks?limit=10&offset=10").json()
    assert len(p2["items"]) == 10
    assert p2["total"] >= 25

    # Verify no overlapping items between Page 1 and Page 2
    p1_ids = {item["id"] for item in p1["items"]}
    p2_ids = {item["id"] for item in p2["items"]}
    assert len(p1_ids.intersection(p2_ids)) == 0

    # Large offset -> empty items list
    p_empty = client.get("/api/v1/tasks?limit=10&offset=100000").json()
    assert len(p_empty["items"]) == 0
    assert p_empty["has_more"] is False

    # Validation: limit out of bounds (limit=0 or limit=101)
    res_bad_limit = client.get("/api/v1/tasks?limit=0")
    assert res_bad_limit.status_code == 422
    res_max_limit = client.get("/api/v1/tasks?limit=101")
    assert res_max_limit.status_code == 422

    # Query Filter: task_type
    filtered = client.get("/api/v1/tasks?task_type=db_process&limit=10").json()
    assert len(filtered["items"]) > 0
    for item in filtered["items"]:
        assert item["task_type"] == "db_process"

    # Query Filter: priority
    filtered_p = client.get("/api/v1/tasks?priority=4&limit=10").json()
    assert len(filtered_p["items"]) > 0
    for item in filtered_p["items"]:
        assert item["priority"] == 4


def test_g84_api_cancellation_workflow(client: TestClient):
    """Test cancelling queued, terminal, and non-existent tasks."""
    # 1. Cancel QUEUED task -> 200 OK
    res = client.post("/api/v1/tasks", json={"task_name": "Cancel Me", "task_type": "noop"})
    task_id = res.json()["id"]

    cancel_res = client.post(f"/api/v1/tasks/{task_id}/cancel")
    assert cancel_res.status_code == 200
    assert cancel_res.json()["status"] == "CANCELLED"

    # 2. Cancel already CANCELLED task -> 409 Conflict
    cancel_again = client.post(f"/api/v1/tasks/{task_id}/cancel")
    assert cancel_again.status_code == 409

    # 3. Cancel non-existent task -> 404 Not Found
    fake_id = str(uuid.uuid4())
    fake_cancel = client.post(f"/api/v1/tasks/{fake_id}/cancel")
    assert fake_cancel.status_code == 404

    # 4. Cancel SUCCESS task -> 409 Conflict
    # Transition a task directly to SUCCESS in DB
    succ_res = client.post("/api/v1/tasks", json={"task_name": "Success Task", "task_type": "noop"})
    succ_id = succ_res.json()["id"]
    with SessionLocal() as db:
        repo = TaskRepository(db)
        repo.update_status(succ_id, TaskStatus.RUNNING)
        repo.update_status(succ_id, TaskStatus.SUCCESS)

    cancel_succ = client.post(f"/api/v1/tasks/{succ_id}/cancel")
    assert cancel_succ.status_code == 409


def test_g84_api_retry_workflow(client: TestClient):
    """Test retrying dead-lettered, queued, and non-existent tasks."""
    # 1. Non-existent task -> 404 Not Found
    fake_id = str(uuid.uuid4())
    fake_retry = client.post(f"/api/v1/tasks/{fake_id}/retry")
    assert fake_retry.status_code == 404

    # 2. Retry a QUEUED task -> 409 Conflict (only FAILED or DEAD_LETTER retryable)
    create_res = client.post("/api/v1/tasks", json={"task_name": "Queued Retry", "task_type": "noop"})
    task_id = create_res.json()["id"]
    retry_queued = client.post(f"/api/v1/tasks/{task_id}/retry")
    assert retry_queued.status_code == 409

    # 3. Transition to DEAD_LETTER and retry -> 200 OK
    with SessionLocal() as db:
        repo = TaskRepository(db)
        repo.update_status(task_id, TaskStatus.RUNNING)
        repo.update_status(task_id, TaskStatus.FAILED)
        repo.update_status(task_id, TaskStatus.DEAD_LETTER)

    retry_res = client.post(f"/api/v1/tasks/{task_id}/retry")
    assert retry_res.status_code == 200
    assert retry_res.json()["status"] == "QUEUED"
    assert retry_res.json()["retry_count"] == 0


@pytest.mark.parametrize(
    "invalid_payload,expected_loc",
    [
        ({"task_name": "", "task_type": "noop"}, "task_name"),
        ({"task_name": "A" * 129, "task_type": "noop"}, "task_name"),
        ({"task_name": "Valid", "task_type": ""}, "task_type"),
        ({"task_name": "Valid", "task_type": "B" * 65}, "task_type"),
        ({"task_name": "Valid", "task_type": "noop", "priority": 0}, "priority"),
        ({"task_name": "Valid", "task_type": "noop", "priority": 5}, "priority"),
        ({"task_name": "Valid", "task_type": "noop", "timeout_seconds": 0}, "timeout_seconds"),
        ({"task_name": "Valid", "task_type": "noop", "timeout_seconds": 3601}, "timeout_seconds"),
        ({"task_name": "Valid", "task_type": "noop", "max_retries": -1}, "max_retries"),
        ({"task_name": "Valid", "task_type": "noop", "max_retries": 11}, "max_retries"),
        ({"task_name": "Valid", "task_type": "noop", "payload": "not_a_dict"}, "payload"),
        ({"task_name": "Valid", "task_type": "noop", "idempotency_key": "K" * 129}, "idempotency_key"),
    ],
)
def test_g84_api_validation_parameterized(client: TestClient, invalid_payload: dict, expected_loc: str):
    """Exhaustive boundary testing asserting 422 with structured API error detail pointing to invalid field."""
    res = client.post("/api/v1/tasks", json=invalid_payload)
    assert res.status_code == 422
    body = res.json()
    assert "error" in body
    assert body["error"]["code"] == "VALIDATION_ERROR"
    assert "correlation_id" in body["error"]
    details = body["error"]["details"]
    assert any(expected_loc in str(err.get("loc", [])) for err in details)


def test_g84_api_error_response_schema_uniformity(client: TestClient):
    """Verify uniform error envelope across 404, 409, 422, and internal errors."""
    # 1. 404 Not Found
    res_404 = client.get(f"/api/v1/tasks/{uuid.uuid4()}")
    assert res_404.status_code == 404
    err_404 = res_404.json()["error"]
    assert "code" in err_404 and "message" in err_404 and "correlation_id" in err_404

    # 2. 422 Validation Error
    res_422 = client.post("/api/v1/tasks", json={})
    assert res_422.status_code == 422
    err_422 = res_422.json()["error"]
    assert "code" in err_422 and "details" in err_422 and "correlation_id" in err_422

    # 3. 409 Conflict (invalid transition)
    create_res = client.post("/api/v1/tasks", json={"task_name": "409 Test", "task_type": "noop"})
    tid = create_res.json()["id"]
    client.post(f"/api/v1/tasks/{tid}/cancel")
    res_409 = client.post(f"/api/v1/tasks/{tid}/cancel")
    assert res_409.status_code == 409
    err_409 = res_409.json()["error"]
    assert "code" in err_409 and "message" in err_409 and "correlation_id" in err_409


def test_g84_api_failed_cancellation_and_retry_leave_db_unmodified(client: TestClient):
    """Verify failed mutation requests (409) perform clean transaction rollback leaving DB untouched."""
    # Setup task in terminal SUCCESS
    res = client.post("/api/v1/tasks", json={"task_name": "Immutability Test", "task_type": "noop"})
    task_id = res.json()["id"]

    with SessionLocal() as db:
        repo = TaskRepository(db)
        repo.update_status(task_id, TaskStatus.RUNNING)
        repo.update_status(task_id, TaskStatus.SUCCESS)

    with SessionLocal() as db:
        repo = TaskRepository(db)
        original = repo.get_by_id(task_id)
        original_status = original.status
        original_updated_at = original.updated_at
        original_events_count = len(original.events)
        original_attempts_count = len(original.attempts)

    # 1. Failed cancellation on terminal SUCCESS -> 409 Conflict
    cancel_res = client.post(f"/api/v1/tasks/{task_id}/cancel")
    assert cancel_res.status_code == 409

    # 2. Failed retry on terminal SUCCESS -> 409 Conflict
    retry_res = client.post(f"/api/v1/tasks/{task_id}/retry")
    assert retry_res.status_code == 409

    # 3. Assert database state remains 100% identical
    with SessionLocal() as db:
        repo = TaskRepository(db)
        current = repo.get_by_id(task_id)
        assert current.status == original_status
        assert current.updated_at == original_updated_at
        assert len(current.events) == original_events_count
        assert len(current.attempts) == original_attempts_count
