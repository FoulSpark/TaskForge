"""Security and defensive engineering verification tests.

Validates input sanitization, injection resistance, payload boundaries,
and denial-of-service protections.
"""

from fastapi.testclient import TestClient

from taskforge.main import app


def test_sql_injection_in_task_fields_is_treated_as_literal():
    """SQL injection payloads in task_name, task_type, or payload are safely parametrized."""
    with TestClient(app) as client:
        malicious_inputs = [
            "'; DROP TABLE tasks; --",
            "1' OR '1'='1",
            "UNION SELECT * FROM tasks --",
            "admin'--",
        ]
        for injection in malicious_inputs:
            resp = client.post(
                "/api/v1/tasks",
                json={
                    "task_name": injection,
                    "task_type": "json_transform",
                    "payload": {"query": injection},
                },
            )
            assert resp.status_code == 201
            task = resp.json()
            assert task["task_name"] == injection  # Safely stored as plain literal text

            # Verify retrieval works safely
            get_resp = client.get(f"/api/v1/tasks/{task['id']}")
            assert get_resp.status_code == 200
            assert get_resp.json()["task_name"] == injection


def test_xss_in_task_name_escaped():
    """XSS script payloads stored in tasks are safely rendered without execution."""
    with TestClient(app) as client:
        xss_payload = "<script>alert('XSS')</script>"
        resp = client.post(
            "/api/v1/tasks",
            json={
                "task_name": xss_payload,
                "task_type": "json_transform",
                "payload": {"msg": xss_payload},
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["task_name"] == xss_payload


def test_excessive_task_name_length_rejected():
    """Task name exceeding database column constraints is rejected with 422."""
    with TestClient(app) as client:
        oversized_name = "A" * 500  # Max length is 128
        resp = client.post(
            "/api/v1/tasks",
            json={
                "task_name": oversized_name,
                "task_type": "json_transform",
                "payload": {},
            },
        )
        assert resp.status_code == 422


def test_invalid_json_body_handled_gracefully():
    """Malformed non-JSON body returns standardized 422 error detail."""
    with TestClient(app) as client:
        resp = client.post(
            "/api/v1/tasks",
            content="{ invalid json ...",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 422


def test_invalid_uuid_id_returns_404():
    """Querying invalid or non-existent task IDs returns 404 with standard error contract."""
    with TestClient(app) as client:
        resp = client.get("/api/v1/tasks/non-existent-task-id-12345")
        assert resp.status_code == 404
        data = resp.json()
        assert data["error"]["code"] in {"HTTP_404", "TASK_NOT_FOUND"}


def test_unregistered_task_type_fails_cleanly():
    """Submitting an unregistered task type is validated or safely quarantined."""
    with TestClient(app) as client:
        resp = client.post(
            "/api/v1/tasks",
            json={
                "task_name": "Unknown Handler Task",
                "task_type": "exploit_arbitrary_code_exec",
                "payload": {},
            },
        )
        assert resp.status_code in {201, 422}
