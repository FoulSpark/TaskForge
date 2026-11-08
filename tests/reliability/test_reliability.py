import os
import tempfile
import time
import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from taskforge.database.session import Base, get_db
from taskforge.main import app
from taskforge.queue.broker import MemoryBroker, set_global_broker
from taskforge.workers.execution_engine import execute_task

TEST_DB_FILE = os.path.join(tempfile.gettempdir(), f"taskforge_test_rel_{uuid.uuid4().hex}.db")
TEST_DATABASE_URL = f"sqlite:///{TEST_DB_FILE}"
test_engine = create_engine(
    TEST_DATABASE_URL,
    connect_args={"check_same_thread": False, "timeout": 15},
)
TestingSessionLocal = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)


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
    monkeypatch.setattr("taskforge.main.check_database", lambda: True)
    monkeypatch.setattr("taskforge.database.session.SessionLocal", TestingSessionLocal)
    monkeypatch.setattr("taskforge.workers.execution_engine.SessionLocal", TestingSessionLocal)
    monkeypatch.setattr("taskforge.workers.lease_manager.SessionLocal", TestingSessionLocal)
    monkeypatch.setattr("taskforge.workers.dispatcher.SessionLocal", TestingSessionLocal)

    broker = MemoryBroker()
    set_global_broker(broker)

    yield broker

    Base.metadata.drop_all(bind=test_engine)
    app.dependency_overrides.clear()


def test_p3_01_idempotency_100_repeated_submissions(setup_test_env):
    """
    Acceptance Criterion P3-01:
    100 repeated submissions with the same idempotency key create exactly 1 logical task.
    """
    idemp_key = f"fixed-key-{uuid.uuid4()}"
    with TestClient(app) as client:
        first_res = client.post(
            "/api/v1/tasks",
            json={
                "task_name": "Repeated Task",
                "task_type": "json_transform",
                "payload": {"num": 42},
                "idempotency_key": idemp_key,
            },
        )
        assert first_res.status_code == 201
        expected_task_id = first_res.json()["id"]

        # Submit 99 more times with the identical key
        for _ in range(99):
            res = client.post(
                "/api/v1/tasks",
                json={
                    "task_name": "Repeated Task",
                    "task_type": "json_transform",
                    "payload": {"num": 42},
                    "idempotency_key": idemp_key,
                },
            )
            assert res.status_code == 200
            assert res.json()["id"] == expected_task_id

        # Verify only 1 task exists in database
        tasks_list = client.get("/api/v1/tasks").json()
        assert tasks_list["total"] == 1


def test_p3_02_and_p3_05_max_retries_and_dead_letter(setup_test_env):
    """
    Acceptance Criteria P3-02 & P3-05:
    Retry count never exceeds configured max (e.g. 2), then enters DEAD_LETTER state,
    and remains fully inspectable with error messages and attempt records.
    """
    _ = setup_test_env
    with TestClient(app) as client:
        res = client.post(
            "/api/v1/tasks",
            json={
                "task_name": "Always Fails",
                "task_type": "failure_simulation",
                "payload": {
                    "fail_until_attempt": 999,
                    "error_message": "Critical simulated failure",
                },
                "max_retries": 2,
            },
        )
        task_id = res.json()["id"]

        # Run attempt 1
        execute_task(task_id, "worker-1")
        t1 = client.get(f"/api/v1/tasks/{task_id}").json()
        assert t1["status"] == "RETRY_WAIT"
        assert t1["retry_count"] == 1
        assert len(t1["attempts"]) == 1

        # Wait for attempt 1 backoff delay (1s) to elapse
        time.sleep(1.1)

        # Run attempt 2 (retry 1)
        execute_task(task_id, "worker-1")
        t2 = client.get(f"/api/v1/tasks/{task_id}").json()
        assert t2["status"] == "RETRY_WAIT"
        assert t2["retry_count"] == 2
        assert len(t2["attempts"]) == 2

        # Wait for attempt 2 backoff delay (2s) to elapse
        time.sleep(2.1)

        # Run attempt 3 (retry 2, reaching max_retries 2)
        execute_task(task_id, "worker-1")
        t3 = client.get(f"/api/v1/tasks/{task_id}").json()
        assert t3["status"] == "DEAD_LETTER"
        assert t3["retry_count"] == 2  # Never exceeds configured max_retries!
        assert len(t3["attempts"]) == 3
        # Attempt audit records exist
        for att in t3["attempts"]:
            assert att["status"] == "FAILED"
            assert "Critical simulated failure" in att["error_message"]


def test_p3_06_duplicate_delivery_safety(setup_test_env):
    """
    Acceptance Criterion P3-06:
    Repeated delivery of a task message when the task is already SUCCESS
    does not cause duplicate side effects or state corruption.
    """
    with TestClient(app) as client:
        res = client.post(
            "/api/v1/tasks",
            json={
                "task_name": "Completed Task",
                "task_type": "json_transform",
                "payload": {"records": [1, 2]},
            },
        )
        task_id = res.json()["id"]

        # Execute once -> SUCCESS
        assert execute_task(task_id, "worker-1").is_success()
        t_after = client.get(f"/api/v1/tasks/{task_id}").json()
        assert t_after["status"] == "SUCCESS"
        assert len(t_after["attempts"]) == 1

        # Deliver the duplicate message to worker
        assert execute_task(task_id, "worker-1").is_success()

        # Status remains SUCCESS and attempts are not duplicated
        t_duplicate = client.get(f"/api/v1/tasks/{task_id}").json()
        assert t_duplicate["status"] == "SUCCESS"
        assert len(t_duplicate["attempts"]) == 1


def test_p3_07_task_timeout_enforcement(setup_test_env):
    """
    Acceptance Criterion P3-07:
    Execution deadlines are enforced when task exceeds timeout_seconds.
    """
    with TestClient(app) as client:
        res = client.post(
            "/api/v1/tasks",
            json={
                "task_name": "Timeout Task",
                "task_type": "timeout_simulation",
                "payload": {"sleep_seconds": 2.0},
                "timeout_seconds": 1,  # 1s timeout
                "max_retries": 0,
            },
        )
        task_id = res.json()["id"]

        execute_task(task_id, "worker-1")

        task = client.get(f"/api/v1/tasks/{task_id}").json()
        assert task["status"] in {"FAILED", "DEAD_LETTER"}
        assert len(task["attempts"]) == 1
        assert "timed out" in task["attempts"][0]["error_message"].lower()
