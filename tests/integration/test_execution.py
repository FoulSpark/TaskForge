import time
import uuid
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from taskforge.database.session import Base, get_db
import os
import tempfile
from taskforge.main import app
from taskforge.queue.broker import MemoryBroker, set_global_broker
from taskforge.queue.outbox import publish_pending_outbox_events
from taskforge.workers.dispatcher import WorkerDispatcher
from taskforge.workers.heartbeat import get_active_workers

TEST_DB_FILE = os.path.join(tempfile.gettempdir(), f"taskforge_test_{uuid.uuid4().hex}.db")
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
    set_global_broker(MemoryBroker())


def test_async_submission_and_outbox_flush(setup_test_env):
    broker = setup_test_env
    with TestClient(app) as client:
        res = client.post(
            "/api/v1/tasks",
            json={
                "task_name": "Outbox Async Task",
                "task_type": "json_transform",
                "payload": {"records": [10, 20, 30]},
            },
        )
        assert res.status_code == 201
        task_id = res.json()["id"]

        # Before outbox flush, broker has 0 items
        assert broker.get_queue_depth() == 0

        # Run outbox publisher
        with TestingSessionLocal() as db:
            count = publish_pending_outbox_events(db, broker)
            assert count >= 1

        # Now broker has the task
        assert broker.get_queue_depth() == 1
        # Dequeue to leave broker empty for subsequent tests
        broker.dequeue()


def test_all_workload_handlers_execute_successfully(setup_test_env):
    broker = setup_test_env
    dispatcher = WorkerDispatcher(worker_id="test-worker-1", concurrency=2, broker=broker)
    dispatcher.start()

    workloads = [
        ("json_transform", {"records": [1, 2, 3, 4]}),
        ("compute_prime", {"n": 50}),
        ("io_simulation", {"delay_seconds": 0.01, "jitter": False}),
        ("db_process", {"batch_size": 20}),
        ("long_running", {"steps": 2, "step_duration": 0.01}),
    ]

    with TestClient(app) as client:
        task_ids = []
        for name, payload in workloads:
            res = client.post(
                "/api/v1/tasks",
                json={"task_name": f"Test {name}", "task_type": name, "payload": payload},
            )
            assert res.status_code == 201
            task_ids.append(res.json()["id"])

        # Flush outbox
        with TestingSessionLocal() as db:
            publish_pending_outbox_events(db, broker)

        # Wait for worker to finish processing
        start_wait = time.time()
        while time.time() - start_wait < 10.0:
            all_done = True
            for tid in task_ids:
                data = client.get(f"/api/v1/tasks/{tid}").json()
                if data["status"] != "SUCCESS":
                    all_done = False
                    break
            if all_done:
                break
            time.sleep(0.05)

        dispatcher.stop()

        # Verify all workloads succeeded and have results
        for tid in task_ids:
            task = client.get(f"/api/v1/tasks/{tid}").json()
            assert task["status"] == "SUCCESS", f"Task {task['task_type']} ({tid}) status was {task['status']}"
            assert task["result"] is not None
            assert task["result"]["return_value"] is not None
            assert len(task["attempts"]) >= 1


def test_worker_heartbeat_observable(setup_test_env):
    broker = setup_test_env
    dispatcher = WorkerDispatcher(worker_id="heartbeat-worker-test", concurrency=4, broker=broker)
    dispatcher.start()

    active = get_active_workers()
    assert any(w["worker_id"] == "heartbeat-worker-test" for w in active)

    dispatcher.stop()


def test_execution_isolation_one_failure_does_not_block_others(setup_test_env):
    broker = setup_test_env
    dispatcher = WorkerDispatcher(worker_id="iso-worker", concurrency=2, broker=broker)
    dispatcher.start()

    with TestClient(app) as client:
        # Submit 1 failing task
        failing_res = client.post(
            "/api/v1/tasks",
            json={
                "task_name": "Failing Task",
                "task_type": "failure_simulation",
                "payload": {"fail_until_attempt": 999},
                "max_retries": 0,
            },
        )
        fail_id = failing_res.json()["id"]

        # Submit 1 normal task
        good_res = client.post(
            "/api/v1/tasks",
            json={
                "task_name": "Good Task",
                "task_type": "json_transform",
                "payload": {"records": [100]},
            },
        )
        good_id = good_res.json()["id"]

        with TestingSessionLocal() as db:
            publish_pending_outbox_events(db, broker)

        # Wait for processing
        start_wait = time.time()
        while time.time() - start_wait < 5.0:
            fail_task = client.get(f"/api/v1/tasks/{fail_id}").json()
            good_task = client.get(f"/api/v1/tasks/{good_id}").json()
            if fail_task["status"] in {"FAILED", "DEAD_LETTER"} and good_task["status"] == "SUCCESS":
                break
            time.sleep(0.05)

        dispatcher.stop()

        fail_task = client.get(f"/api/v1/tasks/{fail_id}").json()
        good_task = client.get(f"/api/v1/tasks/{good_id}").json()

        assert fail_task["status"] in {"FAILED", "DEAD_LETTER"}
        assert good_task["status"] == "SUCCESS"
