"""End-to-end integration tests for Phase 10 Gate F1-B frontend serving, CSP, and vertical slice."""

import os
import tempfile
import time
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from taskforge.core.auth import ApiKeyService
from taskforge.database.session import Base, get_db
from taskforge.main import FRONTEND_CSP, FRONTEND_DIST_DIR, app
from taskforge.queue.broker import MemoryBroker, set_global_broker
from taskforge.queue.outbox import publish_pending_outbox_events
from taskforge.workers.dispatcher import WorkerDispatcher

TEST_DB_FILE = os.path.join(tempfile.gettempdir(), f"taskforge_f1b_e2e_{uuid.uuid4().hex}.db")
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
def setup_e2e_env(monkeypatch):
    Base.metadata.create_all(bind=test_engine)
    broker = MemoryBroker()
    app.dependency_overrides[get_db] = override_get_db
    monkeypatch.setenv("TASKFORGE_STRICT_AUTH", "1")
    monkeypatch.setattr("taskforge.main.check_database", lambda: True)
    monkeypatch.setattr("taskforge.main.check_redis", lambda _client=None: True)
    monkeypatch.setattr("taskforge.database.session.SessionLocal", TestingSessionLocal)
    monkeypatch.setattr("taskforge.workers.execution_engine.SessionLocal", TestingSessionLocal)
    monkeypatch.setattr("taskforge.workers.lease_manager.SessionLocal", TestingSessionLocal)
    monkeypatch.setattr("taskforge.workers.dispatcher.SessionLocal", TestingSessionLocal)

    original_broker = getattr(app.state, "broker", None)
    app.state.broker = broker
    set_global_broker(broker)
    yield broker
    set_global_broker(original_broker)
    app.state.broker = original_broker
    Base.metadata.drop_all(bind=test_engine)
    app.dependency_overrides.clear()


def test_spa_serving_csp_and_root_json_contract():
    """Verify FastAPI serves built Vite SPA with strict CSP for browsers while preserving JSON at /."""
    assert (FRONTEND_DIST_DIR / "index.html").exists(), "frontend/dist/index.html must be built"

    with TestClient(app) as client:
        # Non-HTML Accept on / must return API JSON (preserves Phase 9 root contract)
        json_root = client.get("/", headers={"Accept": "application/json"})
        assert json_root.status_code == 200
        assert json_root.json()["application"] == "TaskForge"

        # HTML browser requests on / and SPA routes serve built React SPA + strict CSP
        for route in ("/", "/ui", "/submit", "/tasks", "/monitoring"):
            res = client.get(route, headers={"Accept": "text/html"})
            assert res.status_code == 200
            assert "text/html" in res.headers["content-type"]
            assert res.headers.get("Content-Security-Policy") == FRONTEND_CSP
            assert "TaskForge" in res.text
            assert "/assets/" in res.text

        # Built JS and CSS bundles under frontend/dist/assets must resolve cleanly
        assets_dir = Path(FRONTEND_DIST_DIR / "assets")
        bundle_files = list(assets_dir.iterdir())
        assert bundle_files, "Expected compiled assets inside frontend/dist/assets"
        for asset_path in bundle_files:
            asset_res = client.get(f"/assets/{asset_path.name}")
            assert asset_res.status_code == 200


def test_vertical_slice_e2e_api_to_worker_execution(setup_e2e_env):
    """Exercise full F1 vertical slice: create key -> auth/me -> task-types -> submit -> execute -> inspect."""
    broker = setup_e2e_env
    with TestingSessionLocal() as db:
        svc = ApiKeyService(db)
        plaintext_key, _ = svc.create_api_key(
            tenant_id="tenant-e2e",
            name="e2e-console",
            scopes=["tasks:read", "tasks:write"],
        )

    headers = {"Authorization": f"Bearer {plaintext_key}"}
    dispatcher = WorkerDispatcher(worker_id="e2e-worker-1", concurrency=2, broker=broker)
    dispatcher.start()

    try:
        with TestClient(app) as client:
            # 1. Authenticate
            me_res = client.get("/api/v1/auth/me", headers=headers)
            assert me_res.status_code == 200
            assert me_res.json()["tenant_id"] == "tenant-e2e"

            # 2. Discover runtime task types
            cat_res = client.get("/api/v1/task-types", headers=headers)
            assert cat_res.status_code == 200
            types = {item["task_type"]: item for item in cat_res.json()["items"]}
            assert types["compute_prime"]["available"] is True

            # 3. Submit task
            sub_res = client.post(
                "/api/v1/tasks",
                headers=headers,
                json={
                    "task_name": "E2E Vertical Slice Prime",
                    "task_type": "compute_prime",
                    "payload": {"n": 30},
                    "priority": 3,
                    "timeout_seconds": 30,
                    "max_retries": 1,
                },
            )
            assert sub_res.status_code == 201
            task_id = sub_res.json()["id"]

            # 4. Dispatch via outbox & wait for worker completion
            with TestingSessionLocal() as db:
                publish_pending_outbox_events(db, broker)

            deadline = time.time() + 10.0
            detail_json = None
            while time.time() < deadline:
                det_res = client.get(f"/api/v1/tasks/{task_id}", headers=headers)
                assert det_res.status_code == 200
                detail_json = det_res.json()
                if detail_json["status"] == "SUCCESS":
                    break
                time.sleep(0.05)

            assert detail_json is not None
            assert detail_json["status"] == "SUCCESS"
            assert detail_json["result"] is not None
            assert detail_json["result"]["return_value"]["prime_count"] == 10
            assert len(detail_json["attempts"]) == 1
            assert detail_json["attempts"][0]["worker_id"] == "e2e-worker-1"
            assert "lease_token" not in str(detail_json)

            # 5. Inspect monitoring summary (same tenant, 1 completed task => low N warning)
            mon_res = client.get("/api/v1/monitoring/summary?window_minutes=5", headers=headers)
            assert mon_res.status_code == 200
            mon_data = mon_res.json()
            assert mon_data["tenant_id"] == "tenant-e2e"
            assert mon_data["throughput"]["observation_count"] == 1
            assert mon_data["latencies_ms"]["handler_duration"]["sufficient_sample_size"] is False
            assert mon_data["cluster"] is None
    finally:
        dispatcher.stop()
