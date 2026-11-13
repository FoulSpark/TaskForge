"""Unit tests for Gate G11.4 — Controlled Execution Adapter.

Verifies:
- DefinitionService.execute_definition() execution guard, version snapshot resolution, and locking
- Input and output schema validation gates (Draft 2020-12)
- Idempotency with deterministic SHA-256 request fingerprinting
- Version pinning immutability across version activations and disables
- Worker execution adapter, live TaskRegistry dispatch, and failure classification
- Lease fencing conditional update protections at the database boundary
- API route POST /api/v1/definitions/{id}/execute responses (201, 200, 404, 409, 422)

Runs on in-memory SQLite with foreign keys enforced.
"""

import uuid
from contextlib import contextmanager
from datetime import UTC
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, select, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from taskforge.api.dependencies import get_current_principal
from taskforge.api.schemas import DefinitionExecuteRequest
from taskforge.core.auth import AuthenticatedPrincipal
from taskforge.core.state_machine import TaskStatus
from taskforge.database.models import (
    Task,
    TaskResult,
)
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import Base, get_db
from taskforge.definitions.models import TaskDefinitionVersion
from taskforge.definitions.service import (
    DefinitionConflictError,
    DefinitionIntegrityError,
    DefinitionNotFoundError,
    DefinitionService,
    DefinitionValidationError,
    IdempotencyConflictError,
)
from taskforge.main import app
from taskforge.tasks.registry import TaskRegistry
from taskforge.workers.execution_engine import (
    ExecutionDisposition,
    execute_task,
)

# In-memory SQLite engine
_test_engine = create_engine(
    "sqlite:///:memory:",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)


@event.listens_for(_test_engine, "connect")
def _enable_fk(dbapi_conn, _record):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


_TestSession = sessionmaker(bind=_test_engine, autoflush=False, expire_on_commit=False)


@pytest.fixture(autouse=True)
def _reset_db():
    Base.metadata.create_all(bind=_test_engine)
    yield
    with _test_engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys=OFF"))
        Base.metadata.drop_all(bind=conn)
        conn.execute(text("PRAGMA foreign_keys=ON"))


@pytest.fixture
def db():
    session = _TestSession()
    try:
        yield session
    finally:
        session.close()


def _make_session_local_mock() -> MagicMock:
    """Return a mock for SessionLocal that acts as a context-manager factory.

    ``execute_task`` calls ``with SessionLocal() as db:`` multiple times.
    Each invocation of the mock must return a fresh context manager that opens
    and closes a real SQLite session backed by the shared _test_engine so that
    state written inside execute_task is visible to the test's own db session.
    """

    def _factory(*_args, **_kwargs):
        @contextmanager
        def _cm():
            session = _TestSession()
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        return _cm()

    mock = MagicMock(side_effect=_factory)
    return mock


@pytest.fixture
def principal():
    return AuthenticatedPrincipal(
        key_id="key-g11-tester",
        tenant_id="tenant-alpha",
        name="G11 Tester",
        scopes=["tasks:read", "tasks:write", "definitions:write"],
    )


@pytest.fixture
def other_principal():
    return AuthenticatedPrincipal(
        key_id="key-other-tester",
        tenant_id="tenant-beta",
        name="Other Tester",
        scopes=["tasks:read", "tasks:write", "definitions:write"],
    )


@pytest.fixture
def active_definition(db, principal):
    """Sets up an active definition with a validated Version 1 registered with 'webhook_send' handler."""
    service = DefinitionService(db)
    defn, _ = service.create_definition(
        principal=principal,
        name="Webhook Dispatcher",
        description="Sends transactional webhooks",
        task_type="webhook_send",
        execution_type="builtin",
        configuration_json={"timeout_ms": 5000},
        input_schema_json={
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["url"],
            "properties": {
                "url": {"type": "string"},
                "retries": {"type": "integer"},
            },
            "additionalProperties": False,
        },
        output_schema_json={
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["status"],
            "properties": {
                "status": {"type": "string"},
                "destination": {"type": "string"},
                "delivered": {"type": "boolean"},
            },
        },
        execution_policy_json={"max_retries": 3, "timeout_seconds": 30},
    )
    # Validate and activate Version 1
    _, val_result = service.validate_version(
        principal=principal,
        definition_id=defn.id,
        version_number=1,
    )
    assert val_result.is_valid is True
    service.activate_version(
        principal=principal,
        definition_id=defn.id,
        version_number=1,
    )
    return service.get_definition(principal, defn.id)


# ---------------------------------------------------------------------------
# 1. Service Layer & Execution Guard Tests (G11.4-01 through G11.4-07, G11.4-12)
# ---------------------------------------------------------------------------


def test_execute_definition_happy_path(db, principal, active_definition):
    """T-G11.4-01: Valid submission creates a queued task pinned to active definition version."""
    service = DefinitionService(db)
    req = DefinitionExecuteRequest(
        input={"url": "https://example.com/webhook"},
        priority=3,
        idempotency_key="happy-1",
        timeout_seconds=20,
    )

    task, created = service.execute_definition(
        principal=principal,
        definition_id=active_definition.id,
        request=req,
    )

    assert created is True
    assert task.status == TaskStatus.QUEUED.value
    assert task.definition_id == active_definition.id
    assert task.definition_version_id == active_definition.active_version_id
    assert task.priority == 3
    assert task.timeout_seconds == 20
    assert task.payload["url"] == "https://example.com/webhook"
    assert task.payload["timeout_ms"] == 5000
    assert "_request_fingerprint" in task.payload

    # Verify audit event was committed in the same transaction (G11.4-12)
    events = service.list_audit_events(principal, active_definition.id)
    exec_event = next(e for e in events if e.action == "DEFINITION_EXECUTED")
    assert exec_event.metadata_json["task_id"] == task.id
    assert exec_event.metadata_json["version"] == 1


def test_execute_definition_with_explicit_version(db, principal, active_definition):
    """Submission specifying explicit version number binds to that exact version."""
    service = DefinitionService(db)
    req = DefinitionExecuteRequest(
        version=1,
        input={"url": "https://example.com/webhook-explicit"},
    )
    task, created = service.execute_definition(
        principal=principal,
        definition_id=active_definition.id,
        request=req,
    )
    assert created is True
    assert task.definition_version_id == active_definition.active_version_id


def test_execute_disabled_definition_fails(db, principal, active_definition):
    """T-G11.4-06: Submission against a DISABLED definition is rejected with 409 Conflict."""
    service = DefinitionService(db)
    service.disable_definition(principal=principal, definition_id=active_definition.id)

    req = DefinitionExecuteRequest(input={"url": "https://example.com/webhook"})
    with pytest.raises(DefinitionConflictError) as exc_info:
        service.execute_definition(
            principal=principal,
            definition_id=active_definition.id,
            request=req,
        )
    assert "not ACTIVE" in str(exc_info.value)


def test_execute_unvalidated_version_fails(db, principal, active_definition):
    """T-G11.4-07: Executing an unvalidated draft version is rejected."""
    service = DefinitionService(db)
    # Create draft version 2
    v2 = service.create_version(
        principal=principal,
        definition_id=active_definition.id,
        task_type="webhook_send",
        execution_type="builtin",
        configuration_json={},
        input_schema_json={},
    )
    assert v2.validation_status == "UNVALIDATED"

    req = DefinitionExecuteRequest(
        version=2,
        input={"url": "https://example.com/webhook"},
    )
    with pytest.raises(DefinitionValidationError) as exc_info:
        service.execute_definition(
            principal=principal,
            definition_id=active_definition.id,
            request=req,
        )
    assert "not VALID" in str(exc_info.value)


def test_execute_input_schema_validation_failure(db, principal, active_definition):
    """T-G11.4-02: Input violating Draft 2020-12 schema is rejected before creating task."""
    service = DefinitionService(db)
    # Missing required 'url' field and invalid property type
    req = DefinitionExecuteRequest(
        input={"retries": "not_an_int"},  # retries must be integer, url is required
    )
    with pytest.raises(DefinitionValidationError) as exc_info:
        service.execute_definition(
            principal=principal,
            definition_id=active_definition.id,
            request=req,
        )
    assert "Input validation failed" in str(exc_info.value)


def test_execute_cross_tenant_isolation(db, other_principal, active_definition):
    """T-G11.4-10: Tenant B cannot execute Tenant A's definition."""
    service = DefinitionService(db)
    req = DefinitionExecuteRequest(input={"url": "https://example.com/webhook"})
    with pytest.raises(DefinitionNotFoundError):
        service.execute_definition(
            principal=other_principal,
            definition_id=active_definition.id,
            request=req,
        )


# ---------------------------------------------------------------------------
# 2. Idempotency & Request Fingerprinting (R2 / G11.4-07)
# ---------------------------------------------------------------------------


def test_idempotent_replay_returns_existing_task(db, principal, active_definition):
    """T-G11.4-05: Repeated submission with same idempotency key and payload returns existing task."""
    service = DefinitionService(db)
    req = DefinitionExecuteRequest(
        input={"url": "https://example.com/webhook"},
        idempotency_key="idemp-key-1",
        priority=2,
    )
    task1, created1 = service.execute_definition(
        principal=principal, definition_id=active_definition.id, request=req
    )
    assert created1 is True

    # Replay identical request
    task2, created2 = service.execute_definition(
        principal=principal, definition_id=active_definition.id, request=req
    )
    assert created2 is False
    assert task2.id == task1.id
    assert task2.definition_version_id == task1.definition_version_id


def test_idempotent_replay_mismatched_payload_conflicts(db, principal, active_definition):
    """R2: Reusing idempotency key with different input payload raises 409 Conflict."""
    service = DefinitionService(db)
    req1 = DefinitionExecuteRequest(
        input={"url": "https://example.com/hook1"},
        idempotency_key="idemp-key-2",
    )
    service.execute_definition(
        principal=principal, definition_id=active_definition.id, request=req1
    )

    # Replay with altered payload
    req2 = DefinitionExecuteRequest(
        input={"url": "https://example.com/hook2"},
        idempotency_key="idemp-key-2",
    )
    with pytest.raises(IdempotencyConflictError):
        service.execute_definition(
            principal=principal, definition_id=active_definition.id, request=req2
        )


def test_idempotent_replay_after_new_version_activated(db, principal, active_definition):
    """R2: Replaying request after V2 is activated returns task pinned to original V1."""
    service = DefinitionService(db)
    req = DefinitionExecuteRequest(
        input={"url": "https://example.com/webhook"},
        idempotency_key="idemp-key-v1",
    )
    task1, created1 = service.execute_definition(
        principal=principal, definition_id=active_definition.id, request=req
    )
    assert created1 is True
    assert task1.definition_version_id == active_definition.active_version_id

    # Create and activate Version 2
    service.create_version(
        principal=principal,
        definition_id=active_definition.id,
        task_type="webhook_send",
        execution_type="builtin",
        configuration_json={"timeout_ms": 10000},
        input_schema_json=active_definition.versions[0].input_schema_json,
    )
    service.validate_version(
        principal=principal, definition_id=active_definition.id, version_number=2
    )
    service.activate_version(
        principal=principal, definition_id=active_definition.id, version_number=2
    )

    # Replay key from before V2 was activated
    task2, created2 = service.execute_definition(
        principal=principal, definition_id=active_definition.id, request=req
    )
    assert created2 is False
    assert task2.id == task1.id
    # Must preserve V1 attribution and NOT bind to V2
    assert task2.definition_version_id == task1.definition_version_id


# ---------------------------------------------------------------------------
# 3. Version Pinning Immutability (R1)
# ---------------------------------------------------------------------------


def test_referenced_version_cannot_be_deleted(db, principal, active_definition):
    """R1: RESTRICT foreign key prevents deleting a version referenced by an existing task."""
    service = DefinitionService(db)
    req = DefinitionExecuteRequest(input={"url": "https://example.com/webhook"})
    task, _ = service.execute_definition(
        principal=principal, definition_id=active_definition.id, request=req
    )

    # Attempt to directly delete version row via session
    from sqlalchemy.exc import IntegrityError

    ver = db.get(TaskDefinitionVersion, task.definition_version_id)
    db.delete(ver)
    with pytest.raises(IntegrityError):  # foreign key violation
        db.commit()
    db.rollback()


def test_tampered_version_content_hash_fails_integrity(db, principal, active_definition):
    """R1: Content hash mismatch causes immediate rejection with DefinitionIntegrityError."""
    service = DefinitionService(db)
    # Tamper with version content hash in DB
    ver = db.get(TaskDefinitionVersion, active_definition.active_version_id)
    ver.content_hash = "0" * 64
    db.commit()

    req = DefinitionExecuteRequest(input={"url": "https://example.com/webhook"})
    with pytest.raises(DefinitionIntegrityError):
        service.execute_definition(
            principal=principal, definition_id=active_definition.id, request=req
        )


# ---------------------------------------------------------------------------
# 4. Worker Execution Adapter & Failure Classification (R4, R5, G11.4-05, G11.4-06)
# ---------------------------------------------------------------------------


def test_worker_executes_custom_task_successfully(db, principal, active_definition):
    """T-G11.4-01: Worker executes custom task, validates input and output, commits SUCCESS."""
    service = DefinitionService(db)
    req = DefinitionExecuteRequest(
        input={"url": "https://example.com/webhook"},
    )
    task, _ = service.execute_definition(
        principal=principal, definition_id=active_definition.id, request=req
    )

    # Register mock webhook_send handler that returns valid output
    def mock_webhook_send(payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "status": "sent",
            "delivered": True,
            "destination": payload.get("url", ""),
        }

    with patch.dict(TaskRegistry._HANDLERS, {"webhook_send": mock_webhook_send}):
        with patch(
            "taskforge.workers.execution_engine.SessionLocal",
            new_callable=_make_session_local_mock,
        ):
            with patch(
                "taskforge.workers.lease_manager.SessionLocal",
                new_callable=_make_session_local_mock,
            ):
                disp = execute_task(
                    task_id=task.id,
                    worker_id="test-worker-1",
                    timeout_seconds=10,
                    use_outbox=False,
                )

    assert disp == ExecutionDisposition.COMPLETED

    # Verify task state in DB (expire identity map to force fresh read from SQLite)
    db.expire_all()
    refreshed = db.get(Task, task.id)
    assert refreshed.status == TaskStatus.SUCCESS.value
    assert refreshed.result is not None
    assert refreshed.result.return_value == {
        "status": "sent",
        "delivered": True,
        "destination": "https://example.com/webhook",
    }


def test_worker_output_schema_violation_fails_permanently(db, principal, active_definition):
    """T-G11.4-03 / R5: Handler output violating output schema fails permanently (non-retryable)."""
    service = DefinitionService(db)
    req = DefinitionExecuteRequest(
        input={"url": "https://example.com/webhook"},
    )
    task, _ = service.execute_definition(
        principal=principal, definition_id=active_definition.id, request=req
    )

    # Handler returns invalid output (missing required 'status' property)
    def bad_output_handler(payload: dict[str, Any]) -> dict[str, Any]:
        return {"invalid_key": "oops"}

    with patch.dict(TaskRegistry._HANDLERS, {"webhook_send": bad_output_handler}):
        with patch(
            "taskforge.workers.execution_engine.SessionLocal",
            new_callable=_make_session_local_mock,
        ):
            with patch(
                "taskforge.workers.lease_manager.SessionLocal",
                new_callable=_make_session_local_mock,
            ):
                disp = execute_task(
                    task_id=task.id,
                    worker_id="test-worker-1",
                    timeout_seconds=10,
                    use_outbox=False,
                )

    assert disp == ExecutionDisposition.TERMINAL_FAILURE_RECORDED

    db.expire_all()
    refreshed = db.get(Task, task.id)
    assert refreshed.status == TaskStatus.FAILED.value
    assert refreshed.retry_count == 0  # No retry scheduled

    # Verify error classification in TaskEvent
    event = next(
        e
        for e in refreshed.events
        if e.event_type == "STATUS_CHANGED" and e.to_state == TaskStatus.FAILED.value
    )
    assert event.metadata_json["error_type"] == "OUTPUT_VALIDATION_ERROR"
    assert event.metadata_json["retryable"] is False


def test_worker_handler_exception_triggers_retry(db, principal, active_definition):
    """T-G11.4-09 / R5: Handler exception is classified as HANDLER_EXECUTION_ERROR and retries."""
    service = DefinitionService(db)
    req = DefinitionExecuteRequest(
        input={"url": "https://example.com/webhook"},
    )
    task, _ = service.execute_definition(
        principal=principal, definition_id=active_definition.id, request=req
    )

    def crashing_handler(payload: dict[str, Any]) -> dict[str, Any]:
        raise RuntimeError("Transient connection reset")

    with patch.dict(TaskRegistry._HANDLERS, {"webhook_send": crashing_handler}):
        with patch(
            "taskforge.workers.execution_engine.SessionLocal",
            new_callable=_make_session_local_mock,
        ):
            with patch(
                "taskforge.workers.lease_manager.SessionLocal",
                new_callable=_make_session_local_mock,
            ):
                disp = execute_task(
                    task_id=task.id,
                    worker_id="test-worker-1",
                    timeout_seconds=10,
                    use_outbox=False,
                )

    assert disp == ExecutionDisposition.RETRY_PENDING_PUBLICATION

    db.expire_all()
    refreshed = db.get(Task, task.id)
    assert refreshed.status == TaskStatus.RETRY_WAIT.value
    assert refreshed.retry_count == 1
    # Pinned version remains immutable across retries (G11.4-08)
    assert refreshed.definition_version_id == active_definition.active_version_id


def test_worker_handler_missing_fails_safely(db, principal, active_definition):
    """T-G11.4-11 / R6: Missing or uncallable handler raises non-retryable HANDLER_UNAVAILABLE_ERROR."""
    service = DefinitionService(db)
    req = DefinitionExecuteRequest(
        input={"url": "https://example.com/webhook"},
    )
    task, _ = service.execute_definition(
        principal=principal, definition_id=active_definition.id, request=req
    )

    # Unregister handler from live registry
    with patch.dict(TaskRegistry._HANDLERS, {}, clear=True):
        with patch(
            "taskforge.workers.execution_engine.SessionLocal",
            new_callable=_make_session_local_mock,
        ):
            with patch(
                "taskforge.workers.lease_manager.SessionLocal",
                new_callable=_make_session_local_mock,
            ):
                disp = execute_task(
                    task_id=task.id,
                    worker_id="test-worker-1",
                    timeout_seconds=10,
                    use_outbox=False,
                )

    assert disp == ExecutionDisposition.TERMINAL_FAILURE_RECORDED
    db.expire_all()
    refreshed = db.get(Task, task.id)
    assert refreshed.status == TaskStatus.FAILED.value
    event = next(
        e
        for e in refreshed.events
        if e.event_type == "STATUS_CHANGED" and e.to_state == TaskStatus.FAILED.value
    )
    assert event.metadata_json["error_type"] == "HANDLER_UNAVAILABLE_ERROR"


# ---------------------------------------------------------------------------
# 5. Lease Fencing & Zero-Artifact DB Boundary Protection (R4 / G11.4-09)
# ---------------------------------------------------------------------------


def test_stale_worker_fencing_at_db_boundary(db, principal, active_definition):
    """T-G11.4-08 / R4: Stale worker with superseded lease_token cannot commit success or failure.

    Zero success artifacts (no TaskResult, no TaskAttempt success update) may be committed.
    """
    service = DefinitionService(db)
    req = DefinitionExecuteRequest(
        input={"url": "https://example.com/webhook"},
    )
    task, _ = service.execute_definition(
        principal=principal, definition_id=active_definition.id, request=req
    )

    repo = TaskRepository(db)
    # Transition QUEUED → RUNNING without a lease token (task has none yet),
    # then directly set the lease_token to simulate Worker 1 claiming the task.
    stale_token = "lease-token-worker-1"
    repo.update_status(task.id, TaskStatus.RUNNING)
    # Directly assign the lease token and expiry at the DB row level to simulate Worker 1 holding it.
    t_row = db.get(Task, task.id)
    t_row.lease_token = stale_token
    from datetime import datetime, timedelta

    t_row.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
    db.commit()
    db.expire_all()

    attempt1 = repo.record_attempt(
        task_id=task.id,
        attempt_number=1,
        worker_id="worker-1",
        status="RUNNING",
        lease_token=stale_token,
    )

    # Worker 2 supersedes Worker 1 with a new lease token
    active_token = "lease-token-worker-2"
    t_row = db.get(Task, task.id)
    t_row.lease_token = active_token
    t_row.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
    db.commit()
    db.expire_all()

    # Worker 1 attempts to commit complete_task_success with stale_token
    with pytest.raises(PermissionError) as exc_info:
        repo.complete_task_success(
            task_id=task.id,
            lease_token=stale_token,
            return_value={"status": "STALE_SUCCESS"},
            attempt_id=attempt1.id,
        )
    assert "Fencing violation" in str(exc_info.value)

    # Assert ZERO success artifacts were written by Worker 1
    db.expire_all()
    results = db.scalars(select(TaskResult).where(TaskResult.task_id == task.id)).all()
    assert len(results) == 0, "Zero TaskResult records must be persisted on fencing violation"

    # Worker 2 successfully commits under active_token
    repo.complete_task_success(
        task_id=task.id,
        lease_token=active_token,
        return_value={
            "status": "sent",
            "delivered": True,
            "destination": "https://example.com/webhook",
        },
    )
    db.expire_all()
    refreshed_task = db.get(Task, task.id)
    assert refreshed_task.status == TaskStatus.SUCCESS.value
    assert refreshed_task.result.return_value == {
        "status": "sent",
        "delivered": True,
        "destination": "https://example.com/webhook",
    }


# ---------------------------------------------------------------------------
# 6. API Route Verification (POST /api/v1/definitions/{id}/execute)
# ---------------------------------------------------------------------------


def test_api_route_execute_definition_201_and_200(db, principal, active_definition):
    """API endpoint returns 201 Created on first call, 200 OK on idempotent replay."""
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_current_principal] = lambda: principal
    client = TestClient(app)

    try:
        body = {
            "input": {"url": "https://example.com/webhook"},
            "priority": 3,
            "idempotency_key": "api-exec-1",
        }
        res1 = client.post(f"/api/v1/definitions/{active_definition.id}/execute", json=body)
        assert res1.status_code == 201
        data1 = res1.json()
        assert data1["definition_id"] == active_definition.id
        assert data1["definition_version_id"] == active_definition.active_version_id

        # Replay
        res2 = client.post(f"/api/v1/definitions/{active_definition.id}/execute", json=body)
        assert res2.status_code == 200
        assert res2.json()["id"] == data1["id"]
    finally:
        app.dependency_overrides.clear()


def test_api_route_execute_definition_422_schema_error(db, principal, active_definition):
    """API endpoint returns 422 Unprocessable Entity when input schema is violated."""
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_current_principal] = lambda: principal
    client = TestClient(app)

    try:
        body = {
            "input": {"invalid_field": True},  # Missing required url
        }
        res = client.post(f"/api/v1/definitions/{active_definition.id}/execute", json=body)
        assert res.status_code == 422
        response_body = res.json()
        # The app uses a custom error envelope: {"error": {"code": ..., "message": ...}}
        assert "Input validation failed" in response_body["error"]["message"]
    finally:
        app.dependency_overrides.clear()


def test_api_route_execute_definition_404_not_found(db, principal):
    """API endpoint returns 404 Not Found for non-existent definition."""
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_current_principal] = lambda: principal
    client = TestClient(app)

    try:
        res = client.post(f"/api/v1/definitions/{uuid.uuid4()}/execute", json={"input": {}})
        assert res.status_code == 404
    finally:
        app.dependency_overrides.clear()
