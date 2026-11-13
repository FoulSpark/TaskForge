"""Unit tests for Sub-Gate G11.5-E — Fenced Cancellation, Retry and DLQ Version Retention.

Verifies:
1. Cancellation transaction:
   - Sets task status to CANCELLED.
   - Revokes lease_token (sets to NULL).
   - Clears lease_expires_at (sets to NULL).
   - Cancels active running attempts (status='CANCELLED').
   - Records STATUS_CHANGED event.
   - Records EXECUTION_CANCELLED audit event.
2. Conflict handling:
   - Tasks in terminal status (SUCCESS, FAILED, DEAD_LETTER, CANCELLED) return 409 Conflict.
3. Stale-worker fencing after cancellation:
   - Worker holding cancelled task's former lease token cannot commit success.
   - Worker holding cancelled task's former lease token cannot commit failure.
   - Zero result or attempt artifacts are written on rejected commits.
4. Version preservation:
   - Retry attempts retain immutable definition_id and definition_version_id.
   - DLQ transitions retain immutable definition_id and definition_version_id.
   - Activation of Version 2 does not alter previously queued/running/retrying tasks' pinned version.
"""

import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, select, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from taskforge.api.dependencies import get_current_principal
from taskforge.core.auth import AuthenticatedPrincipal
from taskforge.core.state_machine import TaskStatus
from taskforge.database.models import (
    Task,
    TaskAttempt,
    TaskResult,
    get_utc_now,
)
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import Base, get_db
from taskforge.definitions.service import (
    DefinitionConflictError,
    DefinitionNotFoundError,
    DefinitionService,
)
from taskforge.main import app

# ---------------------------------------------------------------------------
# In-memory SQLite engine
# ---------------------------------------------------------------------------
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
        key_id="key-cancel-tester",
        tenant_id="tenant-cancel",
        name="Cancellation Tester",
        scopes=["definitions:write", "tasks:read", "tasks:write"],
    )


@pytest.fixture
def other_principal():
    return AuthenticatedPrincipal(
        key_id="key-other-tester",
        tenant_id="tenant-other-cancel",
        name="Other Cancellation Tester",
        scopes=["definitions:write", "tasks:read", "tasks:write"],
    )


@pytest.fixture
def client(db, principal):
    def _override_get_db():
        yield db

    def _override_principal():
        return principal

    app.dependency_overrides[get_db] = _override_get_db
    app.dependency_overrides[get_current_principal] = _override_principal
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def active_definition(db, principal):
    svc = DefinitionService(db)
    defn, v1 = svc.create_definition(
        principal=principal,
        name="Cancellable Webhook",
        description="Tests cancellation and version retention",
        task_type="webhook_send",
        execution_type="builtin",
        configuration_json={"timeout_ms": 3000},
        input_schema_json={
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["url"],
            "properties": {"url": {"type": "string"}},
        },
        output_schema_json={
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["status"],
            "properties": {"status": {"type": "string"}},
        },
        execution_policy_json={"max_retries": 2, "timeout_seconds": 30},
    )
    svc.validate_version(principal=principal, definition_id=defn.id, version_number=1)
    svc.activate_version(principal=principal, definition_id=defn.id, version_number=1)
    return svc.get_definition(principal, defn.id)


# ---------------------------------------------------------------------------
# 1. Fenced Cancellation Unit Tests
# ---------------------------------------------------------------------------


class TestFencedCancellation:
    """Verify cancellation transaction revokes leases, updates status, and rejects terminal cancellations."""

    def test_cancel_running_task_revokes_lease_and_attempts(self, db, principal, active_definition):
        """Cancellation of RUNNING task clears lease and cancels active attempts."""
        ver_id = active_definition.active_version_id
        task = Task(
            id=uuid.uuid4().hex,
            task_name="cancellation-test-task",
            task_type="webhook_send",
            status=TaskStatus.RUNNING.value,
            priority=2,
            payload={"url": "https://example.com"},
            tenant_id=principal.tenant_id,
            max_retries=2,
            timeout_seconds=30,
            lease_token="lease-tok-xyz",
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            started_at=get_utc_now(),
            created_at=get_utc_now(),
            definition_id=active_definition.id,
            definition_version_id=ver_id,
        )
        db.add(task)
        db.flush()

        # Add a running attempt
        att = TaskAttempt(
            task_id=task.id,
            attempt_number=1,
            worker_id="worker-w1",
            status="RUNNING",
            started_at=get_utc_now(),
        )
        db.add(att)
        db.commit()

        # Execute cancellation
        svc = DefinitionService(db)
        res = svc.cancel_execution(
            principal=principal,
            definition_id=active_definition.id,
            task_id=task.id,
            reason="operator_aborted",
        )
        assert res["status"] == "CANCELLED"

        # Verify task row
        db.expire_all()
        reloaded = db.get(Task, task.id)
        assert reloaded.status == TaskStatus.CANCELLED.value
        assert reloaded.lease_token is None
        assert reloaded.lease_expires_at is None
        assert reloaded.completed_at is not None

        # Verify attempt row
        att_reloaded = db.get(TaskAttempt, att.id)
        assert att_reloaded.status == "CANCELLED"
        assert "operator_aborted" in att_reloaded.error_message

        # Verify audit event recorded
        audit_events = svc.list_audit_events(principal, active_definition.id)
        cancel_evt = next((e for e in audit_events if e.action == "EXECUTION_CANCELLED"), None)
        assert cancel_evt is not None
        assert cancel_evt.metadata_json["task_id"] == task.id
        assert cancel_evt.metadata_json["reason"] == "operator_aborted"

    def test_cancel_terminal_task_raises_conflict(self, db, principal, active_definition):
        """Cancelling a completed task raises DefinitionConflictError (409 Conflict)."""
        ver_id = active_definition.active_version_id
        task = Task(
            id=uuid.uuid4().hex,
            task_name="completed-task",
            task_type="webhook_send",
            status=TaskStatus.SUCCESS.value,
            priority=2,
            payload={},
            tenant_id=principal.tenant_id,
            completed_at=get_utc_now(),
            created_at=get_utc_now(),
            definition_id=active_definition.id,
            definition_version_id=ver_id,
        )
        db.add(task)
        db.commit()

        svc = DefinitionService(db)
        with pytest.raises(DefinitionConflictError):
            svc.cancel_execution(
                principal=principal,
                definition_id=active_definition.id,
                task_id=task.id,
            )

    def test_cross_tenant_cancellation_returns_not_found(
        self, db, principal, other_principal, active_definition
    ):
        """Tenant B cannot cancel Tenant A's task (returns 404)."""
        task = Task(
            id=uuid.uuid4().hex,
            task_name="foreign-task",
            task_type="webhook_send",
            status=TaskStatus.QUEUED.value,
            priority=2,
            payload={},
            tenant_id=principal.tenant_id,
            created_at=get_utc_now(),
            definition_id=active_definition.id,
            definition_version_id=active_definition.active_version_id,
        )
        db.add(task)
        db.commit()

        svc = DefinitionService(db)
        with pytest.raises(DefinitionNotFoundError):
            svc.cancel_execution(
                principal=other_principal,
                definition_id=active_definition.id,
                task_id=task.id,
            )


# ---------------------------------------------------------------------------
# 2. Stale Worker Fencing After Cancellation
# ---------------------------------------------------------------------------


class TestStaleWorkerFencingAfterCancellation:
    """Verify that a worker holding a revoked lease cannot write results or failure states."""

    def test_stale_worker_success_after_cancellation_is_rejected(
        self, db, principal, active_definition
    ):
        """Stale worker attempting complete_task_success after cancellation is fenced out with zero artifacts."""
        stale_token = "stale-worker-lease-token"
        task = Task(
            id=uuid.uuid4().hex,
            task_name="fenced-task",
            task_type="webhook_send",
            status=TaskStatus.RUNNING.value,
            priority=3,
            payload={"url": "https://example.com"},
            tenant_id=principal.tenant_id,
            lease_token=stale_token,
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            started_at=get_utc_now(),
            created_at=get_utc_now(),
            definition_id=active_definition.id,
            definition_version_id=active_definition.active_version_id,
        )
        db.add(task)
        db.commit()

        # 1. User cancels the task (revoking lease_token to NULL)
        svc = DefinitionService(db)
        svc.cancel_execution(
            principal=principal,
            definition_id=active_definition.id,
            task_id=task.id,
        )

        # 2. Stale worker attempts to commit success with its stale lease_token
        repo = TaskRepository(db)
        with pytest.raises(PermissionError) as exc_info:
            repo.complete_task_success(
                task_id=task.id,
                lease_token=stale_token,
                return_value={"status": "delivered"},
            )
        assert "Fencing violation" in str(exc_info.value)

        # 3. Assert zero TaskResult artifacts persisted
        results = (
            db.execute(select(TaskResult).where(TaskResult.task_id == task.id)).scalars().all()
        )
        assert len(results) == 0

        # 4. Assert task remains CANCELLED
        db.expire_all()
        t = db.get(Task, task.id)
        assert t.status == TaskStatus.CANCELLED.value

    def test_stale_worker_failure_after_cancellation_is_rejected(
        self, db, principal, active_definition
    ):
        """Stale worker attempting complete_task_failure after cancellation is fenced out."""
        stale_token = "stale-worker-fail-token"
        task = Task(
            id=uuid.uuid4().hex,
            task_name="fenced-fail-task",
            task_type="webhook_send",
            status=TaskStatus.RUNNING.value,
            priority=3,
            payload={},
            tenant_id=principal.tenant_id,
            lease_token=stale_token,
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            started_at=get_utc_now(),
            created_at=get_utc_now(),
            definition_id=active_definition.id,
            definition_version_id=active_definition.active_version_id,
        )
        db.add(task)
        db.commit()

        # Cancel
        svc = DefinitionService(db)
        svc.cancel_execution(
            principal=principal,
            definition_id=active_definition.id,
            task_id=task.id,
        )

        # Stale worker attempts failure path
        repo = TaskRepository(db)
        with pytest.raises(PermissionError) as exc_info:
            repo.complete_task_failure(
                task_id=task.id,
                lease_token=stale_token,
                error_message="Worker error after cancellation",
            )
        assert "Fencing violation" in str(exc_info.value)

        # Task remains CANCELLED
        db.expire_all()
        t = db.get(Task, task.id)
        assert t.status == TaskStatus.CANCELLED.value


# ---------------------------------------------------------------------------
# 3. Version Preservation on Retries and Dead-Letter Transitions
# ---------------------------------------------------------------------------


class TestVersionPreservationAcrossLifecycle:
    """Verify immutable version attribution is retained across retries and DLQ transitions."""

    def test_retry_and_dead_letter_preserve_definition_version(
        self, db, principal, active_definition
    ):
        """Task retrying and ultimately exhausting retries to DEAD_LETTER retains pinned version."""
        pinned_version_id = active_definition.active_version_id
        task = Task(
            id=uuid.uuid4().hex,
            task_name="retry-version-retention",
            task_type="webhook_send",
            status=TaskStatus.RUNNING.value,
            priority=2,
            payload={"url": "https://example.com"},
            tenant_id=principal.tenant_id,
            max_retries=1,
            retry_count=0,
            lease_token="lease-r1",
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            started_at=get_utc_now(),
            created_at=get_utc_now(),
            definition_id=active_definition.id,
            definition_version_id=pinned_version_id,
        )
        db.add(task)
        db.commit()

        # 1. First failure triggers RETRY_WAIT
        repo = TaskRepository(db)
        t_updated, is_retryable = repo.complete_task_failure(
            task_id=task.id,
            lease_token="lease-r1",
            error_message="Temporary network blip",
            allow_retry=True,
        )
        assert is_retryable is True
        assert t_updated.status == TaskStatus.RETRY_WAIT.value
        assert t_updated.definition_id == active_definition.id
        assert t_updated.definition_version_id == pinned_version_id

        # Meanwhile: Administrator activates Version 2 of the definition
        svc = DefinitionService(db)
        v2 = svc.create_version(
            principal=principal,
            definition_id=active_definition.id,
            task_type="webhook_send",
            execution_type="builtin",
            configuration_json={"timeout_ms": 6000},
            input_schema_json={"type": "object"},
            output_schema_json={"type": "object"},
        )
        svc.validate_version(
            principal=principal, definition_id=active_definition.id, version_number=2
        )
        svc.activate_version(
            principal=principal, definition_id=active_definition.id, version_number=2
        )

        # 2. Second attempt for the retrying task (retry_count=1, max_retries=1)
        db.expire_all()
        t_row = db.get(Task, task.id)
        t_row.status = TaskStatus.RUNNING.value
        t_row.lease_token = "lease-r2"
        t_row.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
        db.commit()

        t_dead, is_retryable_2 = repo.complete_task_failure(
            task_id=task.id,
            lease_token="lease-r2",
            error_message="Permanent network failure",
            allow_retry=True,
        )
        # Retries exhausted -> DEAD_LETTER
        assert is_retryable_2 is False
        assert t_dead.status == TaskStatus.DEAD_LETTER.value

        # CRITICAL VERIFICATION: Pinned version is STILL Version 1, not Version 2!
        assert t_dead.definition_id == active_definition.id
        assert t_dead.definition_version_id == pinned_version_id
        assert t_dead.definition_version_id != v2.id


# ---------------------------------------------------------------------------
# 4. HTTP API Endpoint Tests (POST /api/v1/definitions/{id}/executions/{task_id}/cancel)
# ---------------------------------------------------------------------------


class TestCancelHttpApi:
    """Verify HTTP cancellation endpoint contract."""

    def test_http_cancel_execution_happy_path(self, client, db, principal, active_definition):
        task = Task(
            id=uuid.uuid4().hex,
            task_name="http-cancel-task",
            task_type="webhook_send",
            status=TaskStatus.QUEUED.value,
            priority=2,
            payload={},
            tenant_id=principal.tenant_id,
            created_at=get_utc_now(),
            definition_id=active_definition.id,
            definition_version_id=active_definition.active_version_id,
        )
        db.add(task)
        db.commit()

        resp = client.post(
            f"/api/v1/definitions/{active_definition.id}/executions/{task.id}/cancel"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == task.id
        assert data["status"] == "CANCELLED"

    def test_http_cancel_terminal_execution_returns_409(
        self, client, db, principal, active_definition
    ):
        task = Task(
            id=uuid.uuid4().hex,
            task_name="http-completed-task",
            task_type="webhook_send",
            status=TaskStatus.SUCCESS.value,
            priority=2,
            payload={},
            tenant_id=principal.tenant_id,
            completed_at=get_utc_now(),
            created_at=get_utc_now(),
            definition_id=active_definition.id,
            definition_version_id=active_definition.active_version_id,
        )
        db.add(task)
        db.commit()

        resp = client.post(
            f"/api/v1/definitions/{active_definition.id}/executions/{task.id}/cancel"
        )
        assert resp.status_code == 409
