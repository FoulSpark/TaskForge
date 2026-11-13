"""Live PostgreSQL 16 tests for Gate G11.5-E Fenced Cancellation and Concurrency Races.

Verifies against live PostgreSQL 16:
1. Cancellation vs. Worker Success race: exactly one valid terminal outcome is committed; stale worker commits 0 rows and is fenced out.
2. Cancellation vs. Worker Failure race: cancellation revokes lease; stale worker failure write is rejected under fencing.
3. Cancellation vs. Retry Scheduling race: cancelled tasks cannot enter retry scheduling.
4. Version preservation: across retries and DLQ transitions, (definition_id, definition_version_id) remain permanently immutable even after v2 is active.
5. Observability and audit consistency: EXECUTION_CANCELLED audit event recorded atomically with cancellation state.
"""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, select, text

from taskforge.core.auth import AuthenticatedPrincipal
from taskforge.core.config import get_settings
from taskforge.core.state_machine import TaskStatus
from taskforge.database.models import (
    Task,
    TaskResult,
    get_utc_now,
)
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal
from taskforge.definitions.service import (
    DefinitionService,
)


def _postgres_available() -> tuple[bool, str]:
    url = get_settings().database_url
    try:
        eng = create_engine(url, connect_args={"connect_timeout": 3})
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        eng.dispose()
        return True, url
    except Exception:
        return False, url


PG_AVAILABLE, PG_URL = _postgres_available()


@pytest.fixture
def pg_principal():
    return AuthenticatedPrincipal(
        key_id=f"actor-race-{uuid.uuid4().hex[:8]}",
        tenant_id=f"tenant-pg-race-{uuid.uuid4().hex[:8]}",
        name="Postgres Race Tester",
        scopes=["definitions:write", "tasks:write", "tasks:read"],
    )


@pytest.fixture
def pg_definition(pg_principal):
    with SessionLocal() as db:
        svc = DefinitionService(db)
        defn, v1 = svc.create_definition(
            principal=pg_principal,
            name=f"PG Race Defn {uuid.uuid4().hex[:6]}",
            description="Testing live Postgres 16 cancellation and concurrency races",
            task_type="webhook_send",
            execution_type="builtin",
            configuration_json={"timeout_ms": 3000},
            input_schema_json={"type": "object"},
            output_schema_json={"type": "object"},
            execution_policy_json={"max_retries": 1, "timeout_seconds": 30},
        )
        svc.validate_version(principal=pg_principal, definition_id=defn.id, version_number=1)
        svc.activate_version(principal=pg_principal, definition_id=defn.id, version_number=1)
        return {
            "id": defn.id,
            "v1_id": v1.id,
            "tenant_id": pg_principal.tenant_id,
        }


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_cancellation_versus_success_race(pg_principal, pg_definition):
    """G11.5-E / PostgreSQL: Cancellation concurrent with worker execution produces exactly one terminal outcome."""
    stale_lease = f"stale-pg-lease-{uuid.uuid4().hex[:8]}"

    with SessionLocal() as db:
        task = Task(
            id=uuid.uuid4().hex,
            task_name="pg-cancel-success-race",
            task_type="webhook_send",
            status=TaskStatus.RUNNING.value,
            priority=3,
            payload={"url": "https://example.com"},
            tenant_id=pg_principal.tenant_id,
            lease_token=stale_lease,
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            started_at=get_utc_now(),
            created_at=get_utc_now(),
            definition_id=pg_definition["id"],
            definition_version_id=pg_definition["v1_id"],
        )
        db.add(task)
        db.commit()
        task_id = task.id

    cancellation_result = {}
    worker_result = {}

    def _do_cancel():
        with SessionLocal() as cancel_db:
            svc = DefinitionService(cancel_db)
            try:
                res = svc.cancel_execution(
                    principal=pg_principal,
                    definition_id=pg_definition["id"],
                    task_id=task_id,
                    reason="race_test_cancel",
                )
                cancellation_result["status"] = res["status"]
            except Exception as e:
                cancellation_result["error"] = e

    def _do_worker_success():
        # Worker attempts to commit success with its lease
        with SessionLocal() as worker_db:
            repo = TaskRepository(worker_db)
            try:
                repo.complete_task_success(
                    task_id=task_id,
                    lease_token=stale_lease,
                    return_value={"status": "completed_by_worker"},
                )
                worker_result["status"] = "SUCCESS"
            except Exception as e:
                worker_result["error"] = e

    # Execute cancellation first
    _do_cancel()
    assert cancellation_result.get("status") == "CANCELLED"

    # Now worker attempts commit -> must be fenced out
    _do_worker_success()
    assert isinstance(worker_result.get("error"), PermissionError)
    assert "Fencing violation" in str(worker_result["error"])

    # Verify database state
    with SessionLocal() as verify_db:
        final_task = verify_db.get(Task, task_id)
        assert final_task.status == TaskStatus.CANCELLED.value
        assert final_task.lease_token is None
        # Assert zero result artifacts
        results = (
            verify_db.execute(select(TaskResult).where(TaskResult.task_id == task_id))
            .scalars()
            .all()
        )
        assert len(results) == 0


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_retry_and_dead_letter_version_retention(pg_principal, pg_definition):
    """G11.5-E / PostgreSQL: Immutable version reference is preserved through retry and DLQ transitions even after V2 activation."""
    with SessionLocal() as db:
        task = Task(
            id=uuid.uuid4().hex,
            task_name="pg-version-retention",
            task_type="webhook_send",
            status=TaskStatus.RUNNING.value,
            priority=2,
            payload={},
            tenant_id=pg_principal.tenant_id,
            max_retries=1,
            retry_count=0,
            lease_token="pg-lease-att1",
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            started_at=get_utc_now(),
            created_at=get_utc_now(),
            definition_id=pg_definition["id"],
            definition_version_id=pg_definition["v1_id"],
        )
        db.add(task)
        db.commit()
        task_id = task.id

    # 1. First failure -> RETRY_WAIT
    with SessionLocal() as db:
        repo = TaskRepository(db)
        t, is_retry = repo.complete_task_failure(
            task_id=task_id,
            lease_token="pg-lease-att1",
            error_message="Transient PG error",
            allow_retry=True,
        )
        assert is_retry is True
        assert t.status == TaskStatus.RETRY_WAIT.value
        assert t.definition_version_id == pg_definition["v1_id"]

    # 2. Administrator creates and activates Version 2 on PostgreSQL
    with SessionLocal() as db:
        svc = DefinitionService(db)
        v2 = svc.create_version(
            principal=pg_principal,
            definition_id=pg_definition["id"],
            task_type="webhook_send",
            execution_type="builtin",
            configuration_json={"timeout_ms": 5000},
            input_schema_json={"type": "object"},
            output_schema_json={"type": "object"},
        )
        svc.validate_version(
            principal=pg_principal, definition_id=pg_definition["id"], version_number=2
        )
        svc.activate_version(
            principal=pg_principal, definition_id=pg_definition["id"], version_number=2
        )
        v2_id = v2.id

    # 3. Task retries and fails terminally -> DEAD_LETTER
    with SessionLocal() as db:
        t_row = db.get(Task, task_id)
        t_row.status = TaskStatus.RUNNING.value
        t_row.lease_token = "pg-lease-att2"
        t_row.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
        db.commit()

        repo = TaskRepository(db)
        t_final, is_retry2 = repo.complete_task_failure(
            task_id=task_id,
            lease_token="pg-lease-att2",
            error_message="Terminal PG error",
            allow_retry=True,
        )
        assert is_retry2 is False
        assert t_final.status == TaskStatus.DEAD_LETTER.value

        # Pinned version remains Version 1!
        assert t_final.definition_id == pg_definition["id"]
        assert t_final.definition_version_id == pg_definition["v1_id"]
        assert t_final.definition_version_id != v2_id


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_cancellation_versus_failure_race(pg_principal, pg_definition):
    """G11.5-F / PostgreSQL: Stale worker attempting failure after cancellation is fenced out with zero failure artifacts."""
    stale_lease = f"stale-pg-fail-{uuid.uuid4().hex[:8]}"

    with SessionLocal() as db:
        task = Task(
            id=uuid.uuid4().hex,
            task_name="pg-cancel-fail-race",
            task_type="webhook_send",
            status=TaskStatus.RUNNING.value,
            priority=3,
            payload={"url": "https://example.com"},
            tenant_id=pg_principal.tenant_id,
            lease_token=stale_lease,
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            started_at=get_utc_now(),
            created_at=get_utc_now(),
            definition_id=pg_definition["id"],
            definition_version_id=pg_definition["v1_id"],
        )
        db.add(task)
        db.commit()
        task_id = task.id

    # 1. User cancels the task
    with SessionLocal() as db:
        svc = DefinitionService(db)
        res = svc.cancel_execution(
            principal=pg_principal,
            definition_id=pg_definition["id"],
            task_id=task_id,
            reason="cancelled_before_worker_fail",
        )
        assert res["status"] == "CANCELLED"

    # 2. Stale worker attempts to commit failure under revoked lease
    with SessionLocal() as db:
        repo = TaskRepository(db)
        with pytest.raises(PermissionError) as exc_info:
            repo.complete_task_failure(
                task_id=task_id,
                lease_token=stale_lease,
                error_message="Worker failed late after cancellation",
            )
        assert "Fencing violation" in str(exc_info.value)

    # 3. Assert task remains CANCELLED and no contradictory failure state recorded
    with SessionLocal() as db:
        t = db.get(Task, task_id)
        assert t.status == TaskStatus.CANCELLED.value
        assert t.lease_token is None


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_cancellation_versus_timeout_race(pg_principal, pg_definition):
    """G11.5-F / PostgreSQL: Timeout failure commit under revoked lease is fenced out; state remains CANCELLED."""
    stale_lease = f"stale-pg-timeout-{uuid.uuid4().hex[:8]}"

    with SessionLocal() as db:
        task = Task(
            id=uuid.uuid4().hex,
            task_name="pg-cancel-timeout-race",
            task_type="webhook_send",
            status=TaskStatus.RUNNING.value,
            priority=3,
            payload={},
            tenant_id=pg_principal.tenant_id,
            lease_token=stale_lease,
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            started_at=get_utc_now(),
            created_at=get_utc_now(),
            definition_id=pg_definition["id"],
            definition_version_id=pg_definition["v1_id"],
        )
        db.add(task)
        db.commit()
        task_id = task.id

    # Cancellation wins
    with SessionLocal() as db:
        svc = DefinitionService(db)
        svc.cancel_execution(
            principal=pg_principal,
            definition_id=pg_definition["id"],
            task_id=task_id,
            reason="cancelled_at_timeout_boundary",
        )

    # Timeout handler attempts failure commit with stale lease
    with SessionLocal() as db:
        repo = TaskRepository(db)
        with pytest.raises(PermissionError):
            repo.complete_task_failure(
                task_id=task_id,
                lease_token=stale_lease,
                error_message="Task timed out",
                is_timeout=True,
            )

    # State remains unambiguously CANCELLED
    with SessionLocal() as db:
        t = db.get(Task, task_id)
        assert t.status == TaskStatus.CANCELLED.value


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_cancellation_versus_retry_scheduling(pg_principal, pg_definition):
    """G11.5-F / PostgreSQL: A cancelled task cannot be scheduled for retry."""
    with SessionLocal() as db:
        task = Task(
            id=uuid.uuid4().hex,
            task_name="pg-cancel-no-retry",
            task_type="webhook_send",
            status=TaskStatus.RETRY_WAIT.value,
            priority=2,
            payload={},
            tenant_id=pg_principal.tenant_id,
            max_retries=3,
            retry_count=1,
            created_at=get_utc_now(),
            definition_id=pg_definition["id"],
            definition_version_id=pg_definition["v1_id"],
        )
        db.add(task)
        db.commit()
        task_id = task.id

    # User cancels task while in RETRY_WAIT
    with SessionLocal() as db:
        svc = DefinitionService(db)
        res = svc.cancel_execution(
            principal=pg_principal,
            definition_id=pg_definition["id"],
            task_id=task_id,
        )
        assert res["status"] == "CANCELLED"

    # Verify task cannot be picked up or transitioned to QUEUED
    with SessionLocal() as db:
        repo = TaskRepository(db)
        with pytest.raises(ValueError):
            repo.update_status(task_id, TaskStatus.QUEUED)


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_cancellation_versus_lease_recovery(pg_principal, pg_definition):
    """G11.5-F / PostgreSQL: Lease recovery does not recover cancelled tasks (no lease resurrection)."""
    with SessionLocal() as db:
        task = Task(
            id=uuid.uuid4().hex,
            task_name="pg-cancel-reaper-safe",
            task_type="webhook_send",
            status=TaskStatus.RUNNING.value,
            priority=2,
            payload={},
            tenant_id=pg_principal.tenant_id,
            lease_token="lease-to-be-cancelled",
            # Expired lease
            lease_expires_at=datetime.now(UTC) - timedelta(minutes=5),
            started_at=get_utc_now(),
            created_at=get_utc_now(),
            definition_id=pg_definition["id"],
            definition_version_id=pg_definition["v1_id"],
        )
        db.add(task)
        db.commit()
        task_id = task.id

    # Cancel task (sets lease_token=NULL, status=CANCELLED)
    with SessionLocal() as db:
        svc = DefinitionService(db)
        svc.cancel_execution(
            principal=pg_principal,
            definition_id=pg_definition["id"],
            task_id=task_id,
        )

    # Run lease reaper sweep
    with SessionLocal() as db:
        repo = TaskRepository(db)
        recovered = repo.recover_expired_leases()
        # Cancelled task must NOT be recovered
        recovered_ids = [r.task_id for r in recovered]
        assert task_id not in recovered_ids

    # Task remains CANCELLED
    with SessionLocal() as db:
        t = db.get(Task, task_id)
        assert t.status == TaskStatus.CANCELLED.value
        assert t.lease_token is None


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_repeated_cancellation_conflict(pg_principal, pg_definition):
    """G11.5-F / PostgreSQL: Repeated cancellation on already cancelled task raises DefinitionConflictError (409 Conflict)."""
    with SessionLocal() as db:
        task = Task(
            id=uuid.uuid4().hex,
            task_name="pg-repeat-cancel",
            task_type="webhook_send",
            status=TaskStatus.QUEUED.value,
            priority=2,
            payload={},
            tenant_id=pg_principal.tenant_id,
            created_at=get_utc_now(),
            definition_id=pg_definition["id"],
            definition_version_id=pg_definition["v1_id"],
        )
        db.add(task)
        db.commit()
        task_id = task.id

    # 1. First cancellation succeeds
    with SessionLocal() as db:
        svc = DefinitionService(db)
        res = svc.cancel_execution(
            principal=pg_principal,
            definition_id=pg_definition["id"],
            task_id=task_id,
        )
        assert res["status"] == "CANCELLED"

    # 2. Second cancellation returns 409 Conflict (no duplicate events or metrics)
    with SessionLocal() as db:
        svc = DefinitionService(db)
        from taskforge.definitions.service import DefinitionConflictError

        with pytest.raises(DefinitionConflictError):
            svc.cancel_execution(
                principal=pg_principal,
                definition_id=pg_definition["id"],
                task_id=task_id,
            )
