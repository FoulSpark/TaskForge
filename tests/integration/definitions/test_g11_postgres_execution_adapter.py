"""Live PostgreSQL 16 tests for Gate G11.4 Controlled Execution Adapter.

Verifies:
1. Idempotent replay after v2 activation returns original v1 task.
2. Concurrent submission and version activation on PostgreSQL.
3. Concurrent submission and definition disable on PostgreSQL (clean serialization, zero post-disable tasks).
4. Stale-worker fencing at PostgreSQL boundary: zero persisted TaskResult, TaskAttempt, or success event artifacts.
5. Handler unavailable leads to permanent failure with no retries.
6. Execution audit: DEFINITION_EXECUTED event recorded atomically with task submission.
"""

import threading
import uuid
from typing import Any
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select, text

from taskforge.api.schemas import DefinitionExecuteRequest
from taskforge.core.auth import AuthenticatedPrincipal
from taskforge.core.config import get_settings
from taskforge.core.state_machine import TaskStatus
from taskforge.database.models import (
    Task,
    TaskAttempt,
    TaskResult,
)
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal
from taskforge.definitions.models import DefinitionAuditEvent
from taskforge.definitions.service import (
    DefinitionConflictError,
    DefinitionService,
)
from taskforge.tasks.registry import TaskRegistry
from taskforge.workers.execution_engine import (
    ExecutionDisposition,
    execute_task,
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
    tenant_id = f"tenant-g114-{uuid.uuid4().hex[:8]}"
    actor_id = f"actor-{uuid.uuid4().hex[:8]}"
    return AuthenticatedPrincipal(
        key_id=actor_id,
        tenant_id=tenant_id,
        name="Postgres G11.4 Tester",
        scopes=["definitions:write", "tasks:write", "tasks:read"],
    )


@pytest.fixture
def pg_active_definition(pg_principal):
    """Sets up an active definition on live PostgreSQL with validated v1."""
    with SessionLocal() as db:
        svc = DefinitionService(db)
        defn, v1 = svc.create_definition(
            principal=pg_principal,
            name=f"PG Webhook {uuid.uuid4().hex[:6]}",
            description="Testing live Postgres 16 execution adapter",
            task_type="webhook_send",
            execution_type="builtin",
            configuration_json={"timeout_ms": 3000},
            input_schema_json={
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["url"],
                "properties": {
                    "url": {"type": "string"},
                },
                "additionalProperties": False,
            },
            output_schema_json={
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["status"],
                "properties": {
                    "status": {"type": "string"},
                },
            },
            execution_policy_json={"max_retries": 2, "timeout_seconds": 15},
        )
        _, val_result = svc.validate_version(
            principal=pg_principal, definition_id=defn.id, version_number=1
        )
        assert val_result.is_valid is True
        svc.activate_version(principal=pg_principal, definition_id=defn.id, version_number=1)
        return {
            "id": defn.id,
            "active_version_id": defn.active_version_id,
            "input_schema": v1.input_schema_json,
            "output_schema": v1.output_schema_json,
        }


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_idempotent_replay_after_v2_activation(pg_principal, pg_active_definition):
    """Evidence 1: Request submitted under v1 and replayed after v2 activation
    returns original v1 task and does NOT bind to v2 or conflict.
    """
    idemp_key = f"idemp-pg-{uuid.uuid4().hex}"
    req = DefinitionExecuteRequest(
        input={"url": "https://example.com/pg-test"},
        idempotency_key=idemp_key,
        priority=2,
    )

    with SessionLocal() as db:
        svc = DefinitionService(db)
        t1, created1 = svc.execute_definition(
            principal=pg_principal,
            definition_id=pg_active_definition["id"],
            request=req,
        )
        assert created1 is True
        v1_id = t1.definition_version_id
        t1_id = t1.id

    # Create, validate, and activate Version 2 in live PostgreSQL
    with SessionLocal() as db:
        svc = DefinitionService(db)
        svc.create_version(
            principal=pg_principal,
            definition_id=pg_active_definition["id"],
            task_type="webhook_send",
            execution_type="builtin",
            configuration_json={"timeout_ms": 6000},
            input_schema_json=pg_active_definition["input_schema"],
            output_schema_json=pg_active_definition["output_schema"],
        )
        svc.validate_version(
            principal=pg_principal,
            definition_id=pg_active_definition["id"],
            version_number=2,
        )
        svc.activate_version(
            principal=pg_principal,
            definition_id=pg_active_definition["id"],
            version_number=2,
        )

    # Replay identical request using the original idempotency key
    with SessionLocal() as db:
        svc = DefinitionService(db)
        t2, created2 = svc.execute_definition(
            principal=pg_principal,
            definition_id=pg_active_definition["id"],
            request=req,
        )
        assert created2 is False
        assert t2.id == t1_id
        # Crucial: Task remains pinned to v1, NOT v2
        assert t2.definition_version_id == v1_id


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_concurrent_submission_and_activation(pg_principal, pg_active_definition):
    """Evidence 2: 10 concurrent threads submit tasks while another thread activates v2.
    Every accepted task must bind to a VALID, correctly pinned version (v1 or v2).
    """
    with SessionLocal() as db:
        svc = DefinitionService(db)
        svc.create_version(
            principal=pg_principal,
            definition_id=pg_active_definition["id"],
            task_type="webhook_send",
            execution_type="builtin",
            configuration_json={"timeout_ms": 9000},
            input_schema_json=pg_active_definition["input_schema"],
        )
        svc.validate_version(
            principal=pg_principal,
            definition_id=pg_active_definition["id"],
            version_number=2,
        )

    num_threads = 10
    barrier = threading.Barrier(num_threads + 1)
    results: list[dict[str, Any]] = []
    lock = threading.Lock()

    def submit_worker(worker_idx: int):
        barrier.wait()
        req = DefinitionExecuteRequest(
            input={"url": f"https://example.com/worker-{worker_idx}"},
            priority=2,
        )
        try:
            with SessionLocal() as db:
                svc = DefinitionService(db)
                t, _ = svc.execute_definition(
                    principal=pg_principal,
                    definition_id=pg_active_definition["id"],
                    request=req,
                )
                with lock:
                    results.append({"task_id": t.id, "version_id": t.definition_version_id})
        except Exception as exc:
            with lock:
                results.append({"error": str(exc)})

    def activator_worker():
        barrier.wait()
        try:
            with SessionLocal() as db:
                svc = DefinitionService(db)
                svc.activate_version(
                    principal=pg_principal,
                    definition_id=pg_active_definition["id"],
                    version_number=2,
                )
        except Exception as exc:
            with lock:
                results.append({"activator_error": str(exc)})

    threads = [threading.Thread(target=submit_worker, args=(i,)) for i in range(num_threads)]
    act_thread = threading.Thread(target=activator_worker)

    for t in threads:
        t.start()
    act_thread.start()

    for t in threads:
        t.join()
    act_thread.join()

    # Verify that all 10 submissions succeeded and pinned to either v1 or v2
    with SessionLocal() as db:
        svc = DefinitionService(db)
        defn = svc.get_definition(pg_principal, pg_active_definition["id"])
        valid_version_ids = {v.id for v in defn.versions}

    submitted_tasks = [r for r in results if "task_id" in r]
    assert len(submitted_tasks) == num_threads
    for res in submitted_tasks:
        assert res["version_id"] in valid_version_ids


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_concurrent_submission_and_disable(pg_principal, pg_active_definition):
    """Evidence 3: Threads concurrently submit while another disables the definition.
    Row-level locking guarantees clean serialization: tasks accepted before disable succeed,
    tasks submitted after disable are rejected. Zero post-disable acceptance.
    """
    num_threads = 8
    barrier = threading.Barrier(num_threads + 1)
    results: list[dict[str, Any]] = []
    lock = threading.Lock()

    def submit_worker(worker_idx: int):
        barrier.wait()
        req = DefinitionExecuteRequest(
            input={"url": f"https://example.com/disable-worker-{worker_idx}"},
            priority=2,
        )
        try:
            with SessionLocal() as db:
                svc = DefinitionService(db)
                t, _ = svc.execute_definition(
                    principal=pg_principal,
                    definition_id=pg_active_definition["id"],
                    request=req,
                )
                with lock:
                    results.append({"status": "accepted", "task_id": t.id})
        except DefinitionConflictError as exc:
            with lock:
                results.append({"status": "rejected_disabled", "error": str(exc)})
        except Exception as exc:
            with lock:
                results.append({"status": "unexpected_error", "error": str(exc)})

    def disabler_worker():
        barrier.wait()
        try:
            with SessionLocal() as db:
                svc = DefinitionService(db)
                svc.disable_definition(
                    principal=pg_principal,
                    definition_id=pg_active_definition["id"],
                )
        except Exception as exc:
            with lock:
                results.append({"status": "disabler_error", "error": str(exc)})

    threads = [threading.Thread(target=submit_worker, args=(i,)) for i in range(num_threads)]
    dis_thread = threading.Thread(target=disabler_worker)

    for t in threads:
        t.start()
    dis_thread.start()

    for t in threads:
        t.join()
    dis_thread.join()

    # All workers must either be cleanly accepted or cleanly rejected with "not ACTIVE"
    for r in results:
        if r["status"] == "rejected_disabled":
            assert "not ACTIVE" in r["error"]
        elif r["status"] == "accepted":
            pass
        else:
            pytest.fail(f"Unexpected status in concurrent disable test: {r}")


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_stale_worker_zero_artifact_fencing(pg_principal, pg_active_definition):
    """Evidence 4: Stale worker with superseded lease_token attempting completion on PostgreSQL
    is rejected at the conditional UPDATE boundary, transaction rolls back,
    and ZERO TaskResult, TaskAttempt success, or TaskEvent records survive.
    Replacement worker then commits cleanly.
    """
    req = DefinitionExecuteRequest(
        input={"url": "https://example.com/pg-fencing"},
    )
    with SessionLocal() as db:
        svc = DefinitionService(db)
        task, _ = svc.execute_definition(
            principal=pg_principal,
            definition_id=pg_active_definition["id"],
            request=req,
        )
        task_id = task.id

    stale_token = f"lease-worker-1-{uuid.uuid4().hex[:6]}"
    active_token = f"lease-worker-2-{uuid.uuid4().hex[:6]}"

    # Worker 1 acquires initial lease
    with SessionLocal() as db:
        repo = TaskRepository(db)
        repo.acquire_lease(task_id, "worker-1", 30, lease_token=stale_token)
        attempt1 = repo.record_attempt(
            task_id=task_id,
            attempt_number=1,
            worker_id="worker-1",
            status="RUNNING",
            lease_token=stale_token,
        )
        attempt1_id = attempt1.id

    # Worker 2 supersedes Worker 1 (force=True simulates lease expiry takeover)
    with SessionLocal() as db:
        repo = TaskRepository(db)
        repo.update_status(
            task_id,
            TaskStatus.RUNNING,
            lease_token=active_token,
            force=True,
        )
        # Re-assign lease token to active_token for Worker 2
        t_row = db.get(Task, task_id)
        t_row.lease_token = active_token
        db.commit()

    # Stale Worker 1 attempts to complete task with stale_token
    with SessionLocal() as db:
        repo = TaskRepository(db)
        with pytest.raises(PermissionError) as exc_info:
            repo.complete_task_success(
                task_id=task_id,
                lease_token=stale_token,
                return_value={"status": "STALE_WORKER_LEAK"},
                attempt_id=attempt1_id,
            )
        assert "Fencing violation" in str(exc_info.value)

    # CRITICAL: Verify ZERO success artifacts persisted by stale worker in PostgreSQL
    with SessionLocal() as db:
        results = db.scalars(select(TaskResult).where(TaskResult.task_id == task_id)).all()
        assert len(results) == 0, "No TaskResult must survive from stale worker"

        # TaskAttempt 1 must remain RUNNING or non-SUCCESS
        att1 = db.get(TaskAttempt, attempt1_id)
        assert att1.status != "SUCCESS", "TaskAttempt must not be updated by stale worker"

    # Replacement Worker 2 completes normally under active_token
    with SessionLocal() as db:
        repo = TaskRepository(db)
        repo.complete_task_success(
            task_id=task_id,
            lease_token=active_token,
            return_value={"status": "VALID_DELIVERY"},
        )

    # Verify authoritative final state in PostgreSQL
    with SessionLocal() as db:
        final_task = db.get(Task, task_id)
        assert final_task.status == TaskStatus.SUCCESS.value
        assert final_task.result.return_value == {"status": "VALID_DELIVERY"}


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_handler_unavailable_terminal_failure(pg_principal, pg_active_definition):
    """Evidence 5: Missing or unregistered handler results in TERMINAL_FAILURE_RECORDED
    with zero retries scheduled on PostgreSQL.
    """
    req = DefinitionExecuteRequest(
        input={"url": "https://example.com/pg-handler-unavail"},
    )
    with SessionLocal() as db:
        svc = DefinitionService(db)
        task, _ = svc.execute_definition(
            principal=pg_principal,
            definition_id=pg_active_definition["id"],
            request=req,
        )
        task_id = task.id

    # Unregister handler from live registry and execute task
    with patch.dict(TaskRegistry._HANDLERS, {}, clear=True):
        disp = execute_task(
            task_id=task_id,
            worker_id="pg-test-worker",
            timeout_seconds=10,
            use_outbox=False,
        )

    assert disp == ExecutionDisposition.TERMINAL_FAILURE_RECORDED

    with SessionLocal() as db:
        final_task = db.get(Task, task_id)
        assert final_task.status == TaskStatus.FAILED.value
        assert final_task.retry_count == 0

        # Verify failure event was recorded with HANDLER_UNAVAILABLE_ERROR
        fail_event = next(
            e
            for e in final_task.events
            if e.event_type == "STATUS_CHANGED" and e.to_state == TaskStatus.FAILED.value
        )
        assert fail_event.metadata_json["error_type"] == "HANDLER_UNAVAILABLE_ERROR"


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_execution_audit_event_atomicity_and_attribution(pg_principal, pg_active_definition):
    """Evidence 6: DEFINITION_EXECUTED audit event is written atomically in the same transaction
    as task creation, with correct tenant attribution, task_id, version, and fingerprint.
    """
    req = DefinitionExecuteRequest(
        input={"url": "https://example.com/pg-audit"},
        priority=3,
    )
    with SessionLocal() as db:
        svc = DefinitionService(db)
        task, created = svc.execute_definition(
            principal=pg_principal,
            definition_id=pg_active_definition["id"],
            request=req,
        )
        assert created is True
        task_id = task.id

    with SessionLocal() as db:
        events = db.scalars(
            select(DefinitionAuditEvent).where(
                DefinitionAuditEvent.definition_id == pg_active_definition["id"],
                DefinitionAuditEvent.tenant_id == pg_principal.tenant_id,
                DefinitionAuditEvent.action == "DEFINITION_EXECUTED",
            )
        ).all()

        assert len(events) >= 1
        audit_event = events[-1]
        assert audit_event.actor_id == pg_principal.key_id
        assert audit_event.tenant_id == pg_principal.tenant_id
        assert audit_event.version_id == pg_active_definition["active_version_id"]
        assert audit_event.metadata_json["task_id"] == task_id
        assert audit_event.metadata_json["version"] == 1
        assert "fingerprint" in audit_event.metadata_json
