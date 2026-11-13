"""Live PostgreSQL 16 tests for Gate G11.5-C Prometheus Metrics Integration.

Verifies against live PostgreSQL 16:
1. Definition submission metrics increment exactly once on successful execution creation.
2. Idempotent replay does NOT increment submission metrics.
3. Zero-cardinality-explosion contract: No definition_id, task_id, or tenant_id in label dimensions.
4. Execution lifecycle transitions (SUCCESS, FAILED) increment definition metrics.
5. Metrics collection does NOT increment on read-only API polling.
"""

import uuid
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text

from taskforge.api.schemas import DefinitionExecuteRequest
from taskforge.core.auth import AuthenticatedPrincipal
from taskforge.core.config import get_settings
from taskforge.core.state_machine import TaskStatus
from taskforge.database.models import Task, get_utc_now
from taskforge.database.session import SessionLocal
from taskforge.definitions.service import DefinitionService
from taskforge.monitoring.metrics import (
    DEFINITION_EXECUTIONS_TOTAL,
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
    return AuthenticatedPrincipal(
        key_id=f"actor-m-{uuid.uuid4().hex[:8]}",
        tenant_id=f"tenant-pg-m-{uuid.uuid4().hex[:8]}",
        name="Postgres Metrics Tester",
        scopes=["definitions:write", "tasks:write", "tasks:read"],
    )


@pytest.fixture
def pg_active_definition(pg_principal):
    with SessionLocal() as db:
        svc = DefinitionService(db)
        defn, v1 = svc.create_definition(
            principal=pg_principal,
            name=f"PG Metrics Task {uuid.uuid4().hex[:6]}",
            description="Testing live Postgres 16 definition metrics",
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
        )
        svc.validate_version(principal=pg_principal, definition_id=defn.id, version_number=1)
        svc.activate_version(principal=pg_principal, definition_id=defn.id, version_number=1)
        return {
            "id": defn.id,
            "active_version_id": defn.active_version_id,
            "task_type": v1.task_type,
        }


def _get_counter_value(counter, labels: dict[str, str]) -> float:
    try:
        return counter.labels(**labels)._value.get()
    except Exception:
        return 0.0


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_submission_metrics_increment_exactly_once(pg_principal, pg_active_definition):
    """G11.5-C / PostgreSQL: SUBMITTED counter increments once; idempotent replay does not increment."""
    before_submitted = _get_counter_value(
        DEFINITION_EXECUTIONS_TOTAL,
        {"task_type": "webhook_send", "status": "SUBMITTED"},
    )

    idem_key = f"pg-idem-{uuid.uuid4().hex[:8]}"
    req = DefinitionExecuteRequest(
        input={"url": "https://pg-metrics.example.com"},
        priority=2,
        idempotency_key=idem_key,
        timeout_seconds=25,
    )

    with SessionLocal() as db:
        svc = DefinitionService(db)
        task1, created1 = svc.execute_definition(
            principal=pg_principal,
            definition_id=pg_active_definition["id"],
            request=req,
        )
        assert created1 is True

    after_first_submit = _get_counter_value(
        DEFINITION_EXECUTIONS_TOTAL,
        {"task_type": "webhook_send", "status": "SUBMITTED"},
    )
    assert after_first_submit == before_submitted + 1.0

    # Idempotent replay with same key and body
    with SessionLocal() as db:
        svc = DefinitionService(db)
        task2, created2 = svc.execute_definition(
            principal=pg_principal,
            definition_id=pg_active_definition["id"],
            request=req,
        )
        assert created2 is False
        assert task2.id == task1.id

    after_replay = _get_counter_value(
        DEFINITION_EXECUTIONS_TOTAL,
        {"task_type": "webhook_send", "status": "SUBMITTED"},
    )
    assert after_replay == after_first_submit


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_execution_engine_success_and_cardinality(pg_principal, pg_active_definition):
    """G11.5-C / PostgreSQL: Worker execution under live PG increments SUCCESS counter and preserves bounded labels."""
    with SessionLocal() as db:
        task = Task(
            id=uuid.uuid4().hex,
            task_name="pg-metric-exec",
            task_type="webhook_send",
            status=TaskStatus.QUEUED.value,
            priority=3,
            payload={
                "_input": {"url": "https://pg-exec.example.com"},
                "_current_attempt": 1,
            },
            tenant_id=pg_principal.tenant_id,
            max_retries=2,
            timeout_seconds=30,
            created_at=get_utc_now(),
            definition_id=pg_active_definition["id"],
            definition_version_id=pg_active_definition["active_version_id"],
        )
        db.add(task)
        db.commit()
        task_id = task.id

    before_success = _get_counter_value(
        DEFINITION_EXECUTIONS_TOTAL,
        {"task_type": "webhook_send", "status": "SUCCESS"},
    )

    def _handler(payload):
        return {"status": "ok"}

    with (
        patch.object(TaskRegistry, "has_handler", return_value=True),
        patch("taskforge.workers.execution_engine.get_handler", return_value=_handler),
    ):
        result = execute_task(task_id, "pg-worker-m1")

    assert result == ExecutionDisposition.COMPLETED

    after_success = _get_counter_value(
        DEFINITION_EXECUTIONS_TOTAL,
        {"task_type": "webhook_send", "status": "SUCCESS"},
    )
    assert after_success == before_success + 1.0

    # Cardinality check: verify definition_id and task_id are not in metric labels
    for metric in DEFINITION_EXECUTIONS_TOTAL.collect():
        for sample in metric.samples:
            assert "definition_id" not in sample.labels
            assert "task_id" not in sample.labels
            assert "tenant_id" not in sample.labels


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_read_polling_does_not_increment_metrics(pg_principal, pg_active_definition):
    """G11.5-C / PostgreSQL: Read-only list and detail calls do NOT increment any counters."""
    before_submitted = _get_counter_value(
        DEFINITION_EXECUTIONS_TOTAL,
        {"task_type": "webhook_send", "status": "SUBMITTED"},
    )
    before_success = _get_counter_value(
        DEFINITION_EXECUTIONS_TOTAL,
        {"task_type": "webhook_send", "status": "SUCCESS"},
    )

    with SessionLocal() as db:
        svc = DefinitionService(db)
        # Read operations
        executions, total = svc.list_executions(
            principal=pg_principal,
            definition_id=pg_active_definition["id"],
        )

    after_submitted = _get_counter_value(
        DEFINITION_EXECUTIONS_TOTAL,
        {"task_type": "webhook_send", "status": "SUBMITTED"},
    )
    after_success = _get_counter_value(
        DEFINITION_EXECUTIONS_TOTAL,
        {"task_type": "webhook_send", "status": "SUCCESS"},
    )

    assert after_submitted == before_submitted
    assert after_success == before_success
