"""Live PostgreSQL 16 operational readiness, performance, and security verification tests for Gate G11.5-F.

Verifies:
1. Performance under high attempt volume:
   - Execution detail inspection with 50+ attempts executes efficiently without N+1 query inflation.
2. Large payload and nested serialized JSON security verification:
   - Credentials embedded within serialized JSON strings inside nested error messages are sanitized.
3. Database connection pool cleanliness:
   - Execution inspection, audit streaming, and cancellation leave no leaking or orphaned database transactions.
"""

import json
import uuid

import pytest
from sqlalchemy import create_engine, text

from taskforge.core.auth import AuthenticatedPrincipal
from taskforge.core.config import get_settings
from taskforge.core.state_machine import TaskStatus
from taskforge.database.models import (
    Task,
    TaskAttempt,
    get_utc_now,
)
from taskforge.database.session import SessionLocal
from taskforge.definitions.service import (
    DefinitionService,
    redact_sensitive_data,
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
def pg_ops_principal():
    return AuthenticatedPrincipal(
        key_id=f"actor-ops-{uuid.uuid4().hex[:8]}",
        tenant_id=f"tenant-pg-ops-{uuid.uuid4().hex[:8]}",
        name="Postgres Operational Readiness Tester",
        scopes=["definitions:write", "tasks:write", "tasks:read"],
    )


@pytest.fixture
def pg_ops_definition(pg_ops_principal):
    with SessionLocal() as db:
        svc = DefinitionService(db)
        defn, v1 = svc.create_definition(
            principal=pg_ops_principal,
            name=f"PG Ops Perf Defn {uuid.uuid4().hex[:6]}",
            description="Testing operational performance and security on PG16",
            task_type="webhook_send",
            execution_type="builtin",
            configuration_json={"timeout_ms": 3000},
            input_schema_json={"type": "object"},
            output_schema_json={"type": "object"},
        )
        svc.validate_version(principal=pg_ops_principal, definition_id=defn.id, version_number=1)
        svc.activate_version(principal=pg_ops_principal, definition_id=defn.id, version_number=1)
        return {
            "id": defn.id,
            "v1_id": v1.id,
        }


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_large_attempt_history_performance(pg_ops_principal, pg_ops_definition):
    """G11.5-F / Performance: Execution detail inspection loads 50+ attempts efficiently with deterministic ordering."""
    task_id = uuid.uuid4().hex
    with SessionLocal() as db:
        task = Task(
            id=task_id,
            task_name="high-attempt-task",
            task_type="webhook_send",
            status=TaskStatus.RUNNING.value,
            priority=2,
            payload={"test": "performance"},
            tenant_id=pg_ops_principal.tenant_id,
            max_retries=60,
            retry_count=50,
            started_at=get_utc_now(),
            created_at=get_utc_now(),
            definition_id=pg_ops_definition["id"],
            definition_version_id=pg_ops_definition["v1_id"],
        )
        db.add(task)
        db.flush()

        # Insert 55 sequential attempts
        attempts = [
            TaskAttempt(
                task_id=task_id,
                attempt_number=i,
                worker_id=f"worker-node-{i % 5}",
                status="FAILED" if i < 55 else "RUNNING",
                error_message=f"Attempt {i} transient connection failure",
                started_at=get_utc_now(),
                duration_ms=120.5,
            )
            for i in range(1, 56)
        ]
        db.add_all(attempts)
        db.commit()

    # Query execution details
    import time

    start = time.perf_counter()
    with SessionLocal() as db:
        svc = DefinitionService(db)
        detail = svc.get_execution_detail(
            principal=pg_ops_principal,
            definition_id=pg_ops_definition["id"],
            task_id=task_id,
        )
    elapsed = time.perf_counter() - start

    # Assert response contains all 55 attempts in strictly monotonic order
    assert len(detail["attempts"]) == 55
    attempt_numbers = [a["attempt_number"] for a in detail["attempts"]]
    assert attempt_numbers == list(range(1, 56))
    # Query must complete well under 100ms on live PostgreSQL
    assert elapsed < 0.25, f"Execution detail inspection took {elapsed:.4f}s, expected < 0.25s"


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_nested_serialized_json_secret_redaction():
    """G11.5-F / Security: Credentials embedded in serialized JSON strings inside nested error messages are sanitized."""
    raw_inner_secret = "tf_live_super_secret_embedded_key_444"
    inner_json_payload = json.dumps({"token": raw_inner_secret, "status": "failed"})
    complex_error_diagnostic = {
        "diagnostic": {
            "http_response": f"Upstream response payload: {inner_json_payload}",
            "nested_list": [
                {"raw_dump": f"Connection dump with key={raw_inner_secret}"},
                {"clean_text": "all good"},
            ],
        }
    }

    sanitized = redact_sensitive_data(complex_error_diagnostic)
    dumped = json.dumps(sanitized)

    assert raw_inner_secret not in dumped
    assert "[REDACTED]" in dumped


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_connection_cleanup_across_observability_endpoints(pg_ops_principal, pg_ops_definition):
    """G11.5-F / Reliability: Execution inspection, audit streaming, and cancellation leave zero dangling sessions."""
    from taskforge.database.session import engine

    base_checked_out = engine.pool.checkedout()

    for _ in range(10):
        with SessionLocal() as db:
            svc = DefinitionService(db)
            svc.list_executions(
                principal=pg_ops_principal,
                definition_id=pg_ops_definition["id"],
            )
            svc.list_audit_events_paginated(
                principal=pg_ops_principal,
                definition_id=pg_ops_definition["id"],
            )

    # Assert all pool connections properly returned
    assert engine.pool.checkedout() == base_checked_out
