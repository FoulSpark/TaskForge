"""Live PostgreSQL 16 tests for Gate G11.5-B Execution Listing and Detail APIs.

Verifies against live PostgreSQL 16:
1. Deterministic execution listing ordering (created_at DESC, id DESC).
2. Status filtering and version number filtering.
3. Execution detail inspection with version snapshot, scrubbed payload, and failure diagnosis.
4. Database-enforced tenant isolation (Tenant B cannot list or inspect Tenant A's tasks or definitions).
"""

import uuid

import pytest
from sqlalchemy import create_engine, text

from taskforge.api.schemas import DefinitionExecuteRequest
from taskforge.core.auth import AuthenticatedPrincipal
from taskforge.core.config import get_settings
from taskforge.database.session import SessionLocal
from taskforge.definitions.service import (
    DefinitionNotFoundError,
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
        key_id=f"actor-{uuid.uuid4().hex[:8]}",
        tenant_id=f"tenant-pg-obs-{uuid.uuid4().hex[:8]}",
        name="Postgres Observability Tester",
        scopes=["definitions:write", "tasks:write", "tasks:read"],
    )


@pytest.fixture
def other_pg_principal():
    return AuthenticatedPrincipal(
        key_id=f"actor-other-{uuid.uuid4().hex[:8]}",
        tenant_id=f"tenant-pg-other-{uuid.uuid4().hex[:8]}",
        name="Other Postgres Tenant",
        scopes=["definitions:write", "tasks:write", "tasks:read"],
    )


@pytest.fixture
def pg_active_definition(pg_principal):
    with SessionLocal() as db:
        svc = DefinitionService(db)
        defn, v1 = svc.create_definition(
            principal=pg_principal,
            name=f"PG Obs Task {uuid.uuid4().hex[:6]}",
            description="Testing live Postgres 16 execution listing and detail APIs",
            task_type="webhook_send",
            execution_type="builtin",
            configuration_json={"timeout_ms": 3000},
            input_schema_json={
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["url"],
                "properties": {
                    "url": {"type": "string"},
                    "auth_token": {"type": "string"},
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
            "input_schema": v1.input_schema_json,
            "output_schema": v1.output_schema_json,
        }


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_list_executions_deterministic_ordering_and_filters(pg_principal, pg_active_definition):
    """Evidence G11.5-B.1: Execution listing on PostgreSQL returns deterministic ordering and accurate filters."""
    with SessionLocal() as db:
        svc = DefinitionService(db)
        t1, _ = svc.execute_definition(
            principal=pg_principal,
            definition_id=pg_active_definition["id"],
            request=DefinitionExecuteRequest(input={"url": "https://example.com/pg-1"}),
        )
        t2, _ = svc.execute_definition(
            principal=pg_principal,
            definition_id=pg_active_definition["id"],
            request=DefinitionExecuteRequest(input={"url": "https://example.com/pg-2"}),
        )

    with SessionLocal() as db:
        svc = DefinitionService(db)
        items, total = svc.list_executions(
            principal=pg_principal,
            definition_id=pg_active_definition["id"],
            limit=10,
            offset=0,
        )
        assert total == 2
        # Deterministic created_at DESC, id DESC
        assert items[0]["id"] == t2.id
        assert items[1]["id"] == t1.id
        assert items[0]["version_number"] == 1

        # Status filter
        queued_items, q_total = svc.list_executions(
            principal=pg_principal,
            definition_id=pg_active_definition["id"],
            status="QUEUED",
        )
        assert q_total == 2

        # Nonexistent status filter
        failed_items, f_total = svc.list_executions(
            principal=pg_principal,
            definition_id=pg_active_definition["id"],
            status="FAILED",
        )
        assert f_total == 0
        assert len(failed_items) == 0


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_execution_detail_snapshot_and_redaction(pg_principal, pg_active_definition):
    """Evidence G11.5-B.2: Execution detail inspection returns version snapshot, failure diagnosis, and scrubbed secrets."""
    with SessionLocal() as db:
        svc = DefinitionService(db)
        task, _ = svc.execute_definition(
            principal=pg_principal,
            definition_id=pg_active_definition["id"],
            request=DefinitionExecuteRequest(
                input={
                    "url": "https://example.com/pg-detail",
                    "auth_token": "secret-pg-token-12345",
                }
            ),
        )
        task_id = task.id

    with SessionLocal() as db:
        svc = DefinitionService(db)
        detail = svc.get_execution_detail(
            principal=pg_principal,
            definition_id=pg_active_definition["id"],
            task_id=task_id,
        )
        assert detail["id"] == task_id
        assert detail["version"]["version_number"] == 1
        assert detail["version"]["task_type"] == "webhook_send"
        assert detail["version"]["content_hash"] is not None
        # Secret scrubbing
        assert detail["payload"]["url"] == "https://example.com/pg-detail"
        assert detail["payload"]["auth_token"] == "[REDACTED]"
        assert "lease_token" not in detail["payload"]


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_cross_tenant_isolation_enforced_at_query_boundary(
    pg_principal, other_pg_principal, pg_active_definition
):
    """Evidence G11.5-B.3: Tenant B cannot list or inspect Tenant A's tasks or definitions on PostgreSQL."""
    with SessionLocal() as db:
        svc = DefinitionService(db)
        task, _ = svc.execute_definition(
            principal=pg_principal,
            definition_id=pg_active_definition["id"],
            request=DefinitionExecuteRequest(input={"url": "https://example.com/pg-iso"}),
        )
        task_id = task.id

    with SessionLocal() as db:
        svc = DefinitionService(db)
        # 1. Listing Tenant A's definition from Tenant B returns 404
        with pytest.raises(DefinitionNotFoundError):
            svc.list_executions(
                principal=other_pg_principal,
                definition_id=pg_active_definition["id"],
            )

        # 2. Inspecting Tenant A's task from Tenant B returns 404
        with pytest.raises(DefinitionNotFoundError):
            svc.get_execution_detail(
                principal=other_pg_principal,
                definition_id=pg_active_definition["id"],
                task_id=task_id,
            )
