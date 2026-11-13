"""Live PostgreSQL 16 tests for Gate G11.5-D Audit Query API and Deep Recursive Redaction.

Verifies against live PostgreSQL 16:
1. Deterministic audit pagination ordering (created_at DESC, id DESC).
2. Action filtering across PostgreSQL JSONB/JSON audit records.
3. Database query boundary tenant isolation: Tenant B receives 404 when querying Tenant A's definition audit stream.
4. Redaction integrity on real PostgreSQL records: metadata containing nested secrets and lease tokens are sanitized upon query without modifying underlying DB rows.
"""

import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, select, text

from taskforge.core.auth import AuthenticatedPrincipal
from taskforge.core.config import get_settings
from taskforge.database.session import SessionLocal
from taskforge.definitions.models import DefinitionAuditEvent
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
        key_id=f"actor-audit-{uuid.uuid4().hex[:8]}",
        tenant_id=f"tenant-pg-audit-{uuid.uuid4().hex[:8]}",
        name="Postgres Audit Tester A",
        scopes=["definitions:write", "tasks:write", "tasks:read"],
    )


@pytest.fixture
def other_pg_principal():
    return AuthenticatedPrincipal(
        key_id=f"actor-other-audit-{uuid.uuid4().hex[:8]}",
        tenant_id=f"tenant-pg-other-audit-{uuid.uuid4().hex[:8]}",
        name="Postgres Audit Tester B",
        scopes=["definitions:write", "tasks:write", "tasks:read"],
    )


@pytest.fixture
def pg_audited_definition(pg_principal):
    """Creates a definition with a full set of audit events on PostgreSQL 16."""
    with SessionLocal() as db:
        svc = DefinitionService(db)
        defn, v1 = svc.create_definition(
            principal=pg_principal,
            name=f"PG Audit Definition {uuid.uuid4().hex[:6]}",
            description="Testing live Postgres 16 audit event queries and redaction",
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
        )
        svc.validate_version(principal=pg_principal, definition_id=defn.id, version_number=1)
        svc.activate_version(principal=pg_principal, definition_id=defn.id, version_number=1)
        svc.disable_definition(principal=pg_principal, definition_id=defn.id)
        svc.enable_definition(principal=pg_principal, definition_id=defn.id)
        return {
            "id": defn.id,
            "tenant_id": pg_principal.tenant_id,
        }


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_audit_pagination_and_action_filtering(pg_principal, pg_audited_definition):
    """G11.5-D / PostgreSQL: Paginated query order is strictly descending; action filter isolates specific events."""
    with SessionLocal() as db:
        svc = DefinitionService(db)

        # 1. Full paginated list
        items, total = svc.list_audit_events_paginated(
            principal=pg_principal,
            definition_id=pg_audited_definition["id"],
            limit=2,
            offset=0,
        )
        assert total >= 5
        assert len(items) == 2
        # Verify ordering: items[0].created_at >= items[1].created_at
        assert items[0]["created_at"] >= items[1]["created_at"]

        # 2. Filter by action
        validated_items, v_total = svc.list_audit_events_paginated(
            principal=pg_principal,
            definition_id=pg_audited_definition["id"],
            action="VERSION_VALIDATED",
        )
        assert v_total == 1
        assert len(validated_items) == 1
        assert validated_items[0]["action"] == "VERSION_VALIDATED"


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_audit_deep_redaction_and_db_immutability(pg_principal, pg_audited_definition):
    """G11.5-D / PostgreSQL: Redaction sanitizes nested secrets without mutating PostgreSQL row storage."""
    definition_id = pg_audited_definition["id"]

    # Directly insert an audit event containing nested secrets and lease tokens
    with SessionLocal() as db:
        sensitive_metadata = {
            "auth": {
                "ApiKey": "pg_secret_key_999",
                "nestedList": [{"token": "jwt_secret_tok"}, {"safe": "value"}],
            },
            "url": "https://hook.example.com?key=tf_live_pg_leak_123",
            "lease_token": "lease-uuid-should-be-omitted",
            "normal_field": "unaltered",
        }
        evt = DefinitionAuditEvent(
            id=str(uuid.uuid4()),
            tenant_id=pg_principal.tenant_id,
            definition_id=definition_id,
            version_id=None,
            actor_id=pg_principal.key_id,
            action="CUSTOM_DIAGNOSTIC",
            metadata_json=sensitive_metadata,
            created_at=datetime.now(UTC),
        )
        db.add(evt)
        db.commit()
        evt_id = evt.id

    # Query via DefinitionService
    with SessionLocal() as db:
        svc = DefinitionService(db)
        items, _ = svc.list_audit_events_paginated(
            principal=pg_principal,
            definition_id=definition_id,
            action="CUSTOM_DIAGNOSTIC",
        )
        assert len(items) == 1
        meta = items[0]["metadata"]

        # Assert redaction
        assert meta["auth"]["ApiKey"] == "[REDACTED]"
        assert meta["auth"]["nestedList"][0]["token"] == "[REDACTED]"
        assert meta["auth"]["nestedList"][1]["safe"] == "value"
        assert "tf_live_" not in meta["url"]
        assert "[REDACTED]" in meta["url"]
        assert "lease_token" not in meta
        assert meta["normal_field"] == "unaltered"

    # Query directly from database to verify PostgreSQL rows were NOT mutated
    with SessionLocal() as db:
        row = db.execute(
            select(DefinitionAuditEvent).where(DefinitionAuditEvent.id == evt_id)
        ).scalar_one()
        db_meta = row.metadata_json
        assert db_meta["auth"]["ApiKey"] == "pg_secret_key_999"
        assert db_meta["auth"]["nestedList"][0]["token"] == "jwt_secret_tok"
        assert db_meta["lease_token"] == "lease-uuid-should-be-omitted"
        assert "tf_live_pg_leak_123" in db_meta["url"]


@pytest.mark.skipif(not PG_AVAILABLE, reason="Live PostgreSQL 16 instance not reachable")
def test_pg_audit_cross_tenant_isolation(pg_audited_definition, other_pg_principal):
    """G11.5-D / PostgreSQL: Database-level tenant boundary raises DefinitionNotFoundError for foreign tenants."""
    with SessionLocal() as db:
        svc = DefinitionService(db)
        with pytest.raises(DefinitionNotFoundError):
            svc.list_audit_events_paginated(
                principal=other_pg_principal,
                definition_id=pg_audited_definition["id"],
            )
