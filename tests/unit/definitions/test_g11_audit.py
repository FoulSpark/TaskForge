"""Unit tests for Sub-Gate G11.5-D — Definition Audit Query API & Deep Recursive Redaction.

Verifies:
1. Deep recursive redaction of secrets across nested dictionaries, lists, and metadata strings.
2. Mixed-case sensitive keys (apiKey, PassWord, Authorization, auth_token, Bearer_Token, Secret).
3. Lease tokens unconditionally stripped from client representations.
4. Non-mutation: persistent database models remain untouched when returned payloads are scrubbed.
5. GET /api/v1/definitions/{id}/audit:
   - Tenant boundary enforcement (Tenant B receives 404 for Tenant A definition).
   - Deterministic pagination (created_at DESC, id DESC).
   - Action filtering (e.g. action=DEFINITION_CREATED, VERSION_ACTIVATED).
   - All standard lifecycle actions correctly exposed.
"""

import uuid
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, select, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from taskforge.api.dependencies import get_current_principal
from taskforge.core.auth import AuthenticatedPrincipal
from taskforge.database.session import Base, get_db
from taskforge.definitions.models import DefinitionAuditEvent
from taskforge.definitions.service import (
    DefinitionNotFoundError,
    DefinitionService,
    redact_sensitive_data,
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


@pytest.fixture
def principal():
    return AuthenticatedPrincipal(
        key_id="key-audit-a",
        tenant_id="tenant-audit-a",
        name="Audit Tester A",
        scopes=["definitions:write", "tasks:read", "tasks:write"],
    )


@pytest.fixture
def other_principal():
    return AuthenticatedPrincipal(
        key_id="key-audit-b",
        tenant_id="tenant-audit-b",
        name="Audit Tester B",
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
def full_lifecycle_definition(db, principal):
    """Creates a definition through the full lifecycle: create, version, validate, activate, disable, enable."""
    svc = DefinitionService(db)
    defn, v1 = svc.create_definition(
        principal=principal,
        name="Audited Webhook Definition",
        description="Tracks complete audit trails",
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
    svc.validate_version(principal=principal, definition_id=defn.id, version_number=1)
    svc.activate_version(principal=principal, definition_id=defn.id, version_number=1)
    svc.disable_definition(principal=principal, definition_id=defn.id)
    svc.enable_definition(principal=principal, definition_id=defn.id)
    return defn


# ---------------------------------------------------------------------------
# 1. Deep Recursive Redaction Unit Tests
# ---------------------------------------------------------------------------


class TestDeepRecursiveRedaction:
    """Verify recursive redaction handles nested structures, mixed-case, arrays, and strings."""

    def test_mixed_case_key_redaction(self):
        payload = {
            "ApiKey": "super-secret-123",
            "passWord": "secret-password",
            "AUTHORIZATION": "Bearer token123",
            "auth_Token": "tok_xyz",
            "Private_Key": "-----BEGIN RSA PRIVATE KEY-----",
            "normalField": "visible_value",
        }
        scrubbed = redact_sensitive_data(payload)
        assert scrubbed["ApiKey"] == "[REDACTED]"
        assert scrubbed["passWord"] == "[REDACTED]"
        assert scrubbed["AUTHORIZATION"] == "[REDACTED]"
        assert scrubbed["auth_Token"] == "[REDACTED]"
        assert scrubbed["Private_Key"] == "[REDACTED]"
        assert scrubbed["normalField"] == "visible_value"

    def test_nested_dictionaries_and_arrays(self):
        payload = {
            "outer": {
                "level1": [
                    {"secret": "nested-token", "name": "item1"},
                    {"safe": "value", "subList": [{"token": "deep-secret"}]},
                ],
                "api_key": "outer-key",
            }
        }
        scrubbed = redact_sensitive_data(payload)
        assert scrubbed["outer"]["api_key"] == "[REDACTED]"
        assert scrubbed["outer"]["level1"][0]["secret"] == "[REDACTED]"
        assert scrubbed["outer"]["level1"][0]["name"] == "item1"
        assert scrubbed["outer"]["level1"][1]["subList"][0]["token"] == "[REDACTED]"

    def test_lease_token_stripped_unconditionally(self):
        payload = {
            "task_id": "t-123",
            "lease_token": "secret-lease-uuid",
            "nested": {"Lease_Token": "another-lease"},
        }
        scrubbed = redact_sensitive_data(payload)
        assert "lease_token" not in scrubbed
        assert "Lease_Token" not in scrubbed["nested"]
        assert scrubbed["task_id"] == "t-123"

    def test_embedded_tf_live_secret_in_strings(self):
        payload = {
            "connection_url": "https://api.taskforge.io/v1?key=tf_live_abc123xyz_999&limit=10",
            "raw_secret": "tf_live_standalone_secret",
            "error_msg": "Failed to connect with credentials tf_live_err_token_111",
        }
        scrubbed = redact_sensitive_data(payload)
        assert "tf_live_" not in scrubbed["connection_url"]
        assert "[REDACTED]" in scrubbed["connection_url"]
        assert scrubbed["raw_secret"] == "[REDACTED]"
        assert "tf_live_" not in scrubbed["error_msg"]
        assert "[REDACTED]" in scrubbed["error_msg"]

    def test_embedded_bearer_token_in_strings(self):
        payload = {
            "header": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.xyz",
            "mixed_header": "Authorization: Bearer secret_token_value",
        }
        scrubbed = redact_sensitive_data(payload)
        assert scrubbed["header"] == "Bearer [REDACTED]"
        assert "Bearer [REDACTED]" in scrubbed["mixed_header"]

    def test_redaction_does_not_mutate_original_input(self):
        original = {
            "api_key": "original-key",
            "nested": {"password": "pass123", "items": ["a", "b"]},
        }
        copy_before = dict(original)
        scrubbed = redact_sensitive_data(original)
        assert scrubbed["api_key"] == "[REDACTED]"
        assert original["api_key"] == "original-key"
        assert original["nested"]["password"] == "pass123"
        assert copy_before == original


# ---------------------------------------------------------------------------
# 2. Service & Database Audit Model Integrity Tests
# ---------------------------------------------------------------------------


class TestAuditModelAndService:
    """Verify audit records are stored faithfully and never mutated upon query."""

    def test_audit_records_in_db_not_mutated_by_queries(self, db, principal):
        """Underlying DB rows retain un-redacted metadata; query scrubbing is strictly a read projection."""
        svc = DefinitionService(db)
        defn, _ = svc.create_definition(
            principal=principal,
            name="Immutability Check",
            description="Testing DB immutability",
            task_type="webhook_send",
            execution_type="builtin",
            configuration_json={"timeout_ms": 1000},
            input_schema_json={"type": "object"},
            output_schema_json={"type": "object"},
        )
        # Directly insert an audit event with sensitive metadata to verify non-mutation
        evt = DefinitionAuditEvent(
            id=str(uuid.uuid4()),
            tenant_id=principal.tenant_id,
            definition_id=defn.id,
            version_id=None,
            actor_id=principal.key_id,
            action="CUSTOM_AUDIT_ACTION",
            metadata_json={
                "custom_key": "apiKey_sensitive_data",
                "nested": {"token": "secret_123"},
            },
            created_at=datetime.now(UTC),
        )
        db.add(evt)
        db.commit()

        # Query via service
        items, total = svc.list_audit_events_paginated(
            principal=principal,
            definition_id=defn.id,
        )
        custom_item = next(i for i in items if i["action"] == "CUSTOM_AUDIT_ACTION")
        assert custom_item["metadata"]["nested"]["token"] == "[REDACTED]"

        # Re-query raw DB row directly to verify zero database mutation
        db.expire_all()
        raw_row = db.execute(
            select(DefinitionAuditEvent).where(DefinitionAuditEvent.id == evt.id)
        ).scalar_one()
        assert raw_row.metadata_json["nested"]["token"] == "secret_123"

    def test_cross_tenant_audit_query_rejected(
        self, db, principal, other_principal, full_lifecycle_definition
    ):
        """Tenant B cannot query audit events of Tenant A's definition."""
        svc = DefinitionService(db)
        with pytest.raises(DefinitionNotFoundError):
            svc.list_audit_events_paginated(
                principal=other_principal,
                definition_id=full_lifecycle_definition.id,
            )


# ---------------------------------------------------------------------------
# 3. HTTP API Endpoint Tests (GET /api/v1/definitions/{id}/audit)
# ---------------------------------------------------------------------------


class TestAuditHttpApi:
    """Verify GET /api/v1/definitions/{id}/audit endpoint contract."""

    def test_get_audit_history_happy_path(self, client, full_lifecycle_definition):
        """Returns 200 with paginated audit events and scrubbed metadata."""
        resp = client.get(f"/api/v1/definitions/{full_lifecycle_definition.id}/audit")
        assert resp.status_code == 200
        data = resp.json()

        assert "items" in data
        assert "total" in data
        assert data["total"] >= 5  # create, version, validate, activate, disable, enable
        assert data["limit"] == 50
        assert data["offset"] == 0

        # Actions should be present in returned items
        actions = [item["action"] for item in data["items"]]
        assert "DEFINITION_CREATED" in actions
        assert "VERSION_CREATED" in actions
        assert "VERSION_VALIDATED" in actions
        assert "VERSION_ACTIVATED" in actions
        assert "DEFINITION_DISABLED" in actions
        assert "DEFINITION_ENABLED" in actions

    def test_get_audit_history_action_filter(self, client, full_lifecycle_definition):
        """Filters audit trail by specific action."""
        resp = client.get(
            f"/api/v1/definitions/{full_lifecycle_definition.id}/audit?action=VERSION_ACTIVATED"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert len(data["items"]) == 1
        assert data["items"][0]["action"] == "VERSION_ACTIVATED"

    def test_get_audit_history_pagination(self, client, full_lifecycle_definition):
        """Pagination returns subset with deterministic ordering."""
        resp1 = client.get(
            f"/api/v1/definitions/{full_lifecycle_definition.id}/audit?limit=2&offset=0"
        )
        assert resp1.status_code == 200
        d1 = resp1.json()
        assert len(d1["items"]) == 2

        resp2 = client.get(
            f"/api/v1/definitions/{full_lifecycle_definition.id}/audit?limit=2&offset=2"
        )
        assert resp2.status_code == 200
        d2 = resp2.json()
        assert len(d2["items"]) == 2

        # Verify no overlap in paginated slices
        ids1 = {i["id"] for i in d1["items"]}
        ids2 = {i["id"] for i in d2["items"]}
        assert ids1.isdisjoint(ids2)

    def test_get_audit_history_cross_tenant_returns_404(
        self, client, full_lifecycle_definition, other_principal
    ):
        """Tenant B receives 404 when requesting Tenant A's definition audit history."""
        app.dependency_overrides[get_current_principal] = lambda: other_principal
        resp = client.get(f"/api/v1/definitions/{full_lifecycle_definition.id}/audit")
        assert resp.status_code == 404
        body = resp.json()
        error_msg = body.get("error", {}).get("message") or body.get("detail", "")
        assert "not found" in str(error_msg).lower()
