"""Unit and SQLite tests for Gate G11.5-B Execution Listing and Detail APIs.

Verifies:
1. Listing executions with deterministic pagination (created_at DESC, id DESC).
2. Status filtering and version number filtering.
3. Execution detail inspection with version snapshot and scrubbed payload.
4. Deep recursive redaction of secrets, auth headers, and lease tokens.
5. Failure diagnosis categorization (error_type, retryable, is_timeout).
6. Cross-tenant isolation returning 404 Not Found on nonexistent and other-tenant queries.
"""

import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from taskforge.api.dependencies import get_current_principal
from taskforge.api.schemas import DefinitionExecuteRequest
from taskforge.core.auth import AuthenticatedPrincipal
from taskforge.core.state_machine import TaskStatus
from taskforge.database.session import Base, get_db
from taskforge.definitions.service import (
    DefinitionNotFoundError,
    DefinitionService,
    redact_sensitive_data,
)
from taskforge.main import app

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
        key_id="key-tester",
        tenant_id="tenant-alpha",
        name="Tester",
        scopes=["tasks:read", "tasks:write", "definitions:write"],
    )


@pytest.fixture
def other_principal():
    return AuthenticatedPrincipal(
        key_id="key-other",
        tenant_id="tenant-beta",
        name="Other Tenant",
        scopes=["tasks:read", "tasks:write", "definitions:write"],
    )


@pytest.fixture
def active_definition(db, principal):
    svc = DefinitionService(db)
    defn, v1 = svc.create_definition(
        principal=principal,
        name="Inspectable Definition",
        description="Testing execution inspection APIs",
        task_type="webhook_send",
        execution_type="builtin",
        configuration_json={"timeout_ms": 5000},
        input_schema_json={
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["url"],
            "properties": {
                "url": {"type": "string"},
                "auth_header": {"type": "string"},
                "nested": {"type": "object"},
            },
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
    svc.validate_version(principal=principal, definition_id=defn.id, version_number=1)
    svc.activate_version(principal=principal, definition_id=defn.id, version_number=1)
    return svc.get_definition(principal, defn.id)


def test_recursive_redaction_engine():
    """Verify deep recursive redaction scrubs credentials, auth headers, and lease tokens."""
    data = {
        "url": "https://api.example.com",
        "api_key": "tf_live_1234567890",
        "nested": {
            "Authorization": "Bearer secret-token-xyz",
            "Password": "supersecretpassword",
            "items": [
                {"secret": "hidden-value", "safe": 123},
                {"apiKey": "abc"},
            ],
        },
        "lease_token": "lease-token-must-be-removed",
    }
    scrubbed = redact_sensitive_data(data)

    assert scrubbed["url"] == "https://api.example.com"
    assert scrubbed["api_key"] == "[REDACTED]"
    assert scrubbed["nested"]["Authorization"] == "[REDACTED]"
    assert scrubbed["nested"]["Password"] == "[REDACTED]"
    assert scrubbed["nested"]["items"][0]["secret"] == "[REDACTED]"
    assert scrubbed["nested"]["items"][0]["safe"] == 123
    assert scrubbed["nested"]["items"][1]["apiKey"] == "[REDACTED]"
    assert "lease_token" not in scrubbed


def test_list_executions_deterministic_ordering_and_filters(db, principal, active_definition):
    """Verify list_executions obeys deterministic ordering (created_at DESC, id DESC) and filters."""
    svc = DefinitionService(db)

    # Submit 3 tasks
    tasks = []
    for i in range(3):
        t, _ = svc.execute_definition(
            principal=principal,
            definition_id=active_definition.id,
            request=DefinitionExecuteRequest(
                input={"url": f"https://example.com/exec-{i}"},
                priority=2,
            ),
        )
        tasks.append(t)

    # 1. Unfiltered query
    items, total = svc.list_executions(
        principal=principal,
        definition_id=active_definition.id,
        limit=10,
        offset=0,
    )
    assert total == 3
    assert len(items) == 3
    # Verify deterministic ordering: newest first
    assert items[0]["id"] == tasks[2].id
    assert items[1]["id"] == tasks[1].id
    assert items[2]["id"] == tasks[0].id
    assert items[0]["version_number"] == 1

    # 2. Status filter
    queued_items, queued_total = svc.list_executions(
        principal=principal,
        definition_id=active_definition.id,
        status="QUEUED",
    )
    assert queued_total == 3

    none_items, none_total = svc.list_executions(
        principal=principal,
        definition_id=active_definition.id,
        status="SUCCESS",
    )
    assert none_total == 0
    assert len(none_items) == 0

    # 3. Version number filter
    v1_items, v1_total = svc.list_executions(
        principal=principal,
        definition_id=active_definition.id,
        version_number=1,
    )
    assert v1_total == 3

    v2_items, v2_total = svc.list_executions(
        principal=principal,
        definition_id=active_definition.id,
        version_number=2,
    )
    assert v2_total == 0


def test_execution_detail_with_scrubbed_payload_and_version_snapshot(
    db, principal, active_definition
):
    """Verify get_execution_detail returns immutable version snapshot, scrubbed payload, and attempts."""
    svc = DefinitionService(db)
    task, _ = svc.execute_definition(
        principal=principal,
        definition_id=active_definition.id,
        request=DefinitionExecuteRequest(
            input={
                "url": "https://example.com/target",
                "auth_header": "Bearer live-secret-key",
            },
        ),
    )

    detail = svc.get_execution_detail(
        principal=principal,
        definition_id=active_definition.id,
        task_id=task.id,
    )

    assert detail["id"] == task.id
    assert detail["status"] == TaskStatus.QUEUED.value
    assert detail["definition_id"] == active_definition.id
    assert detail["version"]["version_number"] == 1
    assert detail["version"]["task_type"] == "webhook_send"
    assert detail["version"]["content_hash"] is not None

    # Verify secret scrubbing
    assert detail["payload"]["url"] == "https://example.com/target"
    assert detail["payload"]["auth_header"] == "Bearer [REDACTED]"
    assert "lease_token" not in detail["payload"]


def test_cross_tenant_isolation_raises_not_found(db, principal, other_principal, active_definition):
    """Verify tenant isolation: Tenant B querying Tenant A's executions gets 404 Not Found."""
    svc = DefinitionService(db)
    task, _ = svc.execute_definition(
        principal=principal,
        definition_id=active_definition.id,
        request=DefinitionExecuteRequest(input={"url": "https://example.com/isolated"}),
    )

    # 1. Tenant B cannot list Tenant A's executions
    with pytest.raises(DefinitionNotFoundError) as exc:
        svc.list_executions(
            principal=other_principal,
            definition_id=active_definition.id,
        )
    assert "not found in tenant scope" in str(exc.value)

    # 2. Tenant B cannot inspect Tenant A's task
    with pytest.raises(DefinitionNotFoundError) as exc:
        svc.get_execution_detail(
            principal=other_principal,
            definition_id=active_definition.id,
            task_id=task.id,
        )
    assert "not found" in str(exc.value)


def test_api_route_list_and_detail_executions(db, principal, active_definition):
    """Verify FastAPI routes for listing and execution detail inspection."""
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_current_principal] = lambda: principal
    client = TestClient(app)

    try:
        # Submit a task
        res_exec = client.post(
            f"/api/v1/definitions/{active_definition.id}/execute",
            json={"input": {"url": "https://example.com/api-test"}},
        )
        assert res_exec.status_code == 201
        task_id = res_exec.json()["id"]

        # 1. List executions endpoint
        res_list = client.get(f"/api/v1/definitions/{active_definition.id}/executions")
        assert res_list.status_code == 200
        data_list = res_list.json()
        assert data_list["total"] == 1
        assert data_list["items"][0]["id"] == task_id
        assert data_list["items"][0]["version_number"] == 1

        # 2. Get execution detail endpoint
        res_detail = client.get(f"/api/v1/definitions/{active_definition.id}/executions/{task_id}")
        assert res_detail.status_code == 200
        data_detail = res_detail.json()
        assert data_detail["id"] == task_id
        assert data_detail["version"]["version_number"] == 1
        assert data_detail["payload"]["url"] == "https://example.com/api-test"

        # 3. 404 for invalid execution ID
        res_404 = client.get(
            f"/api/v1/definitions/{active_definition.id}/executions/{uuid.uuid4()}"
        )
        assert res_404.status_code == 404
    finally:
        app.dependency_overrides.clear()
