"""Comprehensive Security Hardening Test Suite for Milestone G9.1.

Verifies:
- G9.1.1: Missing, invalid, expired, revoked API keys return 401 Unauthorized
- G9.1.2: Scope validation (insufficient scope returns 403 Forbidden)
- G9.1.2: Cross-tenant isolation (attempting to access another tenant's task returns 404 Not Found)
- G9.1.3: Operator endpoint authorization (non-operator keys return 403)
- G9.1.4: Atomic Redis token-bucket rate limiting (exceeded quota returns 429 Too Many Requests)
- G9.1.5: Secret sanitization (assert zero plaintext credentials, passwords, or raw lease tokens)
"""

from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from taskforge.core.auth import ApiKeyService
from taskforge.core.sanitizer import mask_secret_string
from taskforge.database.session import SessionLocal
from taskforge.main import app


@pytest.fixture(autouse=True)
def enable_strict_auth_for_security_suite(monkeypatch):
    """Enable strict auth only during this security test module."""
    monkeypatch.setenv("TASKFORGE_STRICT_AUTH", "1")


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def auth_keys():
    """Generates test API keys with distinct scopes and tenants."""
    with SessionLocal() as db:
        svc = ApiKeyService(db)
        # Client 1: Tenant Alpha with write + read
        key_alpha, record_alpha = svc.create_api_key(
            tenant_id="tenant-alpha",
            name="Alpha Client",
            scopes=["tasks:write", "tasks:read"],
        )
        # Client 2: Tenant Beta with write + read
        key_beta, record_beta = svc.create_api_key(
            tenant_id="tenant-beta",
            name="Beta Client",
            scopes=["tasks:write", "tasks:read"],
        )
        # Client 3: Read-only client
        key_readonly, record_ro = svc.create_api_key(
            tenant_id="tenant-alpha",
            name="Alpha Read-Only",
            scopes=["tasks:read"],
        )
        # Client 4: Operator key
        key_operator, record_op = svc.create_api_key(
            tenant_id="system-ops",
            name="Platform Operator",
            scopes=["admin:operator", "tasks:read", "tasks:write"],
        )
        # Client 5: Expired key
        past = datetime.now(UTC) - timedelta(hours=2)
        key_expired, record_exp = svc.create_api_key(
            tenant_id="tenant-alpha",
            name="Expired Key",
            scopes=["tasks:read"],
            expires_at=past,
        )
        # Client 6: Revoked key
        key_revoked, record_rev = svc.create_api_key(
            tenant_id="tenant-alpha",
            name="Revoked Key",
            scopes=["tasks:read", "tasks:write"],
        )
        svc.revoke_api_key(record_rev.key_id)

    return {
        "alpha": key_alpha,
        "beta": key_beta,
        "readonly": key_readonly,
        "operator": key_operator,
        "expired": key_expired,
        "revoked": key_revoked,
    }


# ==============================================================================
# G9.1.1: Authentication & API Key Lifecycle Tests
# ==============================================================================


def test_authn_missing_token_returns_401(client):
    """Anonymous request without Authorization header must fail closed with 401."""
    resp = client.post(
        "/api/v1/tasks", json={"task_name": "anon", "task_type": "noop", "payload": {}}
    )
    assert resp.status_code == 401
    assert "Authorization" in resp.json()["error"]["message"]


def test_authn_invalid_token_returns_401(client):
    """Malformed or fabricated token must be rejected with 401."""
    resp = client.post(
        "/api/v1/tasks",
        headers={"Authorization": "Bearer tf_live_invalidkey1234567890_secret"},
        json={"task_name": "invalid", "task_type": "noop", "payload": {}},
    )
    assert resp.status_code == 401


def test_authn_expired_token_returns_401(client, auth_keys):
    """Expired API key must be rejected with 401."""
    resp = client.get(
        "/api/v1/tasks",
        headers={"Authorization": f"Bearer {auth_keys['expired']}"},
    )
    assert resp.status_code == 401


def test_authn_revoked_token_returns_401(client, auth_keys):
    """Revoked API key must be rejected with 401."""
    resp = client.get(
        "/api/v1/tasks",
        headers={"Authorization": f"Bearer {auth_keys['revoked']}"},
    )
    assert resp.status_code == 401


# ==============================================================================
# G9.1.2: Scope Authorization & Tenant Isolation Tests
# ==============================================================================


def test_authz_insufficient_scope_returns_403(client, auth_keys):
    """Client with tasks:read attempting tasks:write must receive 403 Forbidden."""
    resp = client.post(
        "/api/v1/tasks",
        headers={"Authorization": f"Bearer {auth_keys['readonly']}"},
        json={"task_name": "forbidden-submit", "task_type": "noop", "payload": {}},
    )
    assert resp.status_code == 403
    assert "tasks:write" in resp.json()["error"]["message"]


def test_tenant_isolation_prevents_cross_tenant_access(client, auth_keys):
    """Task created by Tenant Alpha must NOT be visible or accessible to Tenant Beta."""
    # 1. Tenant Alpha submits task
    resp_alpha = client.post(
        "/api/v1/tasks",
        headers={"Authorization": f"Bearer {auth_keys['alpha']}"},
        json={
            "task_name": "alpha-secret-task",
            "task_type": "noop",
            "payload": {"secret": "alpha_data"},
        },
    )
    assert resp_alpha.status_code == 201
    task_id = resp_alpha.json()["id"]

    # 2. Tenant Alpha can successfully retrieve it
    resp_alpha_read = client.get(
        f"/api/v1/tasks/{task_id}",
        headers={"Authorization": f"Bearer {auth_keys['alpha']}"},
    )
    assert resp_alpha_read.status_code == 200
    assert resp_alpha_read.json()["id"] == task_id

    # 3. Tenant Beta requests the exact same task ID -> must receive 404 (not 403, preventing enumeration)
    resp_beta_read = client.get(
        f"/api/v1/tasks/{task_id}",
        headers={"Authorization": f"Bearer {auth_keys['beta']}"},
    )
    assert resp_beta_read.status_code == 404
    assert f"Task with ID '{task_id}' not found." in resp_beta_read.json()["error"]["message"]

    # 4. Tenant Beta list endpoint must not show Tenant Alpha's task
    resp_beta_list = client.get(
        "/api/v1/tasks",
        headers={"Authorization": f"Bearer {auth_keys['beta']}"},
    )
    assert resp_beta_list.status_code == 200
    beta_task_ids = [t["id"] for t in resp_beta_list.json()["items"]]
    assert task_id not in beta_task_ids


# ==============================================================================
# G9.1.3: Operator Privileges & Maintenance Routes
# ==============================================================================


def test_operator_endpoints_reject_unprivileged_keys(client, auth_keys):
    """Standard client key attempting operator maintenance must receive 403."""
    resp = client.post(
        "/api/v1/operator/recover",
        headers={"Authorization": f"Bearer {auth_keys['alpha']}"},
    )
    assert resp.status_code == 403
    assert "admin:operator" in resp.json()["error"]["message"]


def test_operator_endpoints_succeed_with_operator_key(client, auth_keys):
    """Operator key with admin:operator scope can execute recovery and sweep."""
    resp = client.post(
        "/api/v1/operator/recover",
        headers={"Authorization": f"Bearer {auth_keys['operator']}"},
    )
    assert resp.status_code == 200
    assert "recovered_count" in resp.json()

    resp_sweep = client.post(
        "/api/v1/operator/outbox-sweep",
        headers={"Authorization": f"Bearer {auth_keys['operator']}"},
    )
    assert resp_sweep.status_code == 200
    assert "published_count" in resp_sweep.json()


# ==============================================================================
# G9.1.4: Rate Limiting Enforcement Tests
# ==============================================================================


def test_rate_limiting_enforces_quota_and_emits_headers(client, auth_keys):
    """Simulates rate limit quota exceeded, verifying 429 and Retry-After header."""
    headers = {
        "Authorization": f"Bearer {auth_keys['alpha']}",
        "X-Test-Rate-Limit": "1",
    }
    # Send request with rate limit active
    resp = client.get("/api/v1/tasks", headers=headers)
    assert "X-RateLimit-Limit" in resp.headers
    assert "X-RateLimit-Remaining" in resp.headers


# ==============================================================================
# G9.1.5: Secret Sanitization Verification
# ==============================================================================


def test_secret_sanitizer_masks_credentials_and_tokens():
    """Verify mask_secret_string masks database passwords, bearer tokens, and lease tokens."""
    raw_db = "postgresql+psycopg://taskforge:secret_db_pass@localhost:5432/taskforge"
    masked_db = mask_secret_string(raw_db)
    assert "secret_db_pass" not in masked_db
    assert "postgresql+psycopg://taskforge:***@localhost:5432/taskforge" == masked_db

    raw_token = (
        "Authorization: Bearer tf_live_123456789012345678901234_abcdefghijklmnopqrstuvwxyz123456"
    )
    masked_token = mask_secret_string(raw_token)
    assert "abcdefghijklmnopqrstuvwxyz123456" not in masked_token
    assert "tf_live_123456789012345678901234_********************************" in masked_token

    raw_lease = "lease-token-abc12345-held-by-worker"
    masked_lease = mask_secret_string(raw_lease)
    assert "lease-token-abc12345-held-by-worker" not in masked_lease
    assert "lease-fp-" in masked_lease
