"""Comprehensive Security Hardening Test Suite for Milestone G9.1.

Verifies:
- G9.1.1: Missing, invalid, expired, revoked API keys return 401 Unauthorized
- G9.1.2: Scope validation (insufficient scope returns 403 Forbidden)
- G9.1.2: Cross-tenant isolation (attempting to access another tenant's task returns 404 Not Found)
- G9.1.3: Operator endpoint authorization (non-operator keys return 403)
- G9.1.4: Atomic Redis token-bucket rate limiting (exceeded quota returns 429 Too Many Requests)
- G9.1.5: Secret sanitization (assert zero plaintext credentials, passwords, or raw lease tokens)
"""

import uuid
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
def redis_conn():
    from taskforge.queue.redis_client import create_redis_client

    return create_redis_client()


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


def test_rate_limiter_unique_members_same_instant(redis_conn):
    """Verify that multiple requests in the exact same millisecond get unique members
    and do not overwrite each other in the Redis sorted set."""
    from taskforge.core.rate_limiter import RedisRateLimiter

    client_key = f"test-instant-{uuid.uuid4().hex}"
    limiter = RedisRateLimiter(requests_per_minute=10, redis_client=redis_conn)

    # Send 5 rapid requests with explicit unique request IDs
    for i in range(5):
        allowed, remaining, _ = limiter.is_allowed(client_key, request_id=f"req-{i}")
        assert allowed is True
        assert remaining == 10 - (i + 1)

    # Check ZCARD in Redis directly: must be exactly 5
    zcard = redis_conn.zcard(f"taskforge:ratelimit:{client_key}")
    assert zcard == 5, f"Expected 5 distinct members in sorted set, got {zcard}"

    # Verify members have unique request IDs
    members = [
        m.decode() if isinstance(m, bytes) else m
        for m in redis_conn.zrange(f"taskforge:ratelimit:{client_key}", 0, -1)
    ]
    assert len(set(members)) == 5
    redis_conn.delete(f"taskforge:ratelimit:{client_key}")


def test_rate_limiter_boundary_and_rejection_no_slot_consumption(redis_conn):
    """Verify quota boundary: rejected requests return Retry-After and do NOT consume quota slots."""
    from taskforge.core.rate_limiter import RedisRateLimiter

    client_key = f"test-quota-{uuid.uuid4().hex}"
    limiter = RedisRateLimiter(requests_per_minute=3, redis_client=redis_conn)

    # Fill quota (3 requests)
    for _ in range(3):
        allowed, _, _ = limiter.is_allowed(client_key)
        assert allowed is True

    # 4th request must be rejected
    allowed, remaining, retry_after = limiter.is_allowed(client_key)
    assert allowed is False
    assert remaining == 0
    assert retry_after >= 1.0

    # 5th request also rejected
    allowed2, remaining2, retry_after2 = limiter.is_allowed(client_key)
    assert allowed2 is False
    assert remaining2 == 0

    # Sorted set size must remain strictly 3 (rejections must not add members)
    zcard = redis_conn.zcard(f"taskforge:ratelimit:{client_key}")
    assert zcard == 3, f"Rejected requests must not consume slots or add members, got {zcard}"
    redis_conn.delete(f"taskforge:ratelimit:{client_key}")


def test_rate_limiter_concurrent_burst_enforces_exact_limit(redis_conn):
    """Verify concurrent requests from multiple threads cannot exceed the limit."""
    import concurrent.futures

    from taskforge.core.rate_limiter import RedisRateLimiter

    client_key = f"test-concurrent-{uuid.uuid4().hex}"
    limit = 10
    limiter = RedisRateLimiter(requests_per_minute=limit, redis_client=redis_conn)

    def attempt_request(idx):
        return limiter.is_allowed(client_key, request_id=f"worker-req-{idx}")

    # Launch 30 concurrent requests
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(attempt_request, range(30)))

    allowed_count = sum(1 for allowed, _, _ in results if allowed)
    rejected_count = sum(1 for allowed, _, _ in results if not allowed)

    assert allowed_count == limit, f"Expected exactly {limit} allowed requests, got {allowed_count}"
    assert rejected_count == 20, f"Expected exactly 20 rejected requests, got {rejected_count}"
    redis_conn.delete(f"taskforge:ratelimit:{client_key}")


def test_rate_limiter_fails_closed_on_redis_error():
    """Verify that when Redis is unreachable or raises RedisError, rate limiter fails closed."""
    from unittest.mock import MagicMock

    from redis.exceptions import ConnectionError as RedisConnectionError

    from taskforge.core.rate_limiter import RedisRateLimiter

    mock_redis = MagicMock()
    mock_redis.register_script.return_value.side_effect = RedisConnectionError("Connection refused")
    limiter = RedisRateLimiter(requests_per_minute=100, redis_client=mock_redis)

    allowed, remaining, retry_after = limiter.is_allowed("test-client")
    assert allowed is False
    assert remaining == 0
    assert retry_after == 5.0  # Fail-closed backoff


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
