# G9.0.2 — Comprehensive STRIDE Threat Model

## 1. Overview & Trust Boundaries

The TaskForge architecture spans three distinct zones of trust:
1. **External Untrusted Zone:** Third-party API callers, webhook dispatchers, anonymous HTTP clients.
2. **DMZ / Gateway Boundary:** Reverse proxy / FastAPI middleware performing authentication, rate limiting, and request validation.
3. **Internal Trusted Zone:** API server, worker execution processes, PostgreSQL database, and Redis broker.

---

## 2. STRIDE Assessment Matrix

| Threat Category | Threat ID | Specific Attack Vector | Current State (`v0.8.0-phase8-freeze`) | Phase 9.1 Target Control | Residual Risk Level |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Spoofing** | T-S-01 | Rogue worker impersonation claiming tasks without authorization | Worker IDs are self-assigned process strings (`worker-pid`) | Unguessable cryptographically random lease tokens validated under `FOR UPDATE` | Low |
| **Spoofing** | T-S-02 | Unauthenticated caller submitting tasks under arbitrary tenant identity | No authentication required on `/api/v1/tasks` | Cryptographic API Bearer tokens mapped to immutable `tenant_id` context | High (Pre-9.1) → Low (Post-9.1) |
| **Tampering** | T-T-01 | Client tampering with task payload or retry budget post-submission | Payloads stored in JSONB; no API update endpoint exists, but database is unprotected from internal tampering | Schema-level field immutability on `payload`, `task_type`, `idempotency_key` | Low |
| **Tampering** | T-T-02 | Redis message manipulation in queue | Redis runs without `requirepass` in development compose | Redis authentication (`requirepass`), TLS in transit, and HMAC envelope validation | Medium (Pre-9.1) → Low (Post-9.1) |
| **Repudiation** | T-R-01 | Anonymous manual task cancellation or lease releases without audit trails | `task_events` records state transitions but lacks caller identity | Mandatory `actor_id`, `request_id`, and `client_ip` audit logging in `task_events` | Medium (Pre-9.1) → Low (Post-9.1) |
| **Information Disclosure** | T-I-01 | Credential and connection string leakage via unhandled 500 error tracebacks | Tracebacks caught in handlers, but development server flags can leak DB strings | Strict exception boundary returning sanitized RFC 7807 problem details; secret masking in logs | Medium (Pre-9.1) → Low (Post-9.1) |
| **Information Disclosure** | T-I-02 | Cross-tenant task snooping via incremental or predictable task IDs | Task IDs are UUIDv4, but no tenant authorization check on `GET /api/v1/tasks/{id}` | Tenant ownership verification: task must belong to authenticated tenant | High (Pre-9.1) → Low (Post-9.1) |
| **Denial of Service** | T-D-01 | API resource exhaustion via unbounded concurrent task ingestion | No rate limiting; API accepts unlimited concurrent requests | Sliding-window log rate limiting (Redis Lua-backed sorted set) returning HTTP `429 Too Many Requests` | High (Pre-9.1) → Low (Post-9.1) |
| **Denial of Service** | T-D-02 | Database connection pool starvation via slow-query injection | Pool size default 10 connections; slow queries can starve pool | Dedicated pool sizing, statement timeouts (e.g. 5s), and saturation circuit breakers | Medium (Pre-9.1) → Low (Post-9.1) |
| **Elevation of Privilege** | T-E-01 | Regular client invoking operator administrative functions | Maintenance / recovery endpoints accessible without role check | Role-Based Access Control (RBAC): `submitter` vs `operator` scopes | High (Pre-9.1) → Low (Post-9.1) |

---

## 3. High-Priority Requirements for Milestone 9.1

1. **Authentication:** Bearer token authentication via HTTP `Authorization: Bearer <token>` header with format `tf_live_<24-hex-key-id>_<32-char-secret>`. Persisted with `hashed_secret` (SHA-256 digest).
2. **Authorization & RBAC:**
   - Scope `tasks:write`: Allowed to submit tasks within own tenant.
   - Scope `tasks:read`: Allowed to inspect tasks within own tenant.
   - Scope `admin:operator`: Allowed to trigger lease recovery, cancel tasks, or view global system health.
3. **Tenant Scoping:** All task queries must include `WHERE tenant_id = :authenticated_tenant_id`.
4. **Rate Limiting:** Distributed Redis-backed sliding window / token bucket algorithm with headers:
   - `X-RateLimit-Limit`
   - `X-RateLimit-Remaining`
   - `Retry-After` (on 429)
5. **Secret Sanitization:** Automated linting and test asserting zero raw database connection strings, passwords, or lease tokens in logs or responses.

