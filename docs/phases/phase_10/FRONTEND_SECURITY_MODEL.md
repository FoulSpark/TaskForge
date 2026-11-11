# Phase 10 — Frontend Security Model (`F0`)

**Scope:** Browser Token Handling, Server-Side Tenant Isolation, RBAC Enforcement, Rate-Limit Handling & XSS/Secret Hygiene  

---

## 1. Threat Assumptions & Security Boundaries

TaskForge is a multi-tenant task processing platform protected by scoped API keys (`tf_live_<24-hex-key_id>_<32-secret>`) and a fail-closed Redis sliding-window rate limiter. A browser UI introduces client-side persistence, cross-tenant information disclosure, and DOM-injection attack surfaces that do not exist in CLI/API usage.

**Core Security Principle:** The React UI **never** enforces authorization or tenant isolation on behalf of the backend. Every permission check and tenant boundary must be enforced server-side in FastAPI (`require_scope`, `tenant_id` SQL filters) and treated purely as UX hint gating on the client.

---

## 2. Browser Authentication Model (Gap `G1`)

### 2.1 In-Memory Token Policy (Phase 10 Prototype Model)
- **Entry Mechanism:** On launch without an active principal, the application renders a minimal Sign-In Gate prompting the user for an API key (`<input type="password">`).
- **Validation Step:** The frontend validates the key by calling `GET /api/v1/auth/me` with `Authorization: Bearer <token>`. On `200 OK`, React stores the plaintext token **exclusively in React in-memory state** (`AuthContext` closure / `useRef`) alongside the parsed `AuthPrincipalResponse` (`key_id`, `tenant_id`, `name`, `scopes`).
- **Hard Prohibitions:**
  1. **Zero Browser Persistence:** `localStorage`, `sessionStorage`, `IndexedDB`, and non-HttpOnly `document.cookie` are strictly forbidden for storing API keys.
  2. **Zero URL / Console / Bundle Exposure:** API keys must never be placed in URL query strings, hash fragments, React `window` globals, `VITE_*` build-time env vars, `console.*` logs, or error boundaries.
  3. **Explicit Clear Paths:** Full page reload, browser tab close, clicking **Sign Out**, or receiving HTTP `401 Unauthorized` immediately zeros the in-memory token reference and reverts to the Sign-In Gate.

### 2.2 Future Production Upgrade Path
In-memory API-key authentication is appropriate for a local-first single-user demonstration console. For a multi-user browser deployment, the recommended upgrade replaces direct API-key entry with an **OAuth2 / BFF Session Exchange** setting an `HttpOnly`, `Secure`, `SameSite=Strict` session cookie backed by CSRF double-submit or custom header verification—eliminating bearer tokens from JavaScript memory entirely.

---

## 3. Server-Side Tenant Isolation & RBAC Matrix (Gap `G3`)

> **Tenant isolation is a server-side security requirement, not a React UI preference.**

| Resource / Action | Ordinary Tenant (`tasks:read` only) | Writer Tenant (`tasks:read`, `tasks:write`) | Operator (`admin:operator`) | Enforcement Layer |
| :--- | :---: | :---: | :---: | :--- |
| Tenant Overview KPIs (`/api/v1/stats`) | Tenant-scoped only | Tenant-scoped only | Tenant-scoped (+ `/operator/system-stats`) | **FastAPI SQL `WHERE tenant_id = :tid`** |
| Task List & Detail (`/api/v1/tasks*`) | Tenant-scoped (`404` on foreign ID) | Tenant-scoped (`404` on foreign ID) | Tenant-scoped (`404` on foreign ID) | **FastAPI `TaskRepository` filter** |
| Submit / Cancel / Retry Task | Disabled in UI (`403` if called) | Allowed for own tenant (`409` on illegal state) | Allowed for own tenant | **FastAPI `require_scope("tasks:write")`** |
| Monitoring Summary (`/api/v1/monitoring/summary`) | `health`, tenant `throughput`, tenant `latencies_ms` (`cluster: null`) | Same (`cluster: null`) | Full `cluster` infrastructure block (`queue_depth_by_priority`, `outbox`, `workers`, `db_pool`) | **FastAPI scope guard** |
| Active Workers & Operator Actions (`/workers`, `/operator/*`) | Hidden in UI (`403` if called) | Hidden in UI (`403` if called) | Visible & permitted | **FastAPI `require_scope("admin:operator")`** |

---

## 4. Lease & Secret Hygiene (Gap `G4`)

1. **Lease Token Non-Exposure:** `TaskResponse` omits `tasks.lease_token` and `tasks.lease_expires_at` at the Pydantic serialization boundary (`src/taskforge/api/schemas.py`). The UI renders operational context cleanly using `attempts[]` (`attempt_number`, `worker_id`, `status`, `error_message`, `duration_ms`) and `events[]` (`LEASE_EXPIRED_RECOVERED`) without ever receiving lease tokens.
2. **Error Sanitization & Correlation IDs:** Backend `mask_secret_string()` masks DB URLs, `tf_live_...` secrets, and lease tokens before returning errors. The UI renders `error.message` and `error.correlation_id` as plain text (React JSX auto-escaping; zero `dangerouslySetInnerHTML`).
3. **XSS & JSON Rendering Safety:** Task payloads, return values, and tracebacks are formatted strictly as escaped text nodes (`JSON.stringify(..., null, 2)` inside `<pre>`), never evaluated or injected as HTML.

---

## 5. Rate-Limit & Outage Protection (`429` / `503`)

Because TaskForge's `RateLimitMiddleware` uses a rolling 60-second quota (`300 req/min` default) and **fails closed (`HTTP 503`)** when Redis is unreachable:
- The frontend **Polling Manager** must honor `Retry-After` headers on both `429` and `503`, immediately halting automatic polling timers during cooldowns so the browser never amplifies an outage or triggers thundering-herd retries.
- Client API `POST` submissions (`Submit Task`, `Cancel`, `Retry`, `Operator Sweep/Recover`) disable buttons while inflight and never auto-retry non-idempotent mutations on `429`/`503`.

---

## 6. Hosting & CORS Security Posture (Gap `G5`)

- **Development:** Vite proxies `/api` and `/health` to `http://127.0.0.1:8000` (same-origin from browser perspective at `http://localhost:5173`).
- **Production / Docker:** Built static bundle (`frontend/dist/`) is served directly on the same origin via FastAPI static mount or an Nginx reverse proxy routing `/api/*` and `/health/*` to `api:8000`. Broad wildcard CORS (`Access-Control-Allow-Origin: *`) is explicitly prohibited.

