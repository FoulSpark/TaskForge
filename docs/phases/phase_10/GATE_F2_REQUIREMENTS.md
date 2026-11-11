# Phase 10 — Gate F2 Requirements & Acceptance Lock (`F2-A` – `F2-E`)

**Branch:** `phase-10-frontend`  
**Phase 9 Freeze Baseline:** `v0.9.0-phase9-freeze` (`fb31546788d565194860568cce87a6f6fa9945a0`, immutable)  
**Prerequisite Closed Gate:** Gate F1 (`e917c1b`, `4150bea`, `9d304bc`)  
**Recommended Implementation Sequence:** `F2-A` -> `F2-C` -> `F2-B` -> `F2-D` -> `F2-E`

---

## 1. Objective & Scope Guardrails

Gate F2 hardens the **TaskForge Web Console** from a working F1 vertical slice into a complete, schema-faithful operational UI verified by real headless browser workflows (`Microsoft Edge` / Playwright against the compiled SPA + FastAPI + PostgreSQL + Redis).

### Non-Negotiable Architecture Constraints
1. PostgreSQL remains the authoritative durable store; Redis remains the broker transport (`at-least-once` execution delivery).
2. The UI must **never** imply exactly-once external side effects; submission/retry controls must explain idempotent deduplication (`idempotency_key`) vs at-least-once execution accurately.
3. Zero exposure of internal `lease_token`, `owner_token`, fencing epoch secrets, API key hashes/secrets, or connection strings anywhere in API responses or frontend components.
4. Zero client-enforced tenant security or cross-tenant filtering shortcuts; tenant scope is strictly server-derived from the API key principal (`principal.tenant_id`).
5. Zero new Prometheus metric series or cardinality increases to drive UI widgets.
6. Keep changes strictly inside `frontend/`, `tests/`, and Phase 10 documentation/contracts (plus additive non-breaking API fixes only if a schema/bug is uncovered, never breaking Phase 9 behavior or modifying `v0.9.0-phase9-freeze`).

### Out of Scope for Gate F2
- Adding Kafka, Celery UI replacements, Kubernetes manifests, or separate GraphQL/WebSocket gateways.
- Building API-key issuance/revocation UI (admin key lifecycle remains CLI/backend script-driven).
- Multi-user SSO/OAuth2 or cookie/Web Storage persistence (in-memory API key auth stays locked per Gate F0/F1).
- Client-side mocking or synthetic percentiles (`0.0` ms fabrication) when observation populations are empty.

---

## 2. Workstream Requirements & Testable Acceptance Criteria

### F2-A — Task Lifecycle Correctness (`SubmitTaskPage` + Action Handlers)
Grounded in `src/taskforge/api/schemas.py` (`TaskCreateRequest`, `TaskResponse`), `src/taskforge/api/routes/tasks.py`, `src/taskforge/core/state_machine.py`, and `src/taskforge/tasks/service.py`:
- **Submission & Catalog Constraints:**
  - Validate `task_name` bounds, JSON object `payload` syntax, `priority` (`1..4`), `timeout_seconds`, and `max_retries` against live `/api/v1/task-types` `submission_constraints`.
  - Support optional future `scheduled_at` ISO timestamp / delay scheduling cleanly in `SubmitTaskPage` (`TaskCreateRequest.scheduled_at`). When submitted with a future `scheduled_at`, surface clearly that the task enters `PENDING` / scheduled wait rather than active `RUNNING` execution.
- **Idempotent Submission UX (`201` vs `200` vs `409`):**
  - Distinguish HTTP `201 Created` (new task enqueued) from HTTP `200 OK` (idempotent replay with matching payload -> navigate/open existing task with an explicit `"Existing idempotent task reused (no duplicate created)"` notice rather than claiming a second task was created).
  - Handle HTTP `409 Conflict` (`IDEMPOTENCY_CONFLICT`) when reusing an `idempotency_key` with differing payload, displaying the conflict error inline without navigating away or clearing form edits.
- **Cancel, Retry & `409` Conflict Safety:**
  - Restrict Cancel actions strictly to `CANCELLABLE_STATES` (`PENDING`, `QUEUED`, `RUNNING`, `RETRY_WAIT`).
  - Restrict Retry actions strictly to backend-supported retry states (`FAILED`, `DEAD_LETTER`) per `TaskService.manual_retry()` (`CANCELLED` and `SUCCESS` must **never** render retry buttons).
  - Never mutate task status optimistically before server confirmation; if a concurrent state change causes HTTP `409 Conflict` (`INVALID_STATE_TRANSITION` / `HTTP_409`), surface the server error message clearly and immediately re-fetch authoritative task state from `GET /api/v1/tasks/{task_id}`.
- **State Immutability & Delivery Semantics:**
  - `SUCCESS` and `CANCELLED` display an explicit immutable terminal badge (`Terminal — immutable`) with zero mutation controls.
  - Scheduled `PENDING` and `RETRY_WAIT` tasks visibly label waiting/backoff state and never render as active `RUNNING` execution.
  - Display clear at-least-once delivery context when submitting or retrying tasks.

### F2-C — Task Detail & Diagnostics (`TaskDetailPage`)
Grounded in exact `TaskResponse` schema (`TaskAttemptResponse`, `TaskResultResponse`, `TaskEventResponse`):
- **Schema Alignment Fix (Caught During F1 Review):**
  - Align `frontend/src/api/types.ts` and `TaskDetailPage.tsx` with exact backend `TaskResponse` fields:
    - `TaskEventResponse`: `id`, `event_type`, `from_state`, `to_state`, `metadata_json`, `created_at` (replacing F1's unused `from_status`/`to_status`/`reason`).
    - `TaskResultResponse`: `id`, `return_value`, `execution_metadata`, `created_at` (replacing unused `result_size_bytes`).
    - `TaskResponse`: omit missing top-level `tenant_id` / `error_message` / `error_type` assumptions; use authenticated `principal.tenant_id` for tenant context and surface failure diagnostics from `attempts` (`error_message`, failure status) and `events` (`metadata_json`).
- **Separation & Chronological Determinism:**
  - Visually separate **Task-Level Summary & Timing** (`status`, `priority`, `created_at`, `scheduled_at`, `started_at`, `completed_at`, end-to-end elapsed vs lease tenure context, `retry_count / max_retries`, `idempotency_key`) from **Attempt-Level Execution Records** (`attempt_number`, `worker_id`, `status`, `duration_ms`, `started_at`, `completed_at`, `error_message`), cleanly distinguishing pure handler `duration_ms` from full worker lease tenure (`completed_at - started_at`).
  - Sort **Attempt History** ascending by `attempt_number` and **Lifecycle Events** deterministically ascending by `(created_at, id)`.
  - Display execution `result.return_value` and `result.execution_metadata` when succeeded, structured attempt/event failure diagnostics when failed/dead-lettered, and never render or request `lease_token` / fencing secrets.

### F2-B — Task Explorer (`TaskExplorerPage`)
- **Server-Side Filters & Pagination:**
  - Filters (`status`, `task_type`, `priority`) and pagination (`limit`, `offset`) must strictly query `GET /api/v1/tasks?...` (zero client-side slicing/filtering over partial datasets).
  - Changing any filter (`status`, `task_type`, `priority`) must immediately reset `offset` to `0`.
- **Deep-Link & URL Query State Synchronization:**
  - Synchronize filter (`status`, `task_type`, `priority`) and pagination (`limit`, `offset` / `page`) state with URL query search parameters (`useSearchParams`), so page refresh or deep-linking (`/tasks?status=FAILED&priority=3&offset=20`) restores the exact backend-filtered view.
  - Sanitize invalid URL params cleanly back to valid bounds without crashing.
- **UX States:**
  - Distinct initial loading skeleton/indicator vs background refresh indicator, clear filter-reset action on empty filtered results, and clean error banner retaining last valid rows on transient failure (`429`/`503` backoff banner).

### F2-D — Operational Monitoring UX (`MonitoringPage`)
- Keep all locked `F1-A` / `R1` definitions verbatim (`outbox_dwell`, `queue_dwell`, `submission_to_start`, `handler_duration`, `worker_lease_tenure`, `end_to_end`).
- Prominently display `population` description, `sample_count`, `minimum_recommended_samples`, and warning badges (`Low N (<20)` / `No observations`); render `—` whenever `p50_ms`/`p95_ms`/`p99_ms` is `null` and visually de-emphasize quantile values when `sufficient_sample_size === false` so insufficient populations are never mistaken for statistically reliable SLAs.
- Clearly separate **Tenant-Scoped Operational View** (`health`, `throughput`, `latencies_ms`) from the **Operator-Only Cluster Section** (`cluster` rendered strictly when `hasScope("admin:operator")` AND `summary.cluster !== null`, plus an explicit non-operator boundary notice when unprivileged).
- Add clear visual distinction between `handler_duration` (attempt handler execution time) and `worker_lease_tenure` (attempt start to durable task completion including lease & persistence overhead).

### F2-E — Real Browser E2E, Auth Security, Responsive & Accessibility Verification
Executed against the **compiled production SPA served by FastAPI (`frontend/dist`) + PostgreSQL + Redis** using headless **Microsoft Edge** via Playwright:
- **Core Workflows:** Interactive browser sign-in -> catalog-driven task submission (standard + duplicate `idempotency_key` reuse verification) -> Task Explorer server-side filter/pagination + deep-link reload -> Task Detail attempt/event timeline inspection & result view -> cancel & manual retry (plus `409` handling verification) -> Monitoring window switching & insufficient-sample visibility check.
- **Auth & Resilience Matrix in Real Browser:**
  - Active polling interrupted by key revocation (`401`) and concurrent in-flight response rejection -> clears React state, halts polling, verifies zero keys in `localStorage`, `sessionStorage`, `document.cookie`, and DOM.
  - `403 Forbidden` (read-only key attempting mutation) preserves signed-in session and shows authorization banner.
  - `429` / `503` backpressure pauses polling cleanly with `Retry-After` banner.
- **Tenant Isolation & Direct Routes:**
  - Cross-tenant key verification (`tenant-a` vs `tenant-b`) and direct deep-link navigation to `/tasks/<id>` and `/monitoring` across fresh browser page loads + sign-in.
- **Keyboard Accessibility & Responsive Viewport:**
  - Tab/keyboard navigation across sign-in form, sidebar nav, task filters, and modal/action controls, plus layout overflow check at desktop (`1280x800`) and mobile (`390x844`) viewports.

---

## 3. Gate F2 Exit Criteria
1. All `F2-A`..`F2-D` Vitest component/integration tests passing cleanly.
2. Production `tsc -b && vite build` clean with zero warnings/errors.
3. Real headless browser E2E suite (`F2-E` via Microsoft Edge) passing 100% against compiled FastAPI SPA + PostgreSQL + Redis.
4. Full backend pytest suite (`pytest -q`) + `ruff check` passing with zero failures/skips on the **final** commit SHA recorded in `GATE_F2_COMPLETION_REPORT.md` (with `benchmarks/scenarios/` clean and `v0.9.0-phase9-freeze` unchanged).
