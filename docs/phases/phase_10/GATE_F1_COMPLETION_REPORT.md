# Phase 10 — Gate F1 Completion Report

**Branch:** `phase-10-frontend`  
**Phase 9 Freeze Tag:** `v0.9.0-phase9-freeze` (immutable)  
**F1-A Backend Commit:** `e917c1b`  
**F1-B Frontend & Integration Scope:** Completed and verified on `phase-10-frontend`

---

## 1. Gate F1 Deliverables

### F1-A — Additive Backend Contracts & Authorization Boundaries (`e917c1b`)
- `src/taskforge/api/routes/auth.py`: Added `GET /api/v1/auth/me` returning `AuthPrincipalResponse` (`key_id`, `tenant_id`, `name`, `scopes`) via `get_current_principal`.
- `src/taskforge/tasks/catalog.py` & `src/taskforge/api/routes/task_types.py`: Added `GET /api/v1/task-types` guarded by `require_scope("tasks:read")`, deriving availability dynamically from `list_registered_handlers()` alongside `default_payload`, `payload_schema`, and `SUBMISSION_CONSTRAINTS`.
- `src/taskforge/monitoring/summary.py` & `src/taskforge/api/routes/metrics.py`:
  - Scoped `GET /api/v1/stats` by `principal.tenant_id` (`scope: "tenant"`) with `require_scope("tasks:read")`, omitting global `workers` and `queue_depth` unless `admin:operator` scope is held.
  - Added `GET /api/v1/monitoring/summary` (`window_minutes=5` default, selectable `1..1440`) computing strict `R1`-separated causal lifecycle percentiles over completed populations (`outbox_dwell`, `queue_dwell`, `submission_to_start`, `handler_duration`, `worker_lease_tenure`, `end_to_end`), `null` percentiles when `sample_count == 0`, `minimum_recommended_samples = 20`, coarse `health`, and operator-only `cluster`.
- `src/taskforge/api/routes/operator.py` & `src/taskforge/api/routes/workers.py`: Added `GET /api/v1/operator/system-stats` and guarded `GET /api/v1/workers` with `require_scope("admin:operator")`.
- `tests/integration/test_f1a_frontend_api_contracts.py`: Added contract, key lifecycle (`401`), tenant isolation, `admin:operator` (`403` vs `200`), `R1` causal stage separation, empty-window `null` percentile, and zero Prometheus cardinality delta tests.

### F1-B — React 18 + TypeScript + Vite Foundation & Vertical Slice
- **Frontend Application (`frontend/`):**
  - `frontend/src/api/types.ts` & `frontend/src/api/client.ts`: Typed `TaskForgeClient` injecting `Authorization: Bearer <key>`, `5000ms` `AbortController` timeout, `401` immediate session invalidation callback, `403` non-destroying authorization notice callback, and `Retry-After` parsing for `429`/`503`.
  - `frontend/src/auth/AuthContext.tsx`: Strictly in-memory credential holder (`useRef<string | null>` + React state); zero reads or writes to `localStorage`, `sessionStorage`, or `document.cookie`.
  - `frontend/src/hooks/useVisibilityPolling.ts`: 2.5s default cadence polling hook that pauses when `document.visibilityState === "hidden"`, honours `429`/`503` backoff intervals, and immediately halts all future polls on `401`.
  - Views & Navigation (`frontend/src/App.tsx`, `frontend/src/components/`, `frontend/src/pages/`):
    - `SignInPage` (`/`)
    - `ShellLayout` (navigation sidebar, `/health/ready` system pill, tenant badge, operator scope badge, explicit Sign Out button)
    - `OverviewPage` (`/`): tenant KPI counts, optional operator-scoped system overview, recent tenant tasks table
    - `SubmitTaskPage` (`/submit`): runtime handler catalog selector, JSON payload validation, submission bounds, idempotency key support
    - `TaskExplorerPage` (`/tasks`): status, task-type, and priority filters, pagination (`limit=20`), cancel & retry actions
    - `TaskDetailPage` (`/tasks/:taskId`): task metadata (no lease-token fields), result JSON viewer, error viewer, attempt history table, lifecycle events timeline, non-terminal auto-polling
    - `MonitoringPage` (`/monitoring`): `R1`-separated latency percentile table with `<20` low-sample warning and `0` sample badges, coarse dependency health, and operator-gated cluster saturation cards
- **FastAPI Static Serving, CSP & Container Build:**
  - `src/taskforge/main.py`: Serves built `frontend/dist/index.html` and `/assets/*` with `Content-Security-Policy: default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; object-src 'none'; base-uri 'self'; frame-ancestors 'none'` on HTML responses (`/`, `/ui`, `/submit`, `/tasks`, `/tasks/{task_id}`, `/monitoring`), while preserving API JSON responses on `GET /` for non-HTML `Accept` clients.
  - `Dockerfile` & `.dockerignore`: Added `node:22-alpine` `frontend-builder` stage compiling `frontend/dist` into the `runner` image and fixed `builder` context copy ordering (`COPY src ./src` prior to `pip install .`).

---

## 2. Verification Summary

| Check | Command | Result |
| --- | --- | --- |
| Frontend Unit & Lifecycle Tests (Vitest) | `npm test` (`frontend/`) | `2` files passed, `5 passed` (`auth_memory.test.tsx`, `vertical_slice.test.tsx`, including concurrent in-flight `401` session-abort race test & secret non-leakage check) |
| Production TypeScript + Vite Build | `npm run build` (`frontend/`) | `tsc -b` + `vite build` exit `0` (`dist/index.html`, JS + CSS bundles) |
| Frontend Contract, R1–R3 Serving & Isolation Tests | `.venv\Scripts\pytest -v tests/integration/test_f1b_frontend_e2e_serving.py tests/integration/test_f1a_frontend_api_contracts.py tests/integration/test_health.py tests/integration/test_observability.py tests/unit/test_f0_frontend_contract_docs.py` | `20 passed in 2.98s` |
| Docker Multi-Stage Build | `docker compose build api` | Exit `0` (`Image taskforge-api Built`) |
| Docker Container Runtime Smoke (`taskforge-api` + `taskforge-postgres` + `taskforge-redis`) | `docker compose up -d --no-deps api` + HTTP runtime probes against `http://127.0.0.1:8000` | `DOCKER_RUNTIME_SMOKE_PASS` (`/assets/index-LOT2Bd-k.js`, HTML SPA on `/`, `/ui`, `/submit`, `/tasks`, `/tasks/{id}`, `/monitoring` with `script-src 'self'`, `404` JSON on `/api/v1/nonexistent-endpoint`, JSON on `GET /` for non-HTML `Accept`, live `compute_prime` -> `SUCCESS` -> `/monitoring/summary`) |
| Full Backend Regression Suite | `.venv\Scripts\pytest -q` | `280 passed in 499.37s`, `0` failures, `0` skips (prior to `R1`/`R2`/`R3` doc/client hardening; `20` targeted integration/contract tests re-run afterward) |

---

## 3. R1 / R2 / R3 Closure Checklist
- **R1 Monitoring Metric Semantics:** [FRONTEND_API_CONTRACT.md](FRONTEND_API_CONTRACT.md) Section 3.3 explicitly locks all six SQL formulas (`outbox.published_at - task.created_at`, `attempt.started_at - outbox.published_at`, `attempt.started_at - task.created_at`, `TaskAttempt.duration_ms`, `task.completed_at - attempt.started_at`, `task.completed_at - task.created_at`), rolling-window anchors, attempt vs task retry accounting, `null` empty-window quantiles, and `minimum_recommended_samples = 20`.
- **R2 Auth Race & Tenant Isolation:** `AuthContext.tsx` + `TaskForgeClient` abort in-flight session signals (`AbortController`) and check captured `sessionEpoch` before returning responses or firing `onUnauthorized`; `403` leaves sessions intact; Tenant A `?tenant_id=tenant-b` parameter manipulation and key redaction verified in `test_f1b_frontend_e2e_serving.py`.
- **R3 Container Runtime Serving & CSP:** Verified inside running `taskforge-api` container over HTTP `8000` across direct deep-link SPA routes, compiled JS bundle delivery, strict `script-src 'self'` CSP, `404` JSON on unknown `/api/*` paths, and non-HTML `GET /` JSON negotiation.

