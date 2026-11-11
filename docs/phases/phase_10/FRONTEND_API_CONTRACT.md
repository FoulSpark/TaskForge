# Phase 10 — Frontend API Contract Specification (`F0`)

**Scope:** Verified Backend Endpoints, Phase 10 Additive Contracts & TypeScript Interface Definitions  
**Baseline Backend Audited:** `v0.9.0-phase9-freeze` (`src/taskforge/api/schemas.py`, `routes/tasks.py`, `routes/metrics.py`, `routes/workers.py`, `routes/operator.py`, `errors.py`, `main.py`)  

---

## 1. HTTP Transport, Headers & Error Envelope

### 1.1 Standard Request Headers
- `Authorization: Bearer tf_live_<24-hex-key_id>_<32-secret>` (injected from React in-memory auth state for all `/api/v1/*` requests except unauthenticated health probes).
- `Content-Type: application/json`
- `X-Correlation-ID`: Optional client UUID echoed by `CorrelationIdMiddleware` in response headers.

### 1.2 Rate-Limit & Correlation Response Headers
- `X-Correlation-ID`: Server-assigned or echoed request correlation UUID (`src/taskforge/api/middleware.py`).
- `X-RateLimit-Limit`: Configured quota (default `300`).
- `X-RateLimit-Remaining`: Remaining requests in rolling 60s window.
- `Retry-After`: Seconds until retry eligibility (present on `429` and fail-closed `503`).

### 1.3 Canonical Error Envelope (`src/taskforge/api/schemas.py` & `errors.py`)
All HTTP `4xx` / `5xx` errors emit `APIErrorResponse`:

```typescript
export interface ErrorDetail {
  code: string; // e.g. "VALIDATION_ERROR", "HTTP_401", "HTTP_403", "HTTP_404", "HTTP_409", "INVALID_STATE_TRANSITION", "IDEMPOTENCY_CONFLICT", "HTTP_429", "HTTP_503"
  message: string;
  details?: unknown;
  correlation_id?: string | null;
}

export interface APIErrorResponse {
  error: ErrorDetail;
}
```

---

## 2. Existing Phase 9 Endpoints (Consumed As-Is)

| Endpoint | Method | Required Scope | Request / Query Schema | Response Schema |
| :--- | :---: | :--- | :--- | :--- |
| `/api/v1/tasks` | `POST` | `tasks:write` | `TaskCreateRequest` | `TaskResponse` (`201 Created` or `200 OK` on idempotent match) |
| `/api/v1/tasks` | `GET` | `tasks:read` | `status?`, `task_type?`, `priority?` (`1..4`), `limit` (`1..100`), `offset` (`>=0`) | `TaskListResponse` (strictly filtered by `principal.tenant_id`) |
| `/api/v1/tasks/{task_id}` | `GET` | `tasks:read` | Path `task_id` | `TaskResponse` (includes nested `attempts`, `result`, `events`; `404` if cross-tenant) |
| `/api/v1/tasks/{task_id}/cancel` | `POST` | `tasks:write` | Path `task_id` | `TaskResponse` (`409` if already terminal) |
| `/api/v1/tasks/{task_id}/retry` | `POST` | `tasks:write` | Path `task_id` | `TaskResponse` (`409` if not `FAILED` / `DEAD_LETTER`) |
| `/api/v1/operator/recover` | `POST` | `admin:operator` | — | `{ recovered_count: number; reclaimed_broker_count: number }` |
| `/api/v1/operator/outbox-sweep` | `POST` | `admin:operator` | — | `{ published_count: number }` |
| `/api/v1/operator/purge-queue` | `POST` | `admin:operator` | `{ queue_name: string; confirm_purge: boolean }` | `{ queue_name: string; purged: boolean; purged_items: number }` |
| `/health/live` (`/healthz/live`) | `GET` | Public | — | `{ status: "alive"; application: string }` |
| `/health/ready` (`/healthz/ready`) | `GET` | Public | — | `{ status: "ready" \| "not_ready"; application: string; checks: { database: "healthy" \| "unavailable"; redis: "healthy" \| "unavailable" }; timestamp?: string }` |

### 2.1 Existing Task TypeScript Interfaces (`src/taskforge/api/schemas.py`)

```typescript
export type TaskStatus =
  | "PENDING"
  | "QUEUED"
  | "RUNNING"
  | "SUCCESS"
  | "RETRY_WAIT"
  | "FAILED"
  | "DEAD_LETTER"
  | "CANCELLED";

export type TaskPriority = 1 | 2 | 3 | 4; // 1=LOW, 2=MEDIUM, 3=HIGH, 4=CRITICAL

export interface TaskCreateRequest {
  task_name: string; // 1..128 chars
  task_type: string; // 1..64 chars
  payload: Record<string, unknown>;
  priority?: TaskPriority; // default 2
  idempotency_key?: string | null; // <=128 chars
  timeout_seconds?: number; // 1..3600, default 60
  max_retries?: number; // 0..10, default 3
  scheduled_at?: string | null; // ISO 8601 UTC timestamp
}

export interface TaskAttemptResponse {
  id: number;
  attempt_number: number;
  worker_id: string | null;
  status: string;
  error_message: string | null;
  started_at: string;
  completed_at: string | null;
  duration_ms: number | null;
}

export interface TaskResultResponse {
  id: number;
  return_value: Record<string, unknown> | null;
  execution_metadata: Record<string, unknown> | null;
  created_at: string;
}

export interface TaskEventResponse {
  id: number;
  event_type: string;
  from_state: string | null;
  to_state: string;
  metadata_json: Record<string, unknown> | null;
  created_at: string;
}

export interface TaskResponse {
  id: string;
  task_name: string;
  task_type: string;
  payload: Record<string, unknown>;
  status: TaskStatus;
  priority: TaskPriority;
  idempotency_key: string | null;
  timeout_seconds: number;
  max_retries: number;
  retry_count: number;
  scheduled_at: string | null;
  started_at: string | null;
  completed_at: string | null;
  created_at: string;
  updated_at: string;
  // Security Contract (Gap G4): lease_token and lease_expires_at are NEVER returned.
  attempts: TaskAttemptResponse[];
  result: TaskResultResponse | null;
  events: TaskEventResponse[];
}

export interface TaskListResponse {
  items: TaskResponse[];
  total: number;
  limit: number;
  offset: number;
  has_more: boolean;
}
```

---

## 3. Phase 10 Additive & Hardened Backend Contracts (To Implement in `F1`)

To resolve Gaps `G1`, `G2`, and `G3` without changing Prometheus cardinality or breaking Phase 9 backend tests:

### 3.1 Gap `G1` Resolution — Principal Introspection (`GET /api/v1/auth/me`)
- **Required Scope:** Valid active Bearer token (`Depends(get_current_principal)`).
- **Behavior:** Returns authenticated identity and RBAC scopes for UI authorization gating; never returns `hashed_secret` or echoes the raw Bearer token.

```typescript
export interface AuthPrincipalResponse {
  key_id: string;
  tenant_id: string;
  name: string;
  scopes: Array<"tasks:read" | "tasks:write" | "admin:operator" | string>;
}
```

### 3.2 Gap `G3` Resolution — Tenant-Scoped `/api/v1/stats` & Operator `/api/v1/operator/system-stats`
- **Hardened `GET /api/v1/stats` (`require_scope("tasks:read")`):**
  - Enforces `WHERE tenant_id = :principal_tenant_id` on `tasks` and `task_attempts` queries.
  - Returns **only** the authenticated tenant's status counts (`total`, `pending`, `queued`, `running`, `succeeded`, `failed`, `retry_wait`, `dead_letter`, `cancelled`) and tenant average execution duration (`performance.avg_execution_ms`).
  - Does **not** expose global cross-tenant counts, global Redis queue depth, or internal worker hostnames to ordinary tenant tokens (`workers` and `queue_depth` are populated **only** when `"admin:operator" in principal.scopes`, preserving backward compatibility for legacy operator-scoped test runners without leaking global data to tenant keys).
- **Explicit System-Wide Operator Endpoint (`GET /api/v1/operator/system-stats`, `require_scope("admin:operator")`):**
  - Exposes cluster-wide task counts across all tenants, Redis queue depth by priority tier (`critical`, `high`, `medium`, `low`), and active worker list (`get_active_workers()`) exclusively for authorized `admin:operator` principals. (`GET /api/v1/workers` is likewise guarded by `require_scope("admin:operator")` in Phase 10 so worker hostnames/heartbeats are never exposed to unauthenticated or ordinary tenant callers.)

```typescript
export interface TenantStatsResponse {
  timestamp: number;
  tenant_id: string;
  scope: "tenant";
  tasks: {
    total: number;
    pending: number;
    queued: number;
    running: number;
    succeeded: number;
    failed: number;
    retry_wait: number;
    dead_letter: number;
    cancelled: number;
  };
  performance: {
    avg_execution_ms: number;
    queue_depth?: number | null; // null/omitted for non-operator tenants
  };
  workers?: null | {
    active_count: number;
    list: WorkerHeartbeatInfo[];
  };
}

export interface WorkerHeartbeatInfo {
  worker_id: string;
  hostname: string;
  concurrency: number;
  active_tasks: number;
  started_at: string;
  last_heartbeat: string;
  status: string;
}

export interface OperatorSystemStatsResponse {
  timestamp: number;
  scope: "system";
  tasks: TenantStatsResponse["tasks"];
  performance: {
    avg_execution_ms: number;
    queue_depth: number;
    queue_depth_by_priority: Record<"critical" | "high" | "medium" | "low", number>;
  };
  workers: {
    active_count: number;
    list: WorkerHeartbeatInfo[];
  };
}
```

### 3.3 Gap `G2` & `R1`/`R2` Resolution — Scoped JSON Monitoring Summary (`GET /api/v1/monitoring/summary`)
- **Required Scope:** `require_scope("tasks:read")`, query param `window_minutes: int = Query(5, ge=1, le=60)` (default rolling **5-minute** window). **Never** accept a client `tenant_id` override; tenant identity comes strictly from `principal.tenant_id`.
- **Exact Non-Conflated Lifecycle Metric Semantics (`R1`):**
  - **Throughput:** Completed tenant tasks (`status IN ('SUCCESS', 'DEAD_LETTER', 'CANCELLED')` with `completed_at` in `[now - window, now]`) returning `observation_count`, `window_seconds`, `tasks_per_second` (`observation_count / window_seconds`), and `tasks_per_minute`.
  - **Population Isolation Rule:** Completed-task/attempt observations (`population: "completed_tasks_only"` or `"completed_attempts_only"`) **never** mix active `RUNNING` attempts into completed latency percentiles.
  - **Strict Lifecycle Stage Definitions (matching Phase 9 Stage 2 R1):**
    * **`outbox_dwell`:** `outbox.published_at - task.created_at` ($t_{\text{published}} - t_{\text{ingested}}$) over published `TASK_SUBMITTED` outbox events belonging to completed/published tenant tasks inside the rolling window (`population: "published_submitted_outbox_events"`).
    * **`queue_dwell`:** `attempt.started_at - outbox.published_at` ($t_{\text{started}} - t_{\text{published}}$) over completed initial attempts joined to their published `TASK_SUBMITTED` outbox event (`population: "completed_initial_attempts_with_outbox"`). **Strictly excludes outbox publication delay.**
    * **`submission_to_start`:** `attempt.started_at - task.created_at` ($t_{\text{started}} - t_{\text{ingested}}$) explicitly labeled as total pre-execution submission-to-start latency over completed attempts (`population: "completed_attempts_only"`). Never mislabeled as `queue_wait`.
    * **`handler_duration`:** `TaskAttempt.duration_ms` strictly over completed attempts (`completed_at IS NOT NULL AND duration_ms IS NOT NULL`, `population: "completed_attempts_only"`).
    * **`worker_lease_tenure`:** `task.completed_at - attempt.started_at` ($t_{\text{completed}} - t_{\text{started}}$) over completed tasks/final successful attempts (`population: "completed_tasks_with_attempt"`), including lease ownership and durable PostgreSQL persistence overhead.
    * **`end_to_end`:** `task.completed_at - task.created_at` ($t_{\text{completed}} - t_{\text{ingested}}$) over completed tenant tasks in `[now - window, now]` (`population: "completed_tasks_only"`).
  - **Empty & Low-Sample Behavior:** When `sample_count === 0`, `p50_ms`, `p95_ms`, and `p99_ms` MUST be `null` (never fabricated `0.0`). Includes `minimum_recommended_samples: 20` and `sufficient_sample_size: sample_count >= 20` so the UI surfaces an insufficient-sample notice instead of misleading tail precision.
- **Authorization & Data Scope (`R2`):**
  - `throughput` and `latencies_ms`: strictly filtered by `principal.tenant_id`.
  - `health`: ordinary tenants see only coarse service availability `{ overall: "healthy" | "degraded", database: "healthy" | "unavailable", redis: "healthy" | "unavailable" }` with zero hostnames, URLs, ports, DSNs, or pool internals.
  - `cluster`: populated **only** when `"admin:operator" in principal.scopes`, else **`null`** (`queue_depth_by_priority`, `outbox_pending_events`, `outbox_lag_seconds`, `workers`, `db_pool`). Zero credentials or connection strings ever included.
  - **Zero Cardinality Impact:** Adds **`0` new Prometheus metric series**.

```typescript
export interface LatencyPercentileSummary {
  sample_count: number;
  sufficient_sample_size: boolean;
  minimum_recommended_samples: number;
  method: "exact_sql_observations";
  population: string;
  p50_ms: number | null;
  p95_ms: number | null;
  p99_ms: number | null;
}

export interface MonitoringSummaryResponse {
  timestamp_utc: string;
  window_minutes: number;
  window_seconds: number;
  tenant_id: string;
  health: {
    overall: "healthy" | "degraded";
    database: "healthy" | "unavailable";
    redis: "healthy" | "unavailable";
  };
  throughput: {
    observation_count: number;
    tasks_per_second: number;
    tasks_per_minute: number;
  };
  latencies_ms: {
    outbox_dwell: LatencyPercentileSummary;
    queue_dwell: LatencyPercentileSummary;
    submission_to_start: LatencyPercentileSummary;
    handler_duration: LatencyPercentileSummary;
    worker_lease_tenure: LatencyPercentileSummary;
    end_to_end: LatencyPercentileSummary;
  };
  cluster: null | {
    queue_depth_by_priority: Record<"critical" | "high" | "medium" | "low", number>;
    outbox_pending_events: number;
    outbox_lag_seconds: number;
    workers: {
      active_count: number;
      total_concurrency: number;
      active_tasks: number;
    };
    db_pool: {
      checked_out: number;
      idle: number;
      overflow: number;
    };
  };
}
```

### 3.4 `R4` Resolution — Registered Task-Type Catalog (`GET /api/v1/task-types`)
- **Required Scope:** `require_scope("tasks:read")`.
- **Contract (`R4`):** Returns structured metadata for every task handler defined in `src/taskforge/tasks/handlers.py` alongside live `TaskRegistry` registration checks (`available: boolean`) and authoritative submission bounds (`TaskCreateRequest`). Frontend form checks mirror these schemas solely for UX convenience; backend Pydantic + `TaskService` remain authoritative.

```typescript
export interface TaskTypeCatalogItem {
  task_type: string;
  display_name: string;
  category: "workload" | "benchmark" | "fault_injection";
  description: string;
  available: boolean;
  default_payload: Record<string, unknown>;
  payload_schema: Record<string, unknown>; // JSON Schema describing fields & limits
}

export interface TaskTypesCatalogResponse {
  items: TaskTypeCatalogItem[];
  submission_constraints: {
    priority: { min: 1; max: 4; default: 2 };
    timeout_seconds: { min: 1; max: 3600; default: 60 };
    max_retries: { min: 0; max: 10; default: 3 };
    task_name: { min_length: 1; max_length: 128 };
    idempotency_key: { max_length: 128 };
  };
}
```

---

## 4. Compatibility Rule for Legacy Phase 9 Integration Tests

Phase 9 integration tests (`tests/integration/test_health.py`, `test_observability.py`, `test_tasks_api.py`, etc.) run under `TASKFORGE_TESTING=1` without explicit Bearer headers and invoke `get_current_principal()`'s test bypass (`legacy-test-runner` with all scopes).
In Phase 10 `F1`:
- Gating `/api/v1/stats` and `/api/v1/workers` with `Depends(require_scope(...))` preserves full backward compatibility for existing tests running with `TASKFORGE_TESTING=1` (since `legacy-test-runner` includes `tasks:read` and `admin:operator`), while enforcing strict `401` / `403` / tenant scoping in runtime and strict-auth tests (`TASKFORGE_STRICT_AUTH=1`).
- Keep `GET /api/v1/stats` JSON keys (`timestamp`, `tasks`, `performance`, `workers` where permitted or preserved under test mode) backward-verified against `tests/integration/test_observability.py` during `F1`!

