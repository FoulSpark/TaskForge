export type TaskStatus =
  | "PENDING"
  | "QUEUED"
  | "RUNNING"
  | "SUCCESS"
  | "RETRY_WAIT"
  | "FAILED"
  | "DEAD_LETTER"
  | "CANCELLED";

export type TaskPriority = 1 | 2 | 3 | 4;

export interface AuthPrincipal {
  key_id: string;
  tenant_id: string;
  name: string;
  scopes: string[];
}

export interface TaskAttemptItem {
  id: number;
  attempt_number: number;
  worker_id: string | null;
  status: string;
  started_at: string;
  completed_at: string | null;
  duration_ms: number | null;
  error_message: string | null;
}

export interface TaskResultData {
  id: number;
  return_value: Record<string, unknown> | null;
  execution_metadata: Record<string, unknown> | null;
  created_at: string;
}

export interface TaskEventItem {
  id: number;
  event_type: string;
  from_state: string | null;
  to_state: string;
  metadata_json: Record<string, unknown> | null;
  created_at: string;
}

export interface TaskItem {
  id: string;
  task_name: string;
  task_type: string;
  payload: Record<string, unknown>;
  status: TaskStatus;
  priority: TaskPriority;
  idempotency_key: string | null;
  retry_count: number;
  max_retries: number;
  timeout_seconds: number;
  scheduled_at: string | null;
  started_at: string | null;
  completed_at: string | null;
  created_at: string;
  updated_at: string;
  definition_id?: string | null;
  definition_version_id?: string | null;
  attempts: TaskAttemptItem[];
  result: TaskResultData | null;
  events: TaskEventItem[];
}

export interface TaskListResponse {
  items: TaskItem[];
  total: number;
  limit: number;
  offset: number;
  has_more: boolean;
}

export interface TaskCreateInput {
  task_name: string;
  task_type: string;
  payload: Record<string, unknown>;
  priority: TaskPriority;
  timeout_seconds: number;
  max_retries: number;
  idempotency_key?: string;
  scheduled_at?: string;
}

export interface TaskSubmissionOutcome {
  task: TaskItem;
  status: number;
  isReplay: boolean;
}

export interface TaskTypeCatalogItem {
  task_type: string;
  display_name: string;
  category: string;
  description: string;
  default_payload: Record<string, unknown>;
  payload_schema: Record<string, unknown>;
  available: boolean;
}

export interface SubmissionConstraints {
  priority: { min: number; max: number; default: number };
  timeout_seconds: { min: number; max: number; default: number };
  max_retries: { min: number; max: number; default: number };
  task_name: { min_length: number; max_length: number };
  idempotency_key: { max_length: number };
}

export interface TaskTypesCatalogResponse {
  items: TaskTypeCatalogItem[];
  submission_constraints: SubmissionConstraints;
}

export interface TenantStatsTasksSummary {
  total: number;
  pending: number;
  queued: number;
  running: number;
  succeeded: number;
  failed: number;
  retry_wait: number;
  dead_letter: number;
  cancelled: number;
}

export interface TenantStatsPerformanceSummary {
  avg_execution_ms: number;
  queue_depth?: number | null;
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

export interface WorkersSummary {
  active_count: number;
  list: WorkerHeartbeatInfo[];
}

export interface TenantStatsResponse {
  timestamp: number;
  scope: "tenant";
  tenant_id: string;
  tasks: TenantStatsTasksSummary;
  performance: TenantStatsPerformanceSummary;
  workers: WorkersSummary | null;
}

export interface OperatorSystemStatsResponse {
  timestamp: number;
  scope: "system";
  tasks: TenantStatsTasksSummary;
  performance: {
    avg_execution_ms: number;
    queue_depth: number;
    queue_depth_by_priority: Record<string, number>;
  };
  workers: WorkersSummary;
}

export interface LatencyPercentileSummary {
  sample_count: number;
  sufficient_sample_size: boolean;
  minimum_recommended_samples: number;
  method: string;
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
  cluster: {
    queue_depth_by_priority: Record<string, number>;
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
  } | null;
}

export interface HealthReadinessResponse {
  status: string;
  checks?: {
    database?: string;
    redis?: string;
  };
}

export interface ApiErrorBody {
  error?: {
    code?: string;
    message?: string;
    details?: unknown;
    correlation_id?: string;
  };
}

// ---------------------------------------------------------------------------
// Phase 11 Custom Task Definition Registry & Execution Types
// ---------------------------------------------------------------------------

export interface DefinitionExecuteRequest {
  version?: number;
  input: Record<string, unknown>;
  task_name?: string;
  priority?: TaskPriority;
  idempotency_key?: string;
  timeout_seconds?: number;
}

export interface DefinitionExecutionSummary {
  id: string;
  task_name: string;
  task_type: string;
  status: string;
  priority: number;
  definition_id: string;
  definition_version_id?: string | null;
  version_number?: number | null;
  created_at: string;
  started_at?: string | null;
  completed_at?: string | null;
  duration_ms?: number | null;
}

export interface DefinitionExecutionListResponse {
  items: DefinitionExecutionSummary[];
  total: number;
  limit: number;
  offset: number;
}

export interface DefinitionExecutionVersionSnapshot {
  id: string;
  version_number: number;
  task_type: string;
  content_hash: string;
}

export interface DefinitionExecutionFailureDiagnosis {
  classification: string;
  error_message?: string | null;
  retryable: boolean;
  is_timeout: boolean;
  retries_exhausted: boolean;
}

export interface DefinitionExecutionDetailResponse {
  id: string;
  task_name: string;
  task_type: string;
  status: string;
  priority: number;
  definition_id: string;
  definition_version_id?: string | null;
  version?: DefinitionExecutionVersionSnapshot | null;
  payload: Record<string, unknown>;
  result?: Record<string, unknown> | null;
  failure_diagnosis?: DefinitionExecutionFailureDiagnosis | null;
  attempts: TaskAttemptItem[];
  timeline: {
    created_at: string;
    started_at?: string | null;
    completed_at?: string | null;
  };
}

export interface DefinitionAuditEventItem {
  id: string;
  tenant_id: string;
  definition_id: string;
  version_id?: string | null;
  actor_id: string;
  action: string;
  metadata: Record<string, unknown>;
  created_at: string;
}

export interface DefinitionAuditListResponse {
  items: DefinitionAuditEventItem[];
  total: number;
  limit: number;
  offset: number;
}

export interface DefinitionCancelOutcome {
  id: string;
  status: string;
  message: string;
}


