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
  id: string;
  attempt_number: number;
  worker_id: string;
  status: string;
  started_at: string | null;
  completed_at: string | null;
  duration_ms: number | null;
  error_message: string | null;
}

export interface TaskResultData {
  id: string;
  return_value: unknown;
  result_size_bytes: number;
  created_at: string;
}

export interface TaskEventItem {
  id: string;
  event_type: string;
  from_status: string | null;
  to_status: string | null;
  reason: string | null;
  created_at: string;
}

export interface TaskItem {
  id: string;
  task_name: string;
  task_type: string;
  payload: Record<string, unknown>;
  status: TaskStatus;
  priority: TaskPriority;
  tenant_id: string;
  idempotency_key: string | null;
  retry_count: number;
  max_retries: number;
  timeout_seconds: number;
  scheduled_at: string | null;
  started_at: string | null;
  completed_at: string | null;
  created_at: string;
  updated_at: string;
  error_message: string | null;
  error_type: string | null;
  attempts?: TaskAttemptItem[];
  result?: TaskResultData | null;
  events?: TaskEventItem[];
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
  hostname?: string | null;
  concurrency?: number;
  active_tasks?: number;
  status?: string;
  last_heartbeat?: string | null;
}

export interface TenantStatsResponse {
  scope: "tenant";
  tenant_id: string;
  tasks: TenantStatsTasksSummary;
  performance: TenantStatsPerformanceSummary;
  workers: WorkerHeartbeatInfo[] | null;
}

export interface OperatorSystemStatsResponse {
  scope: "system";
  tasks: TenantStatsTasksSummary;
  performance: {
    avg_execution_ms: number;
    queue_depth_total: number;
    queue_depth_by_priority: Record<string, number>;
  };
  workers: {
    active_count: number;
    items: WorkerHeartbeatInfo[];
  };
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

