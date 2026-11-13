from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from taskforge.core.state_machine import TaskPriority


class TaskCreateRequest(BaseModel):
    task_name: str = Field(
        ..., min_length=1, max_length=128, description="Human readable task name"
    )
    task_type: str = Field(
        ..., min_length=1, max_length=64, description="Identifier for the registered handler"
    )
    payload: dict[str, Any] = Field(default_factory=dict, description="Arbitrary task input data")
    priority: int = Field(
        default=TaskPriority.MEDIUM.value,
        ge=1,
        le=4,
        description="Priority from 1 (LOW) to 4 (CRITICAL)",
    )
    idempotency_key: str | None = Field(
        default=None, max_length=128, description="Client-provided unique key for deduplication"
    )
    timeout_seconds: int = Field(
        default=60, ge=1, le=3600, description="Max execution time in seconds"
    )
    max_retries: int = Field(default=3, ge=0, le=10, description="Max retry attempts on failure")
    scheduled_at: datetime | None = Field(
        default=None,
        description="Future timestamp to execute task. If timezone offset is omitted (naive), UTC is assumed.",
    )


class TaskAttemptResponse(BaseModel):
    id: int
    attempt_number: int
    worker_id: str | None = None
    status: str
    error_message: str | None = None
    started_at: datetime
    completed_at: datetime | None = None
    duration_ms: float | None = None

    model_config = ConfigDict(from_attributes=True)


class TaskResultResponse(BaseModel):
    id: int
    return_value: dict[str, Any] | None = None
    execution_metadata: dict[str, Any] | None = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class TaskEventResponse(BaseModel):
    id: int
    event_type: str
    from_state: str | None = None
    to_state: str
    metadata_json: dict[str, Any] | None = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class TaskResponse(BaseModel):
    id: str
    task_name: str
    task_type: str
    payload: dict[str, Any]
    status: str
    priority: int
    idempotency_key: str | None = None
    timeout_seconds: int
    max_retries: int
    retry_count: int
    scheduled_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    created_at: datetime
    updated_at: datetime
    definition_id: str | None = None
    definition_version_id: str | None = None
    attempts: list[TaskAttemptResponse] = Field(default_factory=list)
    result: TaskResultResponse | None = None
    events: list[TaskEventResponse] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class DefinitionExecuteRequest(BaseModel):
    version: int | None = Field(
        None, ge=1, description="Specific version number to execute. Defaults to active version."
    )
    input: dict[str, Any] = Field(
        default_factory=dict, description="Input payload matching definition version input schema."
    )
    task_name: str | None = Field(None, max_length=128, description="Optional custom task name.")
    priority: int = Field(2, ge=1, le=4, description="Task priority (1 to 4).")
    idempotency_key: str | None = Field(
        None, max_length=128, description="Unique idempotency key for deduplication."
    )
    timeout_seconds: int | None = Field(None, ge=1, le=3600, description="Task timeout in seconds.")

    model_config = ConfigDict(extra="forbid")


class DefinitionExecutionSummary(BaseModel):
    id: str
    task_name: str
    task_type: str
    status: str
    priority: int
    definition_id: str
    definition_version_id: str | None = None
    version_number: int | None = None
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_ms: float | None = None

    model_config = ConfigDict(from_attributes=True)


class DefinitionExecutionListResponse(BaseModel):
    items: list[DefinitionExecutionSummary]
    total: int
    limit: int
    offset: int


class DefinitionExecutionVersionSnapshot(BaseModel):
    id: str
    version_number: int
    task_type: str
    content_hash: str

    model_config = ConfigDict(from_attributes=True)


class DefinitionExecutionFailureDiagnosis(BaseModel):
    classification: str
    error_message: str | None = None
    retryable: bool = False
    is_timeout: bool = False
    retries_exhausted: bool = False


class DefinitionExecutionTimeline(BaseModel):
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None


class DefinitionExecutionDetailResponse(BaseModel):
    id: str
    task_name: str
    task_type: str
    status: str
    priority: int
    definition_id: str
    definition_version_id: str | None = None
    version: DefinitionExecutionVersionSnapshot | None = None
    payload: dict[str, Any]
    result: dict[str, Any] | None = None
    failure_diagnosis: DefinitionExecutionFailureDiagnosis | None = None
    attempts: list[TaskAttemptResponse] = Field(default_factory=list)
    timeline: DefinitionExecutionTimeline

    model_config = ConfigDict(from_attributes=True)


class DefinitionAuditEventResponse(BaseModel):
    id: str
    tenant_id: str
    definition_id: str
    version_id: str | None = None
    actor_id: str
    action: str
    metadata: dict[str, Any]
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class DefinitionAuditListResponse(BaseModel):
    items: list[DefinitionAuditEventResponse]
    total: int
    limit: int
    offset: int


class TaskListResponse(BaseModel):
    items: list[TaskResponse]
    total: int
    limit: int
    offset: int
    has_more: bool


class ErrorDetail(BaseModel):
    code: str
    message: str
    details: Any | None = None
    correlation_id: str | None = None


class APIErrorResponse(BaseModel):
    error: ErrorDetail


# ---------------------------------------------------------------------------
# Phase 10 (F1-A) Web Console Contract Schemas
# ---------------------------------------------------------------------------


class AuthPrincipalResponse(BaseModel):
    key_id: str
    tenant_id: str
    name: str
    scopes: list[str]


class TenantStatsTasksSummary(BaseModel):
    total: int
    pending: int
    queued: int
    running: int
    succeeded: int
    failed: int
    retry_wait: int
    dead_letter: int
    cancelled: int


class TenantStatsPerformanceSummary(BaseModel):
    avg_execution_ms: float
    queue_depth: int | None = None


class WorkerHeartbeatInfo(BaseModel):
    worker_id: str
    hostname: str
    concurrency: int
    active_tasks: int
    started_at: str
    last_heartbeat: str
    status: str


class WorkersSummary(BaseModel):
    active_count: int
    list: list[WorkerHeartbeatInfo]


class TenantStatsResponse(BaseModel):
    timestamp: float
    tenant_id: str
    scope: str = "tenant"
    tasks: TenantStatsTasksSummary
    performance: TenantStatsPerformanceSummary
    workers: WorkersSummary | None = None


class OperatorSystemPerformanceSummary(BaseModel):
    avg_execution_ms: float
    queue_depth: int
    queue_depth_by_priority: dict[str, int]


class OperatorSystemStatsResponse(BaseModel):
    timestamp: float
    scope: str = "system"
    tasks: TenantStatsTasksSummary
    performance: OperatorSystemPerformanceSummary
    workers: WorkersSummary


class LatencyPercentileSummary(BaseModel):
    sample_count: int
    sufficient_sample_size: bool
    minimum_recommended_samples: int = 20
    method: str = "exact_sql_observations"
    population: str
    p50_ms: float | None = None
    p95_ms: float | None = None
    p99_ms: float | None = None


class MonitoringHealthSummary(BaseModel):
    overall: str
    database: str
    redis: str


class MonitoringThroughputSummary(BaseModel):
    observation_count: int
    tasks_per_second: float
    tasks_per_minute: float


class MonitoringLatenciesSummary(BaseModel):
    outbox_dwell: LatencyPercentileSummary
    queue_dwell: LatencyPercentileSummary
    submission_to_start: LatencyPercentileSummary
    handler_duration: LatencyPercentileSummary
    worker_lease_tenure: LatencyPercentileSummary
    end_to_end: LatencyPercentileSummary


class MonitoringClusterWorkersSummary(BaseModel):
    active_count: int
    total_concurrency: int
    active_tasks: int


class MonitoringClusterDbPoolSummary(BaseModel):
    checked_out: int
    idle: int
    overflow: int


class MonitoringClusterSummary(BaseModel):
    queue_depth_by_priority: dict[str, int]
    outbox_pending_events: int
    outbox_lag_seconds: float
    workers: MonitoringClusterWorkersSummary
    db_pool: MonitoringClusterDbPoolSummary


class MonitoringSummaryResponse(BaseModel):
    timestamp_utc: str
    window_minutes: int
    window_seconds: int
    tenant_id: str
    health: MonitoringHealthSummary
    throughput: MonitoringThroughputSummary
    latencies_ms: MonitoringLatenciesSummary
    cluster: MonitoringClusterSummary | None = None


class TaskTypeCatalogItem(BaseModel):
    task_type: str
    display_name: str
    category: str
    description: str
    available: bool
    default_payload: dict[str, Any]
    payload_schema: dict[str, Any]


class TaskTypesCatalogResponse(BaseModel):
    items: list[TaskTypeCatalogItem]
    submission_constraints: dict[str, Any]
