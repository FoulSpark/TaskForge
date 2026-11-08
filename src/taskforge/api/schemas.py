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
    attempts: list[TaskAttemptResponse] = Field(default_factory=list)
    result: TaskResultResponse | None = None
    events: list[TaskEventResponse] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


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
