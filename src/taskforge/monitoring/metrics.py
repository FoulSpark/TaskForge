from typing import Any

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

from taskforge.core.logging import get_logger

logger = get_logger(__name__)

__all__ = [
    "CONTENT_TYPE_LATEST",
    "get_metrics_content",
    "TASKS_SUBMITTED",
    "TASK_QUEUE_WAIT",
    "TASK_EXECUTION_DURATION",
    "TASKS_COMPLETED",
    "RETRIES_TOTAL",
    "RETRY_TOTAL",
    "DEAD_LETTER_TOTAL",
    "QUEUE_DEPTH",
    "OUTBOX_PENDING_EVENTS",
    "OUTBOX_LAG_SECONDS",
    "LEASE_RECOVERIES_TOTAL",
    "API_REQUEST_DURATION",
    "API_REQUESTS_TOTAL",
    "RATE_LIMIT_EXCEEDED_TOTAL",
    "ACTIVE_WORKERS",
    "DB_POOL_CONNECTIONS",
    "TELEMETRY_SCRAPE_ERRORS_TOTAL",
    "normalize_priority",
    "normalize_task_type",
    "normalize_status",
    "normalize_failure_reason",
    "normalize_initiator",
    "normalize_recovery_result",
    "normalize_http_method",
    "normalize_route",
    "normalize_status_code_class",
    "normalize_pool_state",
    "record_task_submitted",
    "record_queue_wait",
    "record_task_execution_duration",
    "record_task_completed",
    "record_task_retry",
    "record_task_dead_letter",
    "record_lease_recovery",
    "record_api_request",
]

# ---------------------------------------------------------------------------
# Label Whitelists & Finite Bounded Normalizers
# ---------------------------------------------------------------------------

PRIORITY_WHITELIST = {"critical", "high", "medium", "low"}
TASK_TYPE_WHITELIST = {
    "image_resize",
    "payment_reconcile",
    "report_generate",
    "webhook_send",
    "csv_export",
}
STATUS_WHITELIST = {"SUCCESS", "FAILED", "CANCELLED", "DEAD_LETTER"}
FAILURE_REASON_WHITELIST = {"timeout", "exception", "circuit_breaker"}
INITIATOR_WHITELIST = {"supervisor", "operator"}
RECOVERY_RESULT_WHITELIST = {"requeued", "abandoned"}
HTTP_METHOD_WHITELIST = {"GET", "POST"}
ROUTE_WHITELIST = {
    "/api/v1/tasks",
    "/api/v1/tasks/{id}",
    "/api/v1/operator/recover",
    "/api/v1/operator/outbox-sweep",
}
STATUS_CODE_CLASS_WHITELIST = {"2xx", "4xx", "5xx"}
POOL_STATE_WHITELIST = {"checked_out", "idle", "overflow"}


def normalize_priority(val: Any) -> str:
    """Normalize TaskPriority enum, integer (1-4), or string to finite priority whitelist."""
    if hasattr(val, "name"):
        name = str(val.name).lower()
        if name in PRIORITY_WHITELIST:
            return name
    if hasattr(val, "value"):
        val = val.value

    if isinstance(val, int):
        mapping = {4: "critical", 3: "high", 2: "medium", 1: "low"}
        return mapping.get(val, "low")

    s = str(val).strip().lower()
    if s in PRIORITY_WHITELIST:
        return s
    mapping = {"4": "critical", "3": "high", "2": "medium", "1": "low"}
    return mapping.get(s, "low")


def normalize_task_type(val: Any) -> str:
    """Normalize task_type to approved 5-category whitelist or 'other' fallback."""
    s = str(val).strip() if val is not None else ""
    return s if s in TASK_TYPE_WHITELIST else "other"


def normalize_status(val: Any) -> str:
    """Normalize terminal task status to 4-category whitelist."""
    if hasattr(val, "value"):
        val = val.value
    s = str(val).strip().upper() if val is not None else ""
    return s if s in STATUS_WHITELIST else "FAILED"


def normalize_failure_reason(val: Any) -> str:
    """Normalize failure reason to approved 3-category whitelist or 'other' fallback."""
    s = str(val).strip().lower() if val is not None else ""
    return s if s in FAILURE_REASON_WHITELIST else "other"


def normalize_initiator(val: Any) -> str:
    """Normalize lease recovery initiator to 'supervisor' or 'operator'."""
    s = str(val).strip().lower() if val is not None else ""
    return s if s in INITIATOR_WHITELIST else "supervisor"


def normalize_recovery_result(val: Any) -> str:
    """Normalize lease recovery result to 'requeued' or 'abandoned'."""
    s = str(val).strip().lower() if val is not None else ""
    return s if s in RECOVERY_RESULT_WHITELIST else "requeued"


def normalize_http_method(val: Any) -> str:
    """Normalize HTTP method to 'GET', 'POST', or 'other'."""
    s = str(val).strip().upper() if val is not None else ""
    return s if s in HTTP_METHOD_WHITELIST else "other"


def normalize_route(val: Any) -> str:
    """Normalize route template to whitelist or 'other'."""
    s = str(val).strip() if val is not None else ""
    if s in ("/api/v1/tasks/{task_id}", "/api/v1/tasks/{id}", "/tasks/{task_id}", "/tasks/{id}"):
        return "/api/v1/tasks/{id}"
    if s in ("/api/v1/tasks", "/tasks"):
        return "/api/v1/tasks"
    if s in ("/api/v1/operator/recover", "/operator/recover"):
        return "/api/v1/operator/recover"
    if s in ("/api/v1/operator/outbox-sweep", "/operator/outbox-sweep"):
        return "/api/v1/operator/outbox-sweep"
    return s if s in ROUTE_WHITELIST else "other"


def normalize_status_code_class(val: Any) -> str:
    """Normalize HTTP status code to '2xx', '4xx', '5xx', or 'other'."""
    if isinstance(val, int):
        code_class = f"{val // 100}xx"
        return code_class if code_class in STATUS_CODE_CLASS_WHITELIST else "other"
    s = str(val).strip().lower() if val is not None else ""
    return s if s in STATUS_CODE_CLASS_WHITELIST else "other"


def normalize_pool_state(val: Any) -> str:
    """Normalize DB pool state to 'checked_out', 'idle', or 'overflow'."""
    s = str(val).strip().lower() if val is not None else ""
    return s if s in POOL_STATE_WHITELIST else "idle"


# ---------------------------------------------------------------------------
# Metric Registrations (Contract Bounded Cardinality: 662 Base Series)
# ---------------------------------------------------------------------------

# 1. Task submission counter (24 combinations)
TASKS_SUBMITTED = Counter(
    "taskforge_tasks_submitted_total",
    "Total count of tasks submitted to the platform",
    ["task_type", "priority"],
)

# 2. Queue wait histogram (24 combinations * 12 buckets = 288 series)
TASK_QUEUE_WAIT = Histogram(
    "taskforge_task_queue_wait_seconds",
    "Time tasks wait in queue before being picked up by workers",
    ["task_type", "priority"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 15.0],
)

# 3. Task execution duration histogram (6 combinations * 11 buckets = 66 series)
TASK_EXECUTION_DURATION = Histogram(
    "taskforge_task_execution_duration_seconds",
    "Duration of task execution by workers in seconds",
    ["task_type"],
    buckets=[0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 15.0],
)

# 4. Task completion counter (24 combinations)
TASKS_COMPLETED = Counter(
    "taskforge_tasks_completed_total",
    "Total count of tasks completed by execution workers",
    ["task_type", "status"],
)

# 5. Retry counter (24 combinations)
RETRIES_TOTAL = Counter(
    "taskforge_retries_total",
    "Total number of task retry attempts triggered",
    ["task_type", "failure_reason"],
)
RETRY_TOTAL = RETRIES_TOTAL  # Backward-compatibility alias

# 6. Dead letter counter (6 combinations)
DEAD_LETTER_TOTAL = Counter(
    "taskforge_dead_letter_total",
    "Total number of tasks moved to the dead-letter queue",
    ["task_type"],
)

# 7. Queue depth gauge (4 combinations)
QUEUE_DEPTH = Gauge(
    "taskforge_queue_depth",
    "Current number of pending tasks in the broker queue",
    ["priority"],
)

# 8. Outbox pending events gauge (1 series)
OUTBOX_PENDING_EVENTS = Gauge(
    "taskforge_outbox_pending_events",
    "Current count of pending events in PostgreSQL outbox table",
)

# 9. Outbox lag seconds gauge (1 series)
OUTBOX_LAG_SECONDS = Gauge(
    "taskforge_outbox_lag_seconds",
    "Age of oldest pending event in PostgreSQL outbox table in seconds",
)

# 10. Lease recoveries counter (4 combinations)
LEASE_RECOVERIES_TOTAL = Counter(
    "taskforge_lease_recoveries_total",
    "Total number of expired worker leases recovered",
    ["initiator", "recovery_result"],
)

# 11. API request duration histogram (15 combinations * 10 buckets = 150 series)
API_REQUEST_DURATION = Histogram(
    "taskforge_api_request_duration_seconds",
    "HTTP request latency in seconds",
    ["http_method", "route"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1.0],
)

# 12. API requests total counter (60 combinations)
API_REQUESTS_TOTAL = Counter(
    "taskforge_api_requests_total",
    "Total count of HTTP requests processed by API",
    ["http_method", "route", "status_code_class"],
)

# 13. Rate limit exceeded counter (5 combinations)
RATE_LIMIT_EXCEEDED_TOTAL = Counter(
    "taskforge_rate_limit_exceeded_total",
    "Total count of HTTP requests rejected due to rate limit",
    ["route"],
)

# 14. Active workers gauge (1 series)
ACTIVE_WORKERS = Gauge(
    "taskforge_active_workers",
    "Number of currently active workers emitting heartbeats",
)

# 15. DB pool connections gauge (3 combinations)
DB_POOL_CONNECTIONS = Gauge(
    "taskforge_db_pool_connections",
    "Database connection pool allocation",
    ["pool_state"],
)

# 16. Telemetry scrape errors counter (1 series)
TELEMETRY_SCRAPE_ERRORS_TOTAL = Counter(
    "taskforge_telemetry_scrape_errors_total",
    "Total number of telemetry collection failures during scrape",
)


# ---------------------------------------------------------------------------
# Telemetry Failure Isolation Helper Functions
# ---------------------------------------------------------------------------


def record_task_submitted(task_type: Any, priority: Any) -> None:
    """Record task submission metric strictly after database commit."""
    try:
        norm_type = normalize_task_type(task_type)
        norm_prio = normalize_priority(priority)
        TASKS_SUBMITTED.labels(task_type=norm_type, priority=norm_prio).inc()
    except Exception as exc:
        logger.debug("Telemetry isolation: failed to record task submission: %s", exc)


def record_queue_wait(task_type: Any, priority: Any, wait_seconds: float) -> None:
    """Record queue wait histogram strictly after attempt row is committed under row lock."""
    try:
        norm_type = normalize_task_type(task_type)
        norm_prio = normalize_priority(priority)
        val = max(0.0, float(wait_seconds))
        TASK_QUEUE_WAIT.labels(task_type=norm_type, priority=norm_prio).observe(val)
    except Exception as exc:
        logger.debug("Telemetry isolation: failed to record queue wait: %s", exc)


def record_task_execution_duration(task_type: Any, duration_seconds: float) -> None:
    """Record handler-only execution duration histogram."""
    try:
        norm_type = normalize_task_type(task_type)
        val = max(0.0, float(duration_seconds))
        TASK_EXECUTION_DURATION.labels(task_type=norm_type).observe(val)
    except Exception as exc:
        logger.debug("Telemetry isolation: failed to record execution duration: %s", exc)


def record_task_completed(task_type: Any, status: Any) -> None:
    """Record task completion counter strictly after terminal transition commits."""
    try:
        norm_type = normalize_task_type(task_type)
        norm_status = normalize_status(status)
        TASKS_COMPLETED.labels(task_type=norm_type, status=norm_status).inc()
    except Exception as exc:
        logger.debug("Telemetry isolation: failed to record task completion: %s", exc)


def record_task_retry(task_type: Any, failure_reason: Any = "exception") -> None:
    """Record task retry counter strictly after RETRY_WAIT transition commits."""
    try:
        norm_type = normalize_task_type(task_type)
        norm_reason = normalize_failure_reason(failure_reason)
        RETRIES_TOTAL.labels(task_type=norm_type, failure_reason=norm_reason).inc()
    except Exception as exc:
        logger.debug("Telemetry isolation: failed to record task retry: %s", exc)


def record_task_dead_letter(task_type: Any) -> None:
    """Record task dead letter counter strictly after DEAD_LETTER transition commits."""
    try:
        norm_type = normalize_task_type(task_type)
        DEAD_LETTER_TOTAL.labels(task_type=norm_type).inc()
    except Exception as exc:
        logger.debug("Telemetry isolation: failed to record task dead letter: %s", exc)


def record_lease_recovery(initiator: Any, recovery_result: Any) -> None:
    """Record lease recovery counter strictly after recovery transaction commits."""
    try:
        norm_init = normalize_initiator(initiator)
        norm_res = normalize_recovery_result(recovery_result)
        LEASE_RECOVERIES_TOTAL.labels(initiator=norm_init, recovery_result=norm_res).inc()
    except Exception as exc:
        logger.debug("Telemetry isolation: failed to record lease recovery: %s", exc)


def record_api_request(
    http_method: Any,
    route: Any,
    status_code: Any,
    duration_seconds: float,
) -> None:
    """Record HTTP request lifecycle metrics strictly isolated from request failure."""
    try:
        norm_method = normalize_http_method(http_method)
        norm_route = normalize_route(route)
        norm_status_class = normalize_status_code_class(status_code)
        val = max(0.0, float(duration_seconds))

        API_REQUEST_DURATION.labels(http_method=norm_method, route=norm_route).observe(val)
        API_REQUESTS_TOTAL.labels(
            http_method=norm_method,
            route=norm_route,
            status_code_class=norm_status_class,
        ).inc()

        if status_code == 429:
            RATE_LIMIT_EXCEEDED_TOTAL.labels(route=norm_route).inc()
    except Exception as exc:
        logger.debug("Telemetry isolation: failed to record API request metrics: %s", exc)


def get_metrics_content() -> bytes:
    """Generate Prometheus scrape format output with single-flight cached gauge updates."""
    from taskforge.monitoring.collector import get_gauge_snapshot_cache

    cache = get_gauge_snapshot_cache()
    has_valid_snapshot = cache.collect_and_update()

    raw_output = generate_latest()

    if not has_valid_snapshot:
        # Cold-start failure: completely omit unavailable backend infrastructure gauges
        # from exposition (never fabricate zero values for uncollected state)
        omitted_metrics = {
            "taskforge_queue_depth",
            "taskforge_outbox_pending_events",
            "taskforge_outbox_lag_seconds",
            "taskforge_active_workers",
        }
        lines = raw_output.decode("utf-8").splitlines()
        filtered_lines = [
            line for line in lines
            if not any(
                line.startswith(m) or line.startswith(f"# HELP {m}") or line.startswith(f"# TYPE {m}")
                for m in omitted_metrics
            )
        ]
        return ("\n".join(filtered_lines) + "\n").encode("utf-8")

    return raw_output
