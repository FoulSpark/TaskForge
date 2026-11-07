from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

# Task metrics
TASKS_SUBMITTED = Counter(
    "taskforge_tasks_submitted_total",
    "Total count of tasks submitted to the platform",
    ["task_type", "priority"],
)

TASKS_COMPLETED = Counter(
    "taskforge_tasks_completed_total",
    "Total count of tasks completed by execution workers",
    ["task_type", "status"],
)

TASK_EXECUTION_DURATION = Histogram(
    "taskforge_task_execution_duration_seconds",
    "Duration of task execution by workers in seconds",
    ["task_type"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0],
)

TASK_QUEUE_WAIT = Histogram(
    "taskforge_task_queue_wait_seconds",
    "Time tasks wait in queue before being picked up by workers",
    ["task_type"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
)

QUEUE_DEPTH = Gauge(
    "taskforge_queue_depth",
    "Current number of pending tasks in the broker queue",
    ["priority"],
)

ACTIVE_WORKERS = Gauge(
    "taskforge_active_workers",
    "Number of currently active workers emitting heartbeats",
)

RETRY_TOTAL = Counter(
    "taskforge_retry_total",
    "Total number of task retry attempts triggered",
    ["task_type"],
)

DEAD_LETTER_TOTAL = Counter(
    "taskforge_dead_letter_total",
    "Total number of tasks moved to the dead-letter queue",
    ["task_type"],
)

# API metrics
API_REQUESTS_TOTAL = Counter(
    "taskforge_api_requests_total",
    "Total count of HTTP requests processed by API",
    ["method", "endpoint", "status_code"],
)

API_REQUEST_DURATION = Histogram(
    "taskforge_api_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "endpoint", "status_code"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

# Database pool metrics
DB_POOL_SIZE = Gauge(
    "taskforge_db_pool_size",
    "Total size of database connection pool",
)

DB_POOL_CHECKEDOUT = Gauge(
    "taskforge_db_pool_checkedout",
    "Number of checked-out database connections",
)


def get_metrics_content() -> bytes:
    """Generate Prometheus scrape format output."""
    return generate_latest()

