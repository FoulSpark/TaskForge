import time
from fastapi import APIRouter, Depends, Response
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from taskforge.core.state_machine import TaskStatus
from taskforge.database.models import Task, TaskAttempt
from taskforge.database.session import get_db
from taskforge.monitoring.metrics import (
    CONTENT_TYPE_LATEST,
    get_metrics_content,
)
from taskforge.queue.broker import get_broker
from taskforge.workers.heartbeat import get_active_workers

router = APIRouter(tags=["Metrics & Observability"])


@router.get("/metrics", summary="Prometheus scrape endpoint")
def prometheus_metrics():
    return Response(content=get_metrics_content(), media_type=CONTENT_TYPE_LATEST)


@router.get("/api/v1/stats", summary="Real-time system telemetry and summary statistics")
def system_stats(db: Session = Depends(get_db)):
    # Total counts by status
    stmt = select(Task.status, func.count(Task.id)).group_by(Task.status)
    status_counts = dict(db.execute(stmt).all())

    total = sum(status_counts.values())
    pending = status_counts.get(TaskStatus.PENDING.value, 0)
    queued = status_counts.get(TaskStatus.QUEUED.value, 0)
    running = status_counts.get(TaskStatus.RUNNING.value, 0)
    succeeded = status_counts.get(TaskStatus.SUCCESS.value, 0)
    failed = status_counts.get(TaskStatus.FAILED.value, 0)
    retry_wait = status_counts.get(TaskStatus.RETRY_WAIT.value, 0)
    dead_letter = status_counts.get(TaskStatus.DEAD_LETTER.value, 0)
    cancelled = status_counts.get(TaskStatus.CANCELLED.value, 0)

    # Calculate average attempt duration in ms
    avg_dur = (
        db.scalar(
            select(func.avg(TaskAttempt.duration_ms)).where(TaskAttempt.status == "SUCCESS")
        )
        or 0.0
    )

    # Broker queue depth
    broker = get_broker()
    queue_depth = broker.get_queue_depth()

    # Active workers
    active_workers = get_active_workers()

    return {
        "timestamp": time.time(),
        "tasks": {
            "total": total,
            "pending": pending,
            "queued": queued,
            "running": running,
            "succeeded": succeeded,
            "failed": failed,
            "retry_wait": retry_wait,
            "dead_letter": dead_letter,
            "cancelled": cancelled,
        },
        "performance": {
            "avg_execution_ms": round(avg_dur, 2),
            "queue_depth": queue_depth,
        },
        "workers": {
            "active_count": len(active_workers),
            "list": active_workers,
        },
    }

