import time

from fastapi import APIRouter, Depends, Query, Request, Response
from sqlalchemy.orm import Session

from taskforge.api.dependencies import require_scope
from taskforge.api.schemas import (
    MonitoringSummaryResponse,
    TenantStatsPerformanceSummary,
    TenantStatsResponse,
    WorkersSummary,
)
from taskforge.core.auth import AuthenticatedPrincipal
from taskforge.database.session import get_db
from taskforge.monitoring.metrics import (
    CONTENT_TYPE_LATEST,
    get_metrics_content,
)
from taskforge.monitoring.summary import (
    build_monitoring_summary,
    build_workers_summary,
    collect_task_status_counts,
)
from taskforge.queue.broker import get_broker

router = APIRouter(tags=["Metrics & Observability"])


@router.get("/metrics", summary="Prometheus scrape endpoint")
def prometheus_metrics():
    return Response(content=get_metrics_content(), media_type=CONTENT_TYPE_LATEST)


@router.get(
    "/api/v1/stats",
    response_model=TenantStatsResponse,
    summary="Tenant-scoped task statistics",
    description="Returns status counts and execution duration scoped strictly to the authenticated tenant. Operator tokens additionally receive cluster queue depth and active workers.",
)
def system_stats(
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(require_scope("tasks:read")),
):
    tasks_summary, avg_dur = collect_task_status_counts(db, tenant_id=principal.tenant_id)
    is_operator = "admin:operator" in principal.scopes

    queue_depth: int | None = None
    workers_summary: WorkersSummary | None = None
    if is_operator:
        try:
            broker = get_broker()
            queue_depth = broker.get_queue_depth()
        except Exception:
            queue_depth = 0
        active_workers = build_workers_summary()
        workers_summary = WorkersSummary(
            active_count=len(active_workers),
            list=active_workers,
        )

    return TenantStatsResponse(
        timestamp=time.time(),
        tenant_id=principal.tenant_id,
        scope="tenant",
        tasks=tasks_summary,
        performance=TenantStatsPerformanceSummary(
            avg_execution_ms=avg_dur,
            queue_depth=queue_depth,
        ),
        workers=workers_summary,
    )


@router.get(
    "/api/v1/monitoring/summary",
    response_model=MonitoringSummaryResponse,
    summary="Tenant-scoped JSON monitoring summary",
    description="Returns rolling-window throughput, non-conflated lifecycle latency percentiles, service health, and operator-gated cluster state.",
)
def monitoring_summary(
    request: Request,
    window_minutes: int = Query(5, ge=1, le=60, description="Observation window in minutes"),
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(require_scope("tasks:read")),
):
    is_operator = "admin:operator" in principal.scopes
    broker = None
    if is_operator:
        try:
            broker = get_broker()
        except Exception:
            broker = None
    redis_client = getattr(getattr(request.app, "state", None), "redis", None)

    return build_monitoring_summary(
        db=db,
        tenant_id=principal.tenant_id,
        is_operator=is_operator,
        window_minutes=window_minutes,
        broker=broker,
        redis_client=redis_client,
    )
