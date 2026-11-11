"""Operator administrative endpoints for Milestone G9.1.3 & Phase 10.

Guarded strictly by scope `admin:operator`. Provides endpoints for:
- System-wide task and worker statistics across all tenants
- Triggering expired lease recovery sweeps
- Outbox publisher sweeps
- Emergency queue purging with audit logging
"""

import time

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from taskforge.api.dependencies import require_scope
from taskforge.api.schemas import (
    OperatorSystemPerformanceSummary,
    OperatorSystemStatsResponse,
    WorkersSummary,
)
from taskforge.core.auth import AuthenticatedPrincipal
from taskforge.database.models import TaskEvent, get_utc_now
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import get_db
from taskforge.monitoring.summary import (
    build_workers_summary,
    collect_task_status_counts,
    get_broker_queue_depths,
)
from taskforge.queue.broker import get_broker
from taskforge.queue.outbox import publish_pending_outbox_events

router = APIRouter(prefix="/operator", tags=["Operator"])


@router.get(
    "/system-stats",
    response_model=OperatorSystemStatsResponse,
    summary="Cluster-wide system statistics",
    description="Returns global task status counts across all tenants, priority queue depths, and active workers.",
)
def get_system_stats(
    db: Session = Depends(get_db),
    _principal: AuthenticatedPrincipal = Depends(require_scope("admin:operator")),
):
    tasks_summary, avg_dur = collect_task_status_counts(db, tenant_id=None)
    try:
        broker = get_broker()
    except Exception:
        broker = None
    total_depth, by_priority = get_broker_queue_depths(broker)
    active_workers = build_workers_summary()

    return OperatorSystemStatsResponse(
        timestamp=time.time(),
        scope="system",
        tasks=tasks_summary,
        performance=OperatorSystemPerformanceSummary(
            avg_execution_ms=avg_dur,
            queue_depth=total_depth,
            queue_depth_by_priority=by_priority,
        ),
        workers=WorkersSummary(
            active_count=len(active_workers),
            list=active_workers,
        ),
    )


class RecoveryResponse(BaseModel):
    recovered_count: int
    reclaimed_broker_count: int


class OutboxSweepResponse(BaseModel):
    published_count: int


class PurgeQueueRequest(BaseModel):
    queue_name: str
    confirm_purge: bool


class PurgeQueueResponse(BaseModel):
    queue_name: str
    purged: bool
    purged_items: int


@router.post(
    "/recover",
    response_model=RecoveryResponse,
    summary="Trigger expired lease recovery",
    description="Recovers stranded tasks with expired leases to RETRY_WAIT and reclaims broker in-flight items.",
)
def trigger_recovery(
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(require_scope("admin:operator")),
):
    repo = TaskRepository(db)
    recovered = repo.recover_expired_leases(initiator="operator")
    broker = get_broker()
    reclaimed = broker.reclaim_expired() if broker else 0

    return RecoveryResponse(
        recovered_count=len(recovered),
        reclaimed_broker_count=reclaimed,
    )


@router.post(
    "/outbox-sweep",
    response_model=OutboxSweepResponse,
    summary="Trigger transactional outbox sweep",
    description="Sweeps pending outbox events and publishes eligible messages to Redis.",
)
def trigger_outbox_sweep(
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(require_scope("admin:operator")),
):
    broker = get_broker()
    if not broker:
        return OutboxSweepResponse(published_count=0)
    published = publish_pending_outbox_events(db, broker, limit=100)
    return OutboxSweepResponse(published_count=published)


@router.post(
    "/purge-queue",
    response_model=PurgeQueueResponse,
    status_code=status.HTTP_200_OK,
    summary="Emergency queue purge",
    description="Purges an active broker queue with mandatory audit logging in task_events.",
)
def purge_queue(
    payload: PurgeQueueRequest,
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(require_scope("admin:operator")),
):
    if not payload.confirm_purge:
        return PurgeQueueResponse(queue_name=payload.queue_name, purged=False, purged_items=0)

    broker = get_broker()
    purged_count = 0
    if broker and broker.redis:
        # Drain items
        purged_count = broker.get_queue_depth(payload.queue_name)
        broker.redis.delete(payload.queue_name)
        broker.redis.delete(f"{payload.queue_name}:in_flight")

        # Record audit event
        audit_event = TaskEvent(
            task_id="00000000-0000-0000-0000-000000000000",
            event_type="OPERATOR_QUEUE_PURGED",
            from_state=None,
            to_state="PURGED",
            metadata_json={
                "queue_name": payload.queue_name,
                "purged_count": purged_count,
                "operator_key": principal.key_id,
            },
            tenant_id=principal.tenant_id,
            actor_id=principal.key_id,
            created_at=get_utc_now(),
        )
        db.add(audit_event)
        db.commit()

    return PurgeQueueResponse(
        queue_name=payload.queue_name,
        purged=True,
        purged_items=purged_count,
    )
