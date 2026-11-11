"""Shared tenant-scoped stats and Phase 10 monitoring summary builders."""

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from taskforge.api.schemas import (
    LatencyPercentileSummary,
    MonitoringClusterDbPoolSummary,
    MonitoringClusterSummary,
    MonitoringClusterWorkersSummary,
    MonitoringHealthSummary,
    MonitoringLatenciesSummary,
    MonitoringSummaryResponse,
    MonitoringThroughputSummary,
    TenantStatsTasksSummary,
    WorkerHeartbeatInfo,
)
from taskforge.core.state_machine import TERMINAL_STATES, TaskStatus
from taskforge.database.models import OutboxEvent, Task, TaskAttempt
from taskforge.database.session import engine as app_engine
from taskforge.workers.heartbeat import get_active_workers

MINIMUM_RECOMMENDED_SAMPLES = 20
TERMINAL_STATUS_VALUES = tuple(s.value for s in TERMINAL_STATES)


def _to_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _diff_ms(end_dt: datetime | None, start_dt: datetime | None) -> float | None:
    end_u = _to_utc(end_dt)
    start_u = _to_utc(start_dt)
    if end_u is None or start_u is None:
        return None
    return max(0.0, round((end_u - start_u).total_seconds() * 1000.0, 3))


def compute_percentile(sorted_vals: list[float], percentile: float) -> float | None:
    """Compute continuous linear-interpolated percentile (matching SQL PERCENTILE_CONT)."""
    n = len(sorted_vals)
    if n == 0:
        return None
    if n == 1:
        return round(sorted_vals[0], 3)
    idx = (percentile / 100.0) * (n - 1)
    lo = int(idx)
    hi = min(lo + 1, n - 1)
    frac = idx - lo
    val = sorted_vals[lo] + frac * (sorted_vals[hi] - sorted_vals[lo])
    return round(val, 3)


def _summarize_samples(samples: list[float], population: str) -> LatencyPercentileSummary:
    ordered = sorted(samples)
    count = len(ordered)
    if count == 0:
        return LatencyPercentileSummary(
            sample_count=0,
            sufficient_sample_size=False,
            minimum_recommended_samples=MINIMUM_RECOMMENDED_SAMPLES,
            method="exact_sql_observations",
            population=population,
            p50_ms=None,
            p95_ms=None,
            p99_ms=None,
        )
    return LatencyPercentileSummary(
        sample_count=count,
        sufficient_sample_size=count >= MINIMUM_RECOMMENDED_SAMPLES,
        minimum_recommended_samples=MINIMUM_RECOMMENDED_SAMPLES,
        method="exact_sql_observations",
        population=population,
        p50_ms=compute_percentile(ordered, 50.0),
        p95_ms=compute_percentile(ordered, 95.0),
        p99_ms=compute_percentile(ordered, 99.0),
    )


def collect_task_status_counts(
    db: Session, tenant_id: str | None = None
) -> tuple[TenantStatsTasksSummary, float]:
    """Aggregate task status counts and average duration, optionally filtered by tenant_id."""
    stmt = select(Task.status, func.count(Task.id)).group_by(Task.status)
    if tenant_id is not None:
        stmt = stmt.where(Task.tenant_id == tenant_id)
    status_counts = dict(db.execute(stmt).all())

    summary = TenantStatsTasksSummary(
        total=sum(status_counts.values()),
        pending=status_counts.get(TaskStatus.PENDING.value, 0),
        queued=status_counts.get(TaskStatus.QUEUED.value, 0),
        running=status_counts.get(TaskStatus.RUNNING.value, 0),
        succeeded=status_counts.get(TaskStatus.SUCCESS.value, 0),
        failed=status_counts.get(TaskStatus.FAILED.value, 0),
        retry_wait=status_counts.get(TaskStatus.RETRY_WAIT.value, 0),
        dead_letter=status_counts.get(TaskStatus.DEAD_LETTER.value, 0),
        cancelled=status_counts.get(TaskStatus.CANCELLED.value, 0),
    )

    avg_stmt = select(func.avg(TaskAttempt.duration_ms)).where(TaskAttempt.status == "SUCCESS")
    if tenant_id is not None:
        avg_stmt = avg_stmt.join(Task, TaskAttempt.task_id == Task.id).where(
            Task.tenant_id == tenant_id
        )
    avg_dur = float(db.scalar(avg_stmt) or 0.0)
    return summary, round(avg_dur, 2)


def get_broker_queue_depths(broker: Any) -> tuple[int, dict[str, int]]:
    """Return total ready queue depth and priority breakdown safely for Redis or Memory broker."""
    by_priority = {"critical": 0, "high": 0, "medium": 0, "low": 0}
    if broker is None:
        return 0, by_priority

    redis_client = getattr(broker, "client", None)
    queue_name = getattr(broker, "queue_name", None)
    if redis_client is not None and queue_name:
        try:
            pipe = redis_client.pipeline()
            pipe.zcount(queue_name, 3000000000001, 3999999999999)
            pipe.zcount(queue_name, 2000000000001, 2999999999999)
            pipe.zcount(queue_name, 1000000000001, 1999999999999)
            pipe.zcount(queue_name, 1, 999999999999)
            res = pipe.execute()
            by_priority = {
                "critical": int(res[0]),
                "high": int(res[1]),
                "medium": int(res[2]),
                "low": int(res[3]),
            }
            return sum(by_priority.values()), by_priority
        except Exception:
            pass

    try:
        total = int(broker.get_queue_depth())
    except Exception:
        total = 0
    return total, by_priority


def build_workers_summary() -> list[WorkerHeartbeatInfo]:
    return [WorkerHeartbeatInfo(**w) for w in get_active_workers()]


def build_monitoring_summary(
    db: Session,
    tenant_id: str,
    is_operator: bool,
    window_minutes: int,
    broker: Any,
    redis_client: Any,
) -> MonitoringSummaryResponse:
    """Build tenant-scoped monitoring summary with strict R1 lifecycle definitions."""
    import taskforge.main as main_mod

    now = datetime.now(UTC)
    window_start = now - timedelta(minutes=window_minutes)
    window_seconds = window_minutes * 60

    # 1. Completed tasks in window (strictly tenant-scoped)
    completed_tasks = list(
        db.scalars(
            select(Task)
            .where(
                Task.tenant_id == tenant_id,
                Task.status.in_(TERMINAL_STATUS_VALUES),
                Task.completed_at.isnot(None),
                Task.completed_at >= window_start,
            )
            .options(selectinload(Task.attempts))
        ).all()
    )

    e2e_samples: list[float] = []
    lease_tenure_samples: list[float] = []
    for task in completed_tasks:
        e2e = _diff_ms(task.completed_at, task.created_at)
        if e2e is not None:
            e2e_samples.append(e2e)
        completed_atts = [a for a in task.attempts if a.completed_at is not None]
        if completed_atts:
            final_att = max(completed_atts, key=lambda a: a.attempt_number)
            tenure = _diff_ms(task.completed_at, final_att.started_at)
            if tenure is not None:
                lease_tenure_samples.append(tenure)

    obs_count = len(completed_tasks)
    tps = round(obs_count / window_seconds, 4) if window_seconds > 0 else 0.0
    tpm = round(obs_count / window_minutes, 4) if window_minutes > 0 else 0.0

    # 2. Completed attempts in window (never mixing active RUNNING attempts)
    att_rows = db.execute(
        select(TaskAttempt, Task.created_at)
        .join(Task, TaskAttempt.task_id == Task.id)
        .where(
            Task.tenant_id == tenant_id,
            TaskAttempt.completed_at.isnot(None),
            TaskAttempt.completed_at >= window_start,
        )
    ).all()

    sub_to_start_samples: list[float] = []
    handler_dur_samples: list[float] = []
    for att, task_created_at in att_rows:
        s2s = _diff_ms(att.started_at, task_created_at)
        if s2s is not None:
            sub_to_start_samples.append(s2s)
        if att.duration_ms is not None:
            handler_dur_samples.append(max(0.0, round(float(att.duration_ms), 3)))

    # 3. Published outbox dwell (t_published - t_ingested)
    outbox_rows = db.execute(
        select(OutboxEvent.published_at, Task.created_at)
        .join(Task, OutboxEvent.aggregate_id == Task.id)
        .where(
            Task.tenant_id == tenant_id,
            OutboxEvent.aggregate_type == "task",
            OutboxEvent.event_type == "TASK_SUBMITTED",
            OutboxEvent.status == "PUBLISHED",
            OutboxEvent.published_at.isnot(None),
            OutboxEvent.published_at >= window_start,
        )
    ).all()

    outbox_dwell_samples: list[float] = []
    for pub_at, task_created_at in outbox_rows:
        od = _diff_ms(pub_at, task_created_at)
        if od is not None:
            outbox_dwell_samples.append(od)

    # 4. Broker queue dwell (t_started - t_published) strictly excluding outbox delay
    qd_rows = db.execute(
        select(TaskAttempt.started_at, OutboxEvent.published_at)
        .join(Task, TaskAttempt.task_id == Task.id)
        .join(OutboxEvent, OutboxEvent.aggregate_id == Task.id)
        .where(
            Task.tenant_id == tenant_id,
            TaskAttempt.attempt_number == 1,
            TaskAttempt.completed_at.isnot(None),
            TaskAttempt.completed_at >= window_start,
            OutboxEvent.aggregate_type == "task",
            OutboxEvent.event_type == "TASK_SUBMITTED",
            OutboxEvent.status == "PUBLISHED",
            OutboxEvent.published_at.isnot(None),
        )
    ).all()

    queue_dwell_samples: list[float] = []
    for att_started_at, pub_at in qd_rows:
        qd = _diff_ms(att_started_at, pub_at)
        if qd is not None:
            queue_dwell_samples.append(qd)

    # 5. Coarse health enums only (no hostnames, URLs, ports, or DSNs)
    try:
        db_ok = bool(main_mod.check_database())
    except Exception:
        db_ok = False
    try:
        redis_ok = bool(main_mod.check_redis(redis_client))
    except Exception:
        redis_ok = False

    health = MonitoringHealthSummary(
        overall="healthy" if (db_ok and redis_ok) else "degraded",
        database="healthy" if db_ok else "unavailable",
        redis="healthy" if redis_ok else "unavailable",
    )

    # 6. Operator-only cluster block
    cluster_summary: MonitoringClusterSummary | None = None
    if is_operator:
        _, queue_by_prio = get_broker_queue_depths(broker)
        pending_count, min_created = db.execute(
            select(func.count(OutboxEvent.id), func.min(OutboxEvent.created_at)).where(
                OutboxEvent.status == "PENDING"
            )
        ).one()
        pending_int = int(pending_count or 0)
        lag_sec = 0.0
        if pending_int > 0 and min_created is not None:
            min_u = _to_utc(min_created)
            if min_u is not None:
                lag_sec = max(0.0, round((now - min_u).total_seconds(), 3))

        active_workers = get_active_workers()
        total_conc = sum(int(w.get("concurrency", 0)) for w in active_workers)
        active_tasks = sum(int(w.get("active_tasks", 0)) for w in active_workers)

        pool = getattr(app_engine, "pool", None)
        checked_out = pool.checkedout() if pool and hasattr(pool, "checkedout") else 0
        idle = pool.checkedin() if pool and hasattr(pool, "checkedin") else 0
        raw_overflow = pool.overflow() if pool and hasattr(pool, "overflow") else 0

        cluster_summary = MonitoringClusterSummary(
            queue_depth_by_priority=queue_by_prio,
            outbox_pending_events=pending_int,
            outbox_lag_seconds=lag_sec,
            workers=MonitoringClusterWorkersSummary(
                active_count=len(active_workers),
                total_concurrency=total_conc,
                active_tasks=active_tasks,
            ),
            db_pool=MonitoringClusterDbPoolSummary(
                checked_out=max(0, int(checked_out)),
                idle=max(0, int(idle)),
                overflow=max(0, int(raw_overflow)),
            ),
        )

    return MonitoringSummaryResponse(
        timestamp_utc=now.isoformat(),
        window_minutes=window_minutes,
        window_seconds=window_seconds,
        tenant_id=tenant_id,
        health=health,
        throughput=MonitoringThroughputSummary(
            observation_count=obs_count,
            tasks_per_second=tps,
            tasks_per_minute=tpm,
        ),
        latencies_ms=MonitoringLatenciesSummary(
            outbox_dwell=_summarize_samples(
                outbox_dwell_samples, "published_submitted_outbox_events"
            ),
            queue_dwell=_summarize_samples(
                queue_dwell_samples, "completed_initial_attempts_with_outbox"
            ),
            submission_to_start=_summarize_samples(
                sub_to_start_samples, "completed_attempts_only"
            ),
            handler_duration=_summarize_samples(
                handler_dur_samples, "completed_attempts_only"
            ),
            worker_lease_tenure=_summarize_samples(
                lease_tenure_samples, "completed_tasks_with_final_attempt"
            ),
            end_to_end=_summarize_samples(e2e_samples, "completed_tasks_only"),
        ),
        cluster=cluster_summary,
    )
