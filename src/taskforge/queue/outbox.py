import threading
import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from taskforge.core.logging import get_logger
from taskforge.database.models import OutboxEvent, get_utc_now
from taskforge.database.session import SessionLocal
from taskforge.monitoring.profiler import record_diagnostic_event
from taskforge.queue.broker import MessageBroker, get_broker

logger = get_logger(__name__)


def publish_pending_outbox_events(
    db: Session,
    broker: MessageBroker,
    limit: int = 50,
    event_id: str | None = None,
    raise_on_connection_error: bool = False,
) -> int:
    """
    Fetch pending outbox events and publish them to the message broker.
    Ensures at-least-once delivery and respects available_at/scheduled_at execution delays.
    Starvation-free: Future-scheduled events are filtered at the database level and do not consume the batch.
    """
    batch_id = f"batch-{uuid.uuid4().hex[:8]}"
    record_diagnostic_event("outbox_claim_start", batch_id=batch_id)

    now = get_utc_now()
    stmt = (
        select(OutboxEvent)
        .where(OutboxEvent.status == "PENDING")
    )
    if event_id:
        stmt = stmt.where(OutboxEvent.id == event_id)
    else:
        stmt = stmt.where(OutboxEvent.available_at <= now)

    stmt = (
        stmt.order_by(OutboxEvent.available_at.asc(), OutboxEvent.created_at.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    events = list(db.scalars(stmt).all())
    record_diagnostic_event("outbox_claim_commit", batch_id=batch_id, metadata={"claimed_count": len(events)})
    if not events:
        return 0

    published_count = 0
    published_task_ids: list[str] = []

    for event in events:
        try:
            # Respect scheduled execution time: if scheduled_at or available_at is in the future, keep PENDING
            scheduled_at_str = event.payload.get("scheduled_at")
            if scheduled_at_str:
                sched_dt = datetime.fromisoformat(scheduled_at_str)
                if sched_dt.tzinfo is None:
                    sched_dt = sched_dt.replace(tzinfo=UTC)
                if sched_dt > now:
                    continue
            elif event.available_at is not None:
                avail = event.available_at
                if avail.tzinfo is None:
                    avail = avail.replace(tzinfo=UTC)
                if avail > now:
                    continue

            task_id = event.payload.get("task_id", event.aggregate_id)
            priority = event.payload.get("priority", 2)

            # Enqueue to message broker
            record_diagnostic_event("redis_enqueue_start", task_id=task_id, batch_id=batch_id)
            broker.enqueue(task_id=task_id, priority=priority, payload=event.payload)
            record_diagnostic_event("redis_enqueue_complete", task_id=task_id, batch_id=batch_id)

            # If retry event, update task status from RETRY_WAIT to QUEUED
            if event.event_type in ("TASK_RETRY", "task.retry"):
                from taskforge.core.state_machine import TaskStatus
                from taskforge.database.models import Task, TaskEvent

                stmt_t = select(Task).where(Task.id == task_id).with_for_update()
                t = db.scalars(stmt_t).first()
                if t and t.status == TaskStatus.RETRY_WAIT.value:
                    t.status = TaskStatus.QUEUED.value
                    t.scheduled_at = None
                    t.updated_at = now
                    db.add(
                        TaskEvent(
                            task_id=t.id,
                            event_type="STATUS_CHANGED",
                            from_state=TaskStatus.RETRY_WAIT.value,
                            to_state=TaskStatus.QUEUED.value,
                            metadata_json={"reason": "outbox_retry_published"},
                            created_at=now,
                        )
                    )

            event.status = "PUBLISHED"
            event.published_at = now
            published_count += 1
            published_task_ids.append(task_id)
            logger.debug("outbox_event_published", event_id=event.id, task_id=task_id)
        except Exception as exc:
            event.retry_count += 1
            event.error_message = str(exc)
            logger.warning(
                "outbox_event_publish_failed",
                event_id=event.id,
                error=str(exc),
                retry_count=event.retry_count,
            )
            is_conn_err = "Connection" in type(exc).__name__ or "Timeout" in type(exc).__name__
            if is_conn_err:
                db.commit()
                if raise_on_connection_error:
                    raise
                break

    db.commit()
    for tid in published_task_ids:
        record_diagnostic_event("outbox_published_commit", task_id=tid, batch_id=batch_id)
    return published_count


class OutboxPublisherWorker:
    """Background daemon thread that continuously flushes the outbox table."""

    def __init__(self, poll_interval: float = 0.5, broker: MessageBroker | None = None):
        self.poll_interval = poll_interval
        self.broker = broker or get_broker()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="OutboxPublisher", daemon=True)
        self._thread.start()
        logger.info("outbox_publisher_started", interval=self.poll_interval)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2.0)
            logger.info("outbox_publisher_stopped")

    def _run(self) -> None:
        consecutive_errors = 0
        while not self._stop_event.is_set():
            try:
                with SessionLocal() as db:
                    publish_pending_outbox_events(
                        db, self.broker, raise_on_connection_error=True
                    )
                consecutive_errors = 0
                self._stop_event.wait(self.poll_interval)
            except Exception as exc:
                consecutive_errors += 1
                # Bounded exponential backoff: base poll_interval, doubling up to 5.0s max (no tight spin)
                backoff = min(5.0, self.poll_interval * (2 ** (consecutive_errors - 1)))
                logger.warning(
                    "outbox_publisher_cycle_failed",
                    error=str(exc),
                    consecutive_errors=consecutive_errors,
                    backoff_seconds=backoff,
                )
                self._stop_event.wait(backoff)
