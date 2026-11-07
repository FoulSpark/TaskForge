import threading
import time
from typing import Optional
from sqlalchemy import select
from sqlalchemy.orm import Session

from taskforge.core.logging import get_logger
from taskforge.database.models import OutboxEvent, get_utc_now
from taskforge.database.session import SessionLocal
from taskforge.queue.broker import MessageBroker, get_broker

logger = get_logger(__name__)


def publish_pending_outbox_events(db: Session, broker: MessageBroker, limit: int = 50) -> int:
    """
    Fetch pending outbox events and publish them to the message broker.
    Ensures at-least-once delivery.
    """
    stmt = (
        select(OutboxEvent)
        .where(OutboxEvent.status == "PENDING")
        .order_by(OutboxEvent.created_at.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    events = list(db.scalars(stmt).all())
    if not events:
        return 0

    published_count = 0
    now = get_utc_now()

    for event in events:
        try:
            task_id = event.payload.get("task_id", event.aggregate_id)
            priority = event.payload.get("priority", 2)

            # Enqueue to message broker
            broker.enqueue(task_id=task_id, priority=priority, payload=event.payload)

            event.status = "PUBLISHED"
            event.published_at = now
            published_count += 1
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

    db.commit()
    return published_count


class OutboxPublisherWorker:
    """Background daemon thread that continuously flushes the outbox table."""

    def __init__(self, poll_interval: float = 0.5, broker: Optional[MessageBroker] = None):
        self.poll_interval = poll_interval
        self.broker = broker or get_broker()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

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
        while not self._stop_event.is_set():
            try:
                with SessionLocal() as db:
                    publish_pending_outbox_events(db, self.broker)
            except Exception as exc:
                logger.error("outbox_publisher_cycle_error", error=str(exc))
            self._stop_event.wait(self.poll_interval)

