from datetime import UTC, datetime

from sqlalchemy.orm import Session

from taskforge.api.errors import IdempotencyConflictError
from taskforge.api.schemas import TaskCreateRequest
from taskforge.core.logging import get_logger
from taskforge.core.state_machine import TaskStatus
from taskforge.database.models import Task
from taskforge.database.repositories.task_repository import TaskRepository

logger = get_logger(__name__)


def _normalize_dt(dt: datetime | None) -> datetime | None:
    """Normalizes timestamps to UTC for deterministic comparison.
    If the datetime is naive (no tzinfo), UTC is assumed per API contract.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


class TaskService:
    def __init__(self, db: Session):
        self.db = db
        self.repo = TaskRepository(db)

    def submit_task(self, req: TaskCreateRequest) -> tuple[Task, bool]:
        """Submits a task with strict idempotency and deduplication guarantees.

        Returns a tuple of (Task, is_created).
        If idempotency_key is present and already exists:
        - If payload and all task-defining configuration match: returns (existing_task, False) -> HTTP 200 OK.
        - If payload or configuration conflict: raises IdempotencyConflictError -> HTTP 409 Conflict.
        """

        def _verify_payload_match(existing_task: Task) -> None:
            existing_scheduled = _normalize_dt(existing_task.scheduled_at)
            req_scheduled = _normalize_dt(req.scheduled_at)

            if (
                existing_task.task_name != req.task_name
                or existing_task.task_type != req.task_type
                or existing_task.payload != req.payload
                or existing_task.priority != req.priority
                or existing_task.timeout_seconds != req.timeout_seconds
                or existing_task.max_retries != req.max_retries
                or existing_scheduled != req_scheduled
            ):
                raise IdempotencyConflictError(
                    key=req.idempotency_key or "",
                    message=(
                        f"Idempotency key '{req.idempotency_key}' collision with conflicting task payload or parameters."
                    ),
                )

        if req.idempotency_key:
            existing = self.repo.get_by_idempotency_key(req.idempotency_key)
            if existing:
                _verify_payload_match(existing)
                logger.info(
                    "task_submission_deduplicated",
                    task_id=existing.id,
                    idempotency_key=req.idempotency_key,
                )
                return existing, False

        task, is_created = self.repo.create_task(
            task_name=req.task_name,
            task_type=req.task_type,
            payload=req.payload,
            priority=req.priority,
            idempotency_key=req.idempotency_key,
            timeout_seconds=req.timeout_seconds,
            max_retries=req.max_retries,
            scheduled_at=req.scheduled_at,
        )

        if not is_created:
            _verify_payload_match(task)
            logger.info(
                "task_submission_deduplicated_after_collision",
                task_id=task.id,
                idempotency_key=req.idempotency_key,
            )
            return task, False

        logger.info(
            "task_submitted_successfully",
            task_id=task.id,
            task_type=task.task_type,
            priority=task.priority,
        )
        return task, True

    def get_task(self, task_id: str) -> Task | None:
        return self.repo.get_by_id(task_id)

    def list_tasks(
        self,
        status: str | None = None,
        task_type: str | None = None,
        priority: int | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[Task], int]:
        return self.repo.list_tasks(
            status=status,
            task_type=task_type,
            priority=priority,
            limit=limit,
            offset=offset,
        )

    def cancel_task(self, task_id: str, reason: str = "user_requested") -> Task:
        return self.repo.cancel_task(task_id, reason=reason)

    def manual_retry(self, task_id: str) -> Task:
        task = self.repo.get_by_id(task_id)
        if not task:
            raise ValueError(f"Task with id {task_id} not found")

        # Can only manually retry if in FAILED or DEAD_LETTER
        if task.status not in {TaskStatus.FAILED.value, TaskStatus.DEAD_LETTER.value}:
            raise ValueError(
                f"Task is in status '{task.status}', can only retry FAILED or DEAD_LETTER tasks"
            )

        task.retry_count = 0  # reset retries for manual re-run
        if task.result:
            self.db.delete(task.result)

        from taskforge.database.models import OutboxEvent

        outbox_event = OutboxEvent(
            aggregate_id=task.id,
            event_type="task.retry",
            payload={
                "task_id": task.id,
                "task_name": task.task_name,
                "task_type": task.task_type,
                "priority": task.priority,
                "payload": task.payload,
                "timeout_seconds": task.timeout_seconds,
            },
        )
        self.db.add(outbox_event)
        self.db.commit()

        # Update status back to QUEUED using force=True
        return self.repo.update_status(
            task_id, TaskStatus.QUEUED, {"reason": "manual_retry_triggered"}, force=True
        )
