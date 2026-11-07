from typing import Any, List, Optional, Tuple
from sqlalchemy.orm import Session

from taskforge.api.schemas import TaskCreateRequest
from taskforge.core.logging import get_logger
from taskforge.core.state_machine import TaskStatus
from taskforge.database.models import Task
from taskforge.database.repositories.task_repository import TaskRepository

logger = get_logger(__name__)


class TaskService:
    def __init__(self, db: Session):
        self.db = db
        self.repo = TaskRepository(db)

    def submit_task(self, req: TaskCreateRequest) -> Tuple[Task, bool]:
        """
        Submits a task.
        Returns a tuple of (Task, is_created).
        If idempotency_key is present and already exists, returns (existing_task, False).
        """
        if req.idempotency_key:
            existing = self.repo.get_by_idempotency_key(req.idempotency_key)
            if existing:
                logger.info(
                    "task_submission_deduplicated",
                    task_id=existing.id,
                    idempotency_key=req.idempotency_key,
                )
                return existing, False

        task = self.repo.create_task(
            task_name=req.task_name,
            task_type=req.task_type,
            payload=req.payload,
            priority=req.priority,
            idempotency_key=req.idempotency_key,
            timeout_seconds=req.timeout_seconds,
            max_retries=req.max_retries,
            scheduled_at=req.scheduled_at,
        )
        logger.info(
            "task_submitted_successfully",
            task_id=task.id,
            task_type=task.task_type,
            priority=task.priority,
        )
        return task, True

    def get_task(self, task_id: str) -> Optional[Task]:
        return self.repo.get_by_id(task_id)

    def list_tasks(
        self,
        status: Optional[str] = None,
        task_type: Optional[str] = None,
        priority: Optional[int] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Tuple[List[Task], int]:
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
            raise ValueError(f"Task is in status '{task.status}', can only retry FAILED or DEAD_LETTER tasks")

        task.retry_count = 0  # reset retries for manual re-run
        self.db.commit()

        # Update status back to QUEUED
        task.status = TaskStatus.QUEUED.value
        self.repo.update_status(task_id, TaskStatus.QUEUED, {"reason": "manual_retry_triggered"})
        return self.repo.get_by_id(task_id)  # type: ignore

