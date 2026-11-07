import concurrent.futures
import random
import threading
import time
import traceback
from typing import Any, Dict, Optional

from taskforge.core.logging import get_logger
from taskforge.core.state_machine import TaskStatus, is_terminal_state
from taskforge.database.models import TaskAttempt, get_utc_now
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal
from taskforge.queue.broker import get_broker
import taskforge.tasks  # noqa: F401
from taskforge.tasks.registry import get_handler
from taskforge.workers.lease_manager import LeaseManager

logger = get_logger(__name__)


def calculate_backoff(attempt: int, base: float = 1.0, max_backoff: float = 60.0) -> float:
    """Calculate exponential backoff with full jitter."""
    exp = min(attempt, 6)
    backoff = min(max_backoff, base * (2 ** exp))
    jitter = random.uniform(0.05, 0.5)
    return backoff + jitter


def execute_task(task_id: str, worker_id: str) -> bool:
    """
    Executes a single task run with lease management, timeout enforcement,
    attempt tracking, result persistence, and retry backoff.
    Returns True if execution succeeded, False otherwise.
    """
    lease = LeaseManager(task_id, worker_id)
    if not lease.acquire():
        logger.warning("task_lease_acquisition_failed", task_id=task_id, worker_id=worker_id)
        return False

    start_time = time.perf_counter()
    start_utc = get_utc_now()
    attempt_num = 1
    attempt_id: Optional[int] = None
    timeout_seconds = 60
    max_retries = 3
    current_retries = 0

    try:
        with SessionLocal() as db:
            repo = TaskRepository(db)
            task = repo.get_by_id(task_id)
            if not task:
                logger.error("task_not_found_for_execution", task_id=task_id)
                return False

            # AC-12: Duplicate message handling - if already in terminal state, ignore safely
            if is_terminal_state(task.status):
                logger.info(
                    "duplicate_delivery_ignored_terminal_state",
                    task_id=task_id,
                    status=task.status,
                )
                return True

            attempt_num = task.retry_count + 1

            if task.status == TaskStatus.RETRY_WAIT.value:
                repo.update_status(task_id, TaskStatus.QUEUED, {"reason": "retrying_execution"})

            # Transition to RUNNING
            repo.update_status(task_id, TaskStatus.RUNNING, {"worker_id": worker_id})

            # Record attempt start
            attempt = repo.record_attempt(
                task_id=task_id,
                attempt_number=attempt_num,
                worker_id=worker_id,
                status="RUNNING",
                started_at=start_utc,
            )
            attempt_id = attempt.id
            task_type = task.task_type
            payload = dict(task.payload)
            payload["_current_attempt"] = attempt_num
            timeout_seconds = task.timeout_seconds
            max_retries = task.max_retries
            current_retries = task.retry_count

        # Execute handler with timeout enforcement
        handler = get_handler(task_type)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(handler, payload)
            result_data = future.result(timeout=timeout_seconds)

        duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
        end_utc = get_utc_now()

        # Success path
        with SessionLocal() as db:
            repo = TaskRepository(db)
            repo.record_result(
                task_id=task_id,
                return_value=result_data,
                execution_metadata={
                    "worker_id": worker_id,
                    "attempt": attempt_num,
                    "duration_ms": duration_ms,
                },
            )
            repo.update_status(
                task_id,
                TaskStatus.SUCCESS,
                {"worker_id": worker_id, "duration_ms": duration_ms},
            )

            # Update attempt record
            att = db.get(TaskAttempt, attempt_id)
            if att:
                att.status = "SUCCESS"
                att.completed_at = end_utc
                att.duration_ms = duration_ms
                db.commit()

        logger.info(
            "task_completed_successfully",
            task_id=task_id,
            attempt=attempt_num,
            duration_ms=duration_ms,
        )
        return True

    except Exception as exc:
        duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
        end_utc = get_utc_now()
        is_timeout = isinstance(exc, concurrent.futures.TimeoutError)
        error_msg = f"Task timed out after {timeout_seconds}s" if is_timeout else str(exc)
        tb_str = traceback.format_exc()

        logger.warning(
            "task_execution_failed",
            task_id=task_id,
            attempt=attempt_num,
            error=error_msg,
            timeout=is_timeout,
        )

        with SessionLocal() as db:
            repo = TaskRepository(db)
            task = repo.get_by_id(task_id)
            if not task:
                return False

            # Update attempt record
            if attempt_id is not None:
                att = db.get(TaskAttempt, attempt_id)
                if att:
                    att.status = "TIMED_OUT" if is_timeout else "FAILED"
                    att.error_message = error_msg
                    att.error_traceback = tb_str
                    att.completed_at = end_utc
                    att.duration_ms = duration_ms
                    db.commit()

            # Retry evaluation
            if current_retries < max_retries:
                task.retry_count = current_retries + 1
                db.commit()
                repo.update_status(
                    task_id,
                    TaskStatus.RETRY_WAIT,
                    {"error": error_msg, "retry_count": task.retry_count},
                )
                backoff_delay = calculate_backoff(task.retry_count)

                # Schedule retry re-enqueue
                def re_enqueue():
                    time.sleep(backoff_delay)
                    try:
                        with SessionLocal() as re_db:
                            re_repo = TaskRepository(re_db)
                            cur = re_repo.get_by_id(task_id)
                            if cur and cur.status == TaskStatus.RETRY_WAIT.value:
                                re_repo.update_status(
                                    task_id,
                                    TaskStatus.QUEUED,
                                    {"reason": "retry_backoff_elapsed"},
                                )
                                broker = get_broker()
                                broker.enqueue(task_id, cur.priority, cur.payload)
                    except Exception as err:
                        logger.error("retry_re_enqueue_error", task_id=task_id, error=str(err))

                threading.Thread(target=re_enqueue, daemon=True).start()

            else:
                # Retries exhausted -> FAILED -> DEAD_LETTER
                repo.update_status(
                    task_id,
                    TaskStatus.FAILED,
                    {"error": error_msg, "retries_exhausted": True},
                )
                repo.update_status(
                    task_id,
                    TaskStatus.DEAD_LETTER,
                    {"reason": "max_retries_exceeded", "final_error": error_msg},
                )
                logger.error("task_sent_to_dead_letter", task_id=task_id, attempts=attempt_num)

        return False

    finally:
        lease.release()
