import threading
import time
from typing import Optional
from taskforge.core.logging import get_logger
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal

logger = get_logger(__name__)


class LeaseManager:
    """Manages active worker leases on tasks with background auto-renewal."""

    def __init__(self, task_id: str, worker_id: str, lease_duration: int = 30):
        self.task_id = task_id
        self.worker_id = worker_id
        self.lease_duration = lease_duration
        self._renewal_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def acquire(self) -> bool:
        with SessionLocal() as db:
            repo = TaskRepository(db)
            acquired = repo.acquire_lease(self.task_id, self.worker_id, self.lease_duration)
            if acquired:
                self._start_renewal()
            return acquired

    def _start_renewal(self) -> None:
        self._stop_event.clear()
        interval = max(1.0, self.lease_duration / 2.0)
        self._renewal_thread = threading.Thread(
            target=self._renewal_loop,
            args=(interval,),
            name=f"LeaseRenew-{self.task_id[:8]}",
            daemon=True,
        )
        self._renewal_thread.start()

    def _renewal_loop(self, interval: float) -> None:
        while not self._stop_event.is_set():
            if self._stop_event.wait(interval):
                break
            try:
                with SessionLocal() as db:
                    repo = TaskRepository(db)
                    repo.renew_lease(self.task_id, self.worker_id, self.lease_duration)
                    logger.debug("lease_renewed", task_id=self.task_id, worker_id=self.worker_id)
            except Exception as exc:
                logger.warning("lease_renewal_failed", task_id=self.task_id, error=str(exc))

    def release(self) -> None:
        self._stop_event.set()
        if self._renewal_thread:
            self._renewal_thread.join(timeout=1.0)
        try:
            with SessionLocal() as db:
                repo = TaskRepository(db)
                repo.release_lease(self.task_id, self.worker_id)
        except Exception as exc:
            logger.debug("lease_release_error", task_id=self.task_id, error=str(exc))
