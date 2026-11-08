import threading
import uuid

from taskforge.core.logging import get_logger
from taskforge.database.repositories.task_repository import TaskRepository
from taskforge.database.session import SessionLocal

logger = get_logger(__name__)


class LeaseManager:
    """Manages active worker leases on tasks with background auto-renewal and fencing tokens."""

    def __init__(
        self,
        task_id: str,
        worker_id: str,
        lease_duration: int = 30,
        lease_token: str | None = None,
    ):
        self.task_id = task_id
        self.worker_id = worker_id
        self.lease_duration = lease_duration
        self.lease_token: str | None = lease_token
        self._renewal_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._ownership_lost = False

    @property
    def is_ownership_lost(self) -> bool:
        return self._ownership_lost

    def stop_renewal(self) -> None:
        """Immediately halts background lease renewal and marks ownership lost.
        Allows lease to expire naturally in PostgreSQL without blocking caller.
        """
        self._ownership_lost = True
        self._stop_event.set()
        if self._renewal_thread and self._renewal_thread.is_alive():
            self._renewal_thread.join(timeout=1.0)

    def acquire(self) -> bool:
        if not self.lease_token:
            self.lease_token = f"lease-{uuid.uuid4().hex}"
        with SessionLocal() as db:
            repo = TaskRepository(db)
            acquired = repo.acquire_lease(
                self.task_id,
                self.worker_id,
                self.lease_duration,
                lease_token=self.lease_token,
            )
            if acquired:
                self._start_renewal()
                return True
            self.lease_token = None
            return False

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
            if not self.lease_token:
                break
            try:
                with SessionLocal() as db:
                    repo = TaskRepository(db)
                    renewed = repo.renew_lease(self.task_id, self.lease_token, self.lease_duration)
                    if renewed:
                        logger.debug(
                            "lease_renewed",
                            task_id=self.task_id,
                            lease_token=self.lease_token,
                        )
                    else:
                        self._ownership_lost = True
                        logger.warning(
                            "lease_renewal_rejected_lost_ownership",
                            task_id=self.task_id,
                            lease_token=self.lease_token,
                        )
                        break
            except Exception as exc:
                logger.warning("lease_renewal_failed", task_id=self.task_id, error=str(exc))

    def release(self) -> None:
        self._stop_event.set()
        if self._renewal_thread:
            self._renewal_thread.join(timeout=1.0)
        if not self.lease_token:
            return
        try:
            with SessionLocal() as db:
                repo = TaskRepository(db)
                repo.release_lease(self.task_id, self.lease_token)
        except Exception as exc:
            logger.debug("lease_release_error", task_id=self.task_id, error=str(exc))
