import datetime
import socket
import threading
import time
from typing import Any, Dict, List, Optional
import redis

from taskforge.core.logging import get_logger

logger = get_logger(__name__)

# In-memory registry fallback for workers
_WORKER_HEARTBEATS: Dict[str, Dict[str, Any]] = {}
_lock = threading.Lock()


class WorkerHeartbeat:
    """Manages worker status and periodic heartbeat emission."""

    def __init__(
        self,
        worker_id: str,
        concurrency: int = 4,
        redis_client: Optional[redis.Redis] = None,
        heartbeat_interval: float = 5.0,
    ):
        self.worker_id = worker_id
        self.concurrency = concurrency
        self.redis_client = redis_client
        self.heartbeat_interval = heartbeat_interval
        self.hostname = socket.gethostname()
        self.active_tasks: int = 0
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.started_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

    def start(self) -> None:
        self._stop_event.clear()
        self.beat()  # Initial heartbeat
        self._thread = threading.Thread(
            target=self._run, name=f"Heartbeat-{self.worker_id}", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=1.0)
        with _lock:
            _WORKER_HEARTBEATS.pop(self.worker_id, None)

    def set_active_tasks(self, count: int) -> None:
        self.active_tasks = max(0, count)
        self.beat()

    def beat(self) -> None:
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        info = {
            "worker_id": self.worker_id,
            "hostname": self.hostname,
            "concurrency": self.concurrency,
            "active_tasks": self.active_tasks,
            "started_at": self.started_at,
            "last_heartbeat": now_iso,
            "status": "healthy",
        }
        with _lock:
            _WORKER_HEARTBEATS[self.worker_id] = info

        if self.redis_client:
            try:
                import json
                key = f"taskforge:worker:{self.worker_id}"
                self.redis_client.set(key, json.dumps(info), ex=int(self.heartbeat_interval * 3))
            except Exception:
                pass

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self.beat()
            self._stop_event.wait(self.heartbeat_interval)


def get_active_workers() -> List[Dict[str, Any]]:
    """Return all currently active workers whose heartbeat is within TTL (15s)."""
    now = datetime.datetime.now(datetime.timezone.utc)
    active = []
    with _lock:
        for wid, info in list(_WORKER_HEARTBEATS.items()):
            try:
                last_beat = datetime.datetime.fromisoformat(info["last_heartbeat"])
                if (now - last_beat).total_seconds() <= 15:
                    active.append(info)
            except Exception:
                active.append(info)
    return active

