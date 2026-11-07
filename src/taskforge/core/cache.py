import json
import threading
import time
from typing import Any, Dict, Optional
import redis

from taskforge.core.logging import get_logger

logger = get_logger(__name__)


class TaskCache:
    """Read-through and invalidation cache for tasks."""

    def __init__(self, redis_client: Optional[redis.Redis] = None, default_ttl: int = 30):
        self.redis_client = redis_client
        self.default_ttl = default_ttl
        self._memory_cache: Dict[str, Tuple[Dict[str, Any], float]] = {}
        self._lock = threading.Lock()

    def get(self, task_id: str) -> Optional[Dict[str, Any]]:
        key = f"taskforge:cache:task:{task_id}"
        if self.redis_client:
            try:
                val = self.redis_client.get(key)
                if val:
                    return json.loads(val)
            except Exception:
                pass

        with self._lock:
            if key in self._memory_cache:
                data, expiry = self._memory_cache[key]
                if time.time() < expiry:
                    return data
                del self._memory_cache[key]
        return None

    def set(self, task_id: str, data: Dict[str, Any], ttl: Optional[int] = None) -> None:
        effective_ttl = ttl or self.default_ttl
        key = f"taskforge:cache:task:{task_id}"

        if self.redis_client:
            try:
                self.redis_client.set(key, json.dumps(data, default=str), ex=effective_ttl)
            except Exception:
                pass

        with self._lock:
            self._memory_cache[key] = (data, time.time() + effective_ttl)

    def invalidate(self, task_id: str) -> None:
        key = f"taskforge:cache:task:{task_id}"
        if self.redis_client:
            try:
                self.redis_client.delete(key)
            except Exception:
                pass

        with self._lock:
            self._memory_cache.pop(key, None)


_global_cache: Optional[TaskCache] = None
_cache_lock = threading.Lock()


def get_cache(redis_client: Optional[redis.Redis] = None) -> TaskCache:
    global _global_cache
    with _cache_lock:
        if _global_cache is None:
            _global_cache = TaskCache(redis_client=redis_client)
        return _global_cache

