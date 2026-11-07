import json
import queue
import threading
from typing import Any, Dict, Optional
import redis

from taskforge.core.logging import get_logger

logger = get_logger(__name__)


class MessageBroker:
    """Interface for message broker."""

    def enqueue(self, task_id: str, priority: int, payload: Dict[str, Any]) -> None:
        raise NotImplementedError

    def dequeue(self, timeout_seconds: float = 1.0) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def get_queue_depth(self) -> int:
        raise NotImplementedError


class MemoryBroker(MessageBroker):
    """
    In-memory thread-safe Priority Queue broker.
    Prioritizes higher priority numbers (e.g. 4 > 3 > 2 > 1).
    Python's PriorityQueue pops the lowest value first, so we store negative priority.
    """

    def __init__(self):
        self._queue = queue.PriorityQueue()
        self._counter = 0
        self._lock = threading.Lock()

    def enqueue(self, task_id: str, priority: int, payload: Dict[str, Any]) -> None:
        with self._lock:
            self._counter += 1
            item = {
                "task_id": task_id,
                "priority": priority,
                "payload": payload,
            }
            # (-priority, counter, item) ensures FIFO among same priority
            self._queue.put((-priority, self._counter, item))
            logger.debug("memory_broker_enqueued", task_id=task_id, priority=priority)

    def dequeue(self, timeout_seconds: float = 1.0) -> Optional[Dict[str, Any]]:
        try:
            _, _, item = self._queue.get(timeout=timeout_seconds)
            return item
        except queue.Empty:
            return None

    def get_queue_depth(self) -> int:
        return self._queue.qsize()


class RedisBroker(MessageBroker):
    """
    Redis-backed Priority Queue broker using Redis Sorted Sets (ZSET).
    Score = priority (higher is popped first via ZPOPMIN/ZPOPMAX) or separate priority lists.
    """

    def __init__(self, redis_client: redis.Redis, queue_name: str = "taskforge:tasks"):
        self.client = redis_client
        self.queue_name = queue_name

    def enqueue(self, task_id: str, priority: int, payload: Dict[str, Any]) -> None:
        message = json.dumps({
            "task_id": task_id,
            "priority": priority,
            "payload": payload,
        })
        # Score is priority (e.g. 1 to 4)
        self.client.zadd(self.queue_name, {message: priority})
        logger.debug("redis_broker_enqueued", task_id=task_id, priority=priority)

    def dequeue(self, timeout_seconds: float = 1.0) -> Optional[Dict[str, Any]]:
        # ZPOPMAX pops the item with highest score (highest priority)
        res = self.client.zpopmax(self.queue_name, count=1)
        if res:
            message_str, _ = res[0]
            return json.loads(message_str)
        return None

    def get_queue_depth(self) -> int:
        try:
            return self.client.zcard(self.queue_name)
        except Exception:
            return 0


# Singleton global broker instance
_global_broker: Optional[MessageBroker] = None
_broker_lock = threading.Lock()


def get_broker(redis_client: Optional[redis.Redis] = None) -> MessageBroker:
    global _global_broker
    with _broker_lock:
        if _global_broker is None:
            if redis_client:
                try:
                    redis_client.ping()
                    _global_broker = RedisBroker(redis_client)
                    logger.info("using_redis_broker")
                    return _global_broker
                except Exception:
                    pass
            _global_broker = MemoryBroker()
            logger.info("using_memory_broker")
        return _global_broker


def set_global_broker(broker: MessageBroker) -> None:
    global _global_broker
    with _broker_lock:
        _global_broker = broker
