import time
import pytest
from taskforge.core.cache import TaskCache
from taskforge.core.rate_limiter import InMemoryRateLimiter
from taskforge.queue.broker import MemoryBroker


def test_rate_limiter_sliding_window():
    # 5 requests per minute limit
    limiter = InMemoryRateLimiter(requests_per_minute=5)
    client_id = "test-client-1"

    # First 5 should succeed
    for _ in range(5):
        allowed, remaining, _ = limiter.is_allowed(client_id)
        assert allowed is True

    # 6th should be rejected
    allowed, remaining, retry_after = limiter.is_allowed(client_id)
    assert allowed is False
    assert remaining == 0
    assert retry_after > 0


def test_priority_queue_ordering():
    broker = MemoryBroker()

    # Enqueue low priority task first, then critical task
    broker.enqueue("task-low", priority=1, payload={"msg": "low"})
    broker.enqueue("task-high", priority=3, payload={"msg": "high"})
    broker.enqueue("task-critical", priority=4, payload={"msg": "critical"})
    broker.enqueue("task-medium", priority=2, payload={"msg": "medium"})

    # Must dequeue in strict priority order: CRITICAL (4) -> HIGH (3) -> MEDIUM (2) -> LOW (1)
    first = broker.dequeue()
    assert first["task_id"] == "task-critical"
    assert first["priority"] == 4

    second = broker.dequeue()
    assert second["task_id"] == "task-high"
    assert second["priority"] == 3

    third = broker.dequeue()
    assert third["task_id"] == "task-medium"
    assert third["priority"] == 2

    fourth = broker.dequeue()
    assert fourth["task_id"] == "task-low"
    assert fourth["priority"] == 1


def test_task_cache_set_get_and_invalidate():
    cache = TaskCache(default_ttl=5)

    data = {"id": "task-123", "status": "SUCCESS", "result": 42}
    cache.set("task-123", data)

    # Cache hit
    hit = cache.get("task-123")
    assert hit is not None
    assert hit["status"] == "SUCCESS"

    # Invalidation
    cache.invalidate("task-123")
    miss = cache.get("task-123")
    assert miss is None

