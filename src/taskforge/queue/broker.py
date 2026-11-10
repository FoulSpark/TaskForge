import json
import queue
import threading
import time
import uuid
from typing import Any

import redis

from taskforge.core.config import get_settings
from taskforge.core.logging import get_logger

logger = get_logger(__name__)

# ============================================================================
# Lua Scripts for Atomic Priority Queue & Visibility Timeout Invariants
# ============================================================================

# Enqueue with strict priority + per-priority monotonic sequence FIFO tie-breaking:
# Priorities: 1 (LOW), 2 (MEDIUM), 3 (HIGH), 4 (CRITICAL).
# Score Band Formula: score = (priority * 10^12) - seq
#
# Boundary and Separation Proof:
# 1. Per-priority sequence: Each priority has an independent counter (seq_key:priority).
#    Traffic in one priority tier never consumes sequence space in another tier.
# 2. Maximum sequence per tier: MAX_SEQ = 10^12 - 1 = 999,999,999,999.
# 3. Disjoint score bands:
#    - Priority 4: [3,000,000,000,001  ..  3,999,999,999,999]
#    - Priority 3: [2,000,000,000,001  ..  2,999,999,999,999]
#    - Priority 2: [1,000,000,000,001  ..  1,999,999,999,999]
#    - Priority 1: [                 1  ..    999,999,999,999]
#    For any adjacent tiers: min_score(p) = (p-1)*10^12 + 1 > max_score(p-1) = (p-1)*10^12 - 1.
#    The bands are strictly disjoint with a safety gap of 2 between tiers.
# 4. IEEE-754 precision: Double precision has 53 bits of mantissa (exact integers up to 9.007 * 10^15).
#    The maximum score 4 * 10^12 is 2,250x smaller than 2^53 - 1. Zero precision loss.
LUA_ENQUEUE = """
local queue = KEYS[1]
local base_seq_key = KEYS[2]
local task_id = ARGV[1]
local priority = tonumber(ARGV[2])
local payload = ARGV[3]

if not priority or priority < 1 or priority > 4 then
    priority = 2
end

local seq_key = base_seq_key .. ':' .. priority
local seq = redis.call('INCR', seq_key)

local MAX_SEQ = 999999999999
if seq > MAX_SEQ then
    return redis.error_reply("Sequence boundary exceeded for priority tier")
end

local score = (priority * 1000000000000) - seq
redis.call('ZADD', queue, score, payload)
return seq
"""

# Dequeue with atomic transition from ready queue to in_flight queue with delivery fencing token:
LUA_DEQUEUE = """
local queue = KEYS[1]
local in_flight = KEYS[2]
local now = tonumber(ARGV[1])
local visibility_timeout = tonumber(ARGV[2])
local delivery_token = ARGV[3]

local res = redis.call('ZPOPMAX', queue, 1)
if #res == 0 then
    return nil
end

local msg = res[1]
local expires_at = now + visibility_timeout

local envelope = cjson.encode({
    task_id = cjson.decode(msg)['task_id'],
    delivery_token = delivery_token,
    raw_message = msg
})

redis.call('ZADD', in_flight, expires_at, envelope)
return envelope
"""

# Acknowledge completion with strict delivery token fencing:
# Verifies delivery_token so an expired/stale delivery cannot remove a newer delivery of the same task.
# Empty token is strictly rejected (no bypass permitted).
LUA_ACK = """
local in_flight = KEYS[1]
local task_id = ARGV[1]
local delivery_token = ARGV[2]

if not delivery_token or delivery_token == "" then
    return 0
end

local items = redis.call('ZRANGE', in_flight, 0, -1)
for i, item in ipairs(items) do
    local ok, env = pcall(cjson.decode, item)
    if ok and env['task_id'] == task_id and env['delivery_token'] == delivery_token then
        redis.call('ZREM', in_flight, item)
        return 1
    end
end
return 0
"""

# Reclaim expired in-flight messages back to ready queue:
# Semantics & Invariants:
# 1. Reclaimed messages receive a newly allocated sequence number within their priority tier,
#    guaranteeing FIFO ordering by re-entry into the ready queue.
# 2. Priority validation: priorities outside 1-4 default to 2 (MEDIUM).
# 3. Sequence boundary: enforces MAX_SEQ = 999,999,999,999 identical to LUA_ENQUEUE.
#    Sequence capacity is validated BEFORE any destructive ZREM or INCR mutation.
#    If the boundary is exceeded, fails closed with an error reply without removing the message.
# 4. Decode safety: cjson.decode is wrapped in pcall for both envelope and payload.
#    Malformed envelopes are not removed destructively.
LUA_RECLAIM = """
local in_flight = KEYS[1]
local queue = KEYS[2]
local base_seq_key = KEYS[3]
local now = tonumber(ARGV[1])

local expired = redis.call('ZRANGEBYSCORE', in_flight, 0, now)
local reclaimed_count = 0
local MAX_SEQ = 999999999999

for i, item in ipairs(expired) do
    local ok_env, env = pcall(cjson.decode, item)
    if ok_env and type(env) == 'table' and env['raw_message'] then
        local raw_msg = env['raw_message']
        local ok_data, data = pcall(cjson.decode, raw_msg)
        if ok_data and type(data) == 'table' then
            local priority = tonumber(data['priority'])
            if not priority or priority < 1 or priority > 4 then
                priority = 2
            end
            local seq_key = base_seq_key .. ':' .. priority
            local cur_seq = tonumber(redis.call('GET', seq_key) or 0)
            if cur_seq >= MAX_SEQ then
                return redis.error_reply("Sequence boundary exceeded for priority tier during reclaim")
            end
            -- Safe to mutate: sequence is bounded and envelope is valid
            local seq = redis.call('INCR', seq_key)
            redis.call('ZREM', in_flight, item)
            local score = (priority * 1000000000000) - seq
            redis.call('ZADD', queue, score, raw_msg)
            reclaimed_count = reclaimed_count + 1
        end
    end
end

return reclaimed_count
"""


class MessageBroker:
    """Interface for message broker."""

    def enqueue(self, task_id: str, priority: int, payload: dict[str, Any]) -> None:
        raise NotImplementedError

    def dequeue(self, timeout_seconds: float = 1.0) -> dict[str, Any] | None:
        raise NotImplementedError

    def ack(self, task_id: str, message: dict[str, Any] | None = None) -> bool:
        return True

    def reclaim_expired(self, timeout_seconds: float = 30.0) -> int:
        return 0

    def get_queue_depth(self) -> int:
        raise NotImplementedError

    def get_in_flight_depth(self) -> int:
        return 0


class MemoryBroker(MessageBroker):
    """In-memory thread-safe Priority Queue broker with FIFO tie-breaking."""

    def __init__(self):
        self._queue = queue.PriorityQueue()
        self._counter = 0
        self._lock = threading.Lock()
        self._in_flight: dict[str, dict[str, Any]] = {}

    def enqueue(self, task_id: str, priority: int, payload: dict[str, Any]) -> None:
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

    def dequeue(self, timeout_seconds: float = 1.0) -> dict[str, Any] | None:
        try:
            _, _, item = self._queue.get(timeout=timeout_seconds)
            delivery_token = f"del-{uuid.uuid4().hex}"
            item["delivery_token"] = delivery_token
            with self._lock:
                self._in_flight[item["task_id"]] = {
                    "delivery_token": delivery_token,
                    "item": dict(item),
                    "expires_at": time.time() + 30.0,
                }
            return item
        except queue.Empty:
            return None

    def reclaim_expired(self, timeout_seconds: float = 30.0) -> int:
        now = time.time()
        reclaimed_count = 0
        with self._lock:
            expired_keys = [
                task_id
                for task_id, val in self._in_flight.items()
                if val.get("expires_at", 0) <= now
            ]
            for task_id in expired_keys:
                val = self._in_flight.pop(task_id, None)
                if val and "item" in val:
                    item = dict(val["item"])
                    item.pop("delivery_token", None)
                    self._counter += 1
                    priority = item.get("priority", 2)
                    self._queue.put((-priority, self._counter, item))
                    reclaimed_count += 1
        return reclaimed_count

    def ack(
        self,
        task_id: str,
        message: dict[str, Any] | None = None,
        delivery_token: str | None = None,
    ) -> bool:
        token = delivery_token or (message.get("delivery_token") if message else None)
        if not token:
            return False
        with self._lock:
            stored = self._in_flight.get(task_id)
            if not stored:
                return False
            if stored.get("delivery_token") != token:
                return False
            self._in_flight.pop(task_id, None)
            return True

    def get_queue_depth(self) -> int:
        return self._queue.qsize()

    def get_in_flight_depth(self) -> int:
        with self._lock:
            return len(self._in_flight)


class RedisBroker(MessageBroker):
    """Redis-backed Priority Queue broker using Redis Sorted Sets (ZSET)

    with atomic Lua-backed FIFO tie-breaking and in-flight visibility timeouts.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        queue_name: str = "taskforge:tasks",
        visibility_timeout_seconds: int = 30,
    ):
        self.client = redis_client
        self.queue_name = queue_name
        self.seq_key = f"{queue_name}:seq"
        self.in_flight_name = f"{queue_name}:in_flight"
        self.visibility_timeout = visibility_timeout_seconds

        # Preload Lua scripts
        self._sha_enqueue = self.client.script_load(LUA_ENQUEUE)
        self._sha_dequeue = self.client.script_load(LUA_DEQUEUE)
        self._sha_ack = self.client.script_load(LUA_ACK)
        self._sha_reclaim = self.client.script_load(LUA_RECLAIM)

    def _evalsha_with_reload(
        self, sha_attr_name: str, script_code: str, numkeys: int, *keys_and_args: Any
    ) -> Any:
        """Execute evalsha with automatic script cache reloading if Redis restarted/flushed."""
        try:
            sha = getattr(self, sha_attr_name)
            return self.client.evalsha(sha, numkeys, *keys_and_args)
        except redis.exceptions.NoScriptError:
            logger.warning("redis_script_cache_missing_reloading", script_attr=sha_attr_name)
            new_sha = self.client.script_load(script_code)
            setattr(self, sha_attr_name, new_sha)
            return self.client.evalsha(new_sha, numkeys, *keys_and_args)

    def enqueue(self, task_id: str, priority: int, payload: dict[str, Any]) -> None:
        message = json.dumps(
            {
                "task_id": task_id,
                "priority": priority,
                "payload": payload,
            }
        )
        self._evalsha_with_reload(
            "_sha_enqueue",
            LUA_ENQUEUE,
            2,
            self.queue_name,
            self.seq_key,
            task_id,
            priority,
            message,
        )
        logger.debug("redis_broker_enqueued", task_id=task_id, priority=priority)

    def dequeue(self, timeout_seconds: float = 1.0) -> dict[str, Any] | None:
        now = time.time()
        delivery_token = f"del-{uuid.uuid4().hex}"
        res = self._evalsha_with_reload(
            "_sha_dequeue",
            LUA_DEQUEUE,
            2,
            self.queue_name,
            self.in_flight_name,
            now,
            self.visibility_timeout,
            delivery_token,
        )
        if res:
            envelope_str = res.decode() if isinstance(res, bytes) else str(res)
            envelope = json.loads(envelope_str)
            raw_msg = envelope.get("raw_message", "{}")
            msg_dict = json.loads(raw_msg)
            msg_dict["delivery_token"] = envelope.get("delivery_token", delivery_token)
            msg_dict["_raw_envelope"] = envelope_str
            return msg_dict
        return None

    def ack(
        self,
        task_id: str,
        message: dict[str, Any] | None = None,
        delivery_token: str | None = None,
    ) -> bool:
        token = delivery_token or (message.get("delivery_token") if message else "") or ""
        if not token:
            logger.warning("ack_rejected_missing_delivery_token", task_id=task_id)
            return False
        removed = self._evalsha_with_reload(
            "_sha_ack",
            LUA_ACK,
            1,
            self.in_flight_name,
            task_id,
            token,
        )
        return bool(removed)

    def reclaim_expired(self, timeout_seconds: float = 30.0) -> int:
        now = time.time()
        reclaimed = self._evalsha_with_reload(
            "_sha_reclaim",
            LUA_RECLAIM,
            3,
            self.in_flight_name,
            self.queue_name,
            self.seq_key,
            now,
        )
        return int(reclaimed or 0)

    def get_queue_depth(self) -> int:
        # Fails closed on connection failure instead of masking with 0
        return self.client.zcard(self.queue_name)

    def get_in_flight_depth(self) -> int:
        return self.client.zcard(self.in_flight_name)


# Singleton global broker instance
_global_broker: MessageBroker | None = None
_broker_lock = threading.Lock()


def get_broker(redis_client: redis.Redis | None = None) -> MessageBroker:
    """Returns the globally configured broker, enforcing strict fail-closed

    behavior in Redis/production mode.
    """
    global _global_broker
    with _broker_lock:
        if _global_broker is None:
            settings = get_settings()
            broker_mode = getattr(settings, "broker_type", "redis").lower()

            if broker_mode == "redis":
                client = redis_client
                if client is None:
                    from taskforge.queue.redis_client import create_redis_client

                    client = create_redis_client()
                try:
                    client.ping()
                    _global_broker = RedisBroker(client)
                    logger.info("using_redis_broker")
                    return _global_broker
                except Exception as exc:
                    logger.critical(
                        "redis_broker_connection_failed_aborting",
                        error=str(exc),
                        url=settings.redis_url,
                    )
                    raise RuntimeError(
                        f"FATAL: Unable to connect to Redis broker at '{settings.redis_url}' in '{settings.app_env}' environment. "
                        f"Silent fallback to MemoryBroker is strictly prohibited in '{broker_mode}' mode."
                    ) from exc
            elif broker_mode == "memory":
                _global_broker = MemoryBroker()
                logger.info("using_memory_broker")
                return _global_broker
            else:
                raise ValueError(
                    f"Unknown broker_type: '{broker_mode}'. Must be 'redis' or 'memory'."
                )

        return _global_broker


def set_global_broker(broker: MessageBroker) -> None:
    global _global_broker
    with _broker_lock:
        _global_broker = broker
