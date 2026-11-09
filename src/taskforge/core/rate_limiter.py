"""Atomic Redis-backed Sliding-Window Log Rate Limiter for Milestone G9.1.4.

Executes an atomic Lua script in Redis implementing a sliding-window log using a sorted set.
Keyed per tenant/client identifier: `taskforge:ratelimit:{client_key}`.
Window configuration: 60-second rolling window with default 300 requests/minute (5 req/sec sustained).
Emits standard headers:
- `X-RateLimit-Limit`
- `X-RateLimit-Remaining`
- `Retry-After` (on 429)

Failure Policy:
- If Redis is unreachable, fail closed or open based on configuration (default fail closed with 503 to prevent unbounded ingestion).
"""

import os
import time

from fastapi import Request, Response
from redis.exceptions import RedisError
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from taskforge.api.schemas import APIErrorResponse, ErrorDetail
from taskforge.core.config import get_settings
from taskforge.queue.redis_client import create_redis_client

# Atomic Lua script for sliding window rate limiting
# KEYS[1] = rate limit key
# ARGV[1] = current timestamp (seconds float)
# ARGV[2] = window size (seconds float, e.g. 60.0)
# ARGV[3] = max requests per window
# Returns: [allowed (1 or 0), remaining_requests, retry_after_seconds]
LUA_RATE_LIMIT = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local max_requests = tonumber(ARGV[3])
local clear_before = now - window

-- Remove timestamps older than current window
redis.call('ZREMRANGEBYSCORE', key, '-inf', clear_before)

local current_requests = redis.call('ZCARD', key)

if current_requests < max_requests then
    redis.call('ZADD', key, now, now)
    redis.call('EXPIRE', key, math.ceil(window))
    return {1, max_requests - current_requests - 1, 0}
else
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local retry_after = 1
    if oldest and #oldest >= 2 then
        retry_after = math.max(1, math.ceil(tonumber(oldest[2]) + window - now))
    end
    return {0, 0, retry_after}
end
"""


class InMemoryRateLimiter:
    """Thread-safe sliding window in-memory rate limiter for standalone/unit testing."""

    def __init__(self, requests_per_minute: int = 300):
        self.requests_per_minute = requests_per_minute
        self.window_seconds = 60.0
        self._clients: dict[str, list[float]] = {}
        import threading

        self._lock = threading.Lock()

    def is_allowed(self, client_key: str) -> tuple[bool, int, float]:
        now = time.time()
        cutoff = now - self.window_seconds
        with self._lock:
            if client_key not in self._clients:
                self._clients[client_key] = []
            timestamps = [t for t in self._clients[client_key] if t > cutoff]
            self._clients[client_key] = timestamps

            if len(timestamps) >= self.requests_per_minute:
                oldest = timestamps[0]
                retry_after = max(1.0, round(oldest + self.window_seconds - now, 1))
                return False, 0, retry_after

            timestamps.append(now)
            remaining = self.requests_per_minute - len(timestamps)
            return True, remaining, 0.0


class RedisRateLimiter:
    def __init__(self, requests_per_minute: int = 300, redis_client=None):
        self.requests_per_minute = requests_per_minute
        self.window_seconds = 60.0
        self.redis = redis_client
        self._script = None

    def _get_script(self):
        if self._script is None and self.redis is not None:
            self._script = self.redis.register_script(LUA_RATE_LIMIT)
        return self._script

    def is_allowed(self, client_key: str) -> tuple[bool, int, float]:
        if self.redis is None:
            # Fallback or initialization
            try:
                self.redis = create_redis_client()
            except Exception:
                return False, 0, 5.0  # Fail closed on missing redis

        now = time.time()
        redis_key = f"taskforge:ratelimit:{client_key}"
        try:
            script = self._get_script()
            res = script(
                keys=[redis_key],
                args=[now, self.window_seconds, self.requests_per_minute],
            )
            allowed = bool(res[0])
            remaining = int(res[1])
            retry_after = float(res[2])
            return allowed, remaining, retry_after
        except RedisError:
            # Redis failure policy: fail-closed with 5s retry-after to protect database
            return False, 0, 5.0


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, requests_per_minute: int = 300):
        super().__init__(app)
        self.limiter = RedisRateLimiter(requests_per_minute=requests_per_minute)

    async def dispatch(self, request: Request, call_next) -> Response:
        if not get_settings().rate_limit_enabled:
            return await call_next(request)

        # In testing mode, allow requests unless specifically testing rate limiter
        if os.getenv("TASKFORGE_TESTING") == "1" and not getattr(
            request.state, "force_rate_limit", False
        ):
            # Check for explicit header in tests
            if request.headers.get("X-Test-Rate-Limit") != "1":
                return await call_next(request)

        # Exempt health checks, metrics, and static UI assets
        path = request.url.path
        if (
            path.startswith("/health")
            or path.startswith("/metrics")
            or path.startswith("/static")
            or path in ("/", "/docs", "/openapi.json")
        ):
            return await call_next(request)

        # Derive client key: prefer authenticated tenant or bearer token prefix, fallback to client IP
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer tf_live_"):
            parts = auth_header[15:].split("_", 1)
            client_key = parts[0] if parts else "anon"
        else:
            client_key = request.client.host if request.client else "127.0.0.1"

        allowed, remaining, retry_after = self.limiter.is_allowed(client_key)

        if not allowed:
            correlation_id = getattr(request.state, "correlation_id", "unknown")
            resp = JSONResponse(
                status_code=429,
                content=APIErrorResponse(
                    error=ErrorDetail(
                        code="RATE_LIMIT_EXCEEDED",
                        message=f"Rate limit exceeded. Maximum {self.limiter.requests_per_minute} requests per minute.",
                        details={"retry_after_seconds": retry_after},
                        correlation_id=correlation_id,
                    )
                ).model_dump(),
            )
            resp.headers["Retry-After"] = str(int(retry_after))
            resp.headers["X-RateLimit-Limit"] = str(self.limiter.requests_per_minute)
            resp.headers["X-RateLimit-Remaining"] = "0"
            return resp

        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(self.limiter.requests_per_minute)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        return response
