import os
import threading
import time

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from taskforge.api.schemas import APIErrorResponse, ErrorDetail
from taskforge.core.config import get_settings


class InMemoryRateLimiter:
    """Thread-safe sliding window rate limiter."""

    def __init__(self, requests_per_minute: int = 300):
        self.requests_per_minute = requests_per_minute
        self.window_seconds = 60.0
        self._clients: dict[str, list[float]] = {}
        self._lock = threading.Lock()

    def is_allowed(self, client_key: str) -> tuple[bool, int, float]:
        """
        Check if request is allowed.
        Returns (is_allowed, remaining_requests, retry_after_seconds).
        """
        now = time.time()
        cutoff = now - self.window_seconds

        with self._lock:
            if client_key not in self._clients:
                self._clients[client_key] = []

            # Filter timestamps older than cutoff
            timestamps = [t for t in self._clients[client_key] if t > cutoff]
            self._clients[client_key] = timestamps

            if len(timestamps) >= self.requests_per_minute:
                oldest_in_window = timestamps[0]
                retry_after = max(1.0, round(oldest_in_window + self.window_seconds - now, 1))
                return False, 0, retry_after

            timestamps.append(now)
            remaining = self.requests_per_minute - len(timestamps)
            return True, remaining, 0.0


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, requests_per_minute: int = 300):
        super().__init__(app)
        self.limiter = InMemoryRateLimiter(requests_per_minute=requests_per_minute)

    async def dispatch(self, request: Request, call_next) -> Response:
        if os.getenv("TASKFORGE_TESTING") == "1" or not get_settings().rate_limit_enabled:
            return await call_next(request)

        # Exempt health checks, metrics, and static UI assets from rate limiting
        path = request.url.path
        if (
            path.startswith("/health")
            or path.startswith("/metrics")
            or path.startswith("/static")
            or path == "/"
            or path == "/docs"
            or path == "/openapi.json"
        ):
            return await call_next(request)

        client_ip = request.client.host if request.client else "127.0.0.1"
        allowed, remaining, retry_after = self.limiter.is_allowed(client_ip)

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
