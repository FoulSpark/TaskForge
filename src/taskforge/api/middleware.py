import time
import uuid

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from taskforge.monitoring.metrics import normalize_route, record_api_request


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        correlation_id = request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
        request.state.correlation_id = correlation_id

        # Bind to structlog context
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(correlation_id=correlation_id)

        response = await call_next(request)
        response.headers["X-Correlation-ID"] = correlation_id
        return response


def _find_route_path(routes, raw_path: str) -> str | None:
    """Recursively search Starlette/FastAPI route hierarchy for matching path template."""
    for r in routes:
        if hasattr(r, "path_regex") and r.path_regex.match(raw_path):
            return getattr(r, "path_format", None) or getattr(r, "path", None)
        if hasattr(r, "effective_candidates"):
            candidates = r.effective_candidates() if callable(r.effective_candidates) else r.effective_candidates
            sub = _find_route_path(candidates, raw_path)
            if sub is not None:
                return sub
        if hasattr(r, "routes"):
            sub = _find_route_path(r.routes, raw_path)
            if sub is not None:
                return sub
    return None


def resolve_request_route_template(request: Request) -> str:
    """Resolve Starlette route template, falling back to route hierarchy search on early rejection."""
    try:
        # 1. Direct route object in scope (populated by Starlette router on successful dispatch)
        route_obj = request.scope.get("route")
        if route_obj is not None:
            template = getattr(route_obj, "path_format", None) or getattr(route_obj, "path", None)
            if template:
                return normalize_route(template)

        # 2. Fallback to router hierarchy inspection (e.g. RateLimitMiddleware early 429 rejection or 404)
        app = getattr(request, "app", None)
        if app is not None and hasattr(app, "routes"):
            raw_path = request.url.path
            matched_template = _find_route_path(app.routes, raw_path)
            if matched_template:
                return normalize_route(matched_template)
    except Exception:
        pass

    return "other"


class PrometheusHttpMiddleware(BaseHTTPMiddleware):
    """Outermost HTTP middleware for end-to-end request latency and counter instrumentation."""

    async def dispatch(self, request: Request, call_next) -> Response:
        start_time = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception as exc:
            duration = max(0.0, time.perf_counter() - start_time)
            route = resolve_request_route_template(request)
            record_api_request(
                http_method=request.method,
                route=route,
                status_code=500,
                duration_seconds=duration,
            )
            raise exc

        duration = max(0.0, time.perf_counter() - start_time)
        route = resolve_request_route_template(request)
        record_api_request(
            http_method=request.method,
            route=route,
            status_code=response.status_code,
            duration_seconds=duration,
        )
        return response
