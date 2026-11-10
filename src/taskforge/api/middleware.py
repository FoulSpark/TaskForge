import time
import uuid

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from taskforge.monitoring.metrics import normalize_route, record_api_request
from taskforge.monitoring.profiler import record_diagnostic_event


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
    """Resolve Starlette route template, matching against app route hierarchy with scope fallback."""
    try:
        app = getattr(request, "app", None)
        if app is not None and hasattr(app, "routes"):
            raw_path = request.url.path
            matched_template = _find_route_path(app.routes, raw_path)
            if matched_template:
                return normalize_route(matched_template)

        route_obj = request.scope.get("route")
        if route_obj is not None:
            template = getattr(route_obj, "path_format", None) or getattr(route_obj, "path", None)
            if template:
                return normalize_route(template)
    except Exception:
        pass

    return "other"


class PrometheusHttpMiddleware(BaseHTTPMiddleware):
    """Outermost HTTP middleware for end-to-end request latency and counter instrumentation.

    Guarantees:
    1. Measures duration of the complete HTTP lifecycle including streaming response bodies.
    2. Exactly-once metric observation across successful completions, streaming chunks, client disconnects, and unhandled exceptions.
    3. Strict telemetry failure isolation: metrics failures are caught, logged at DEBUG, and never mask application exceptions.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        start_time = time.perf_counter()
        mono_start_ns = time.perf_counter_ns()
        epoch_start = time.time()
        request.state.http_ingress_mono_ns = mono_start_ns
        request.state.http_ingress_epoch = epoch_start
        observed = False

        def observe_once(status_code: int) -> None:
            nonlocal observed
            if not observed:
                observed = True
                try:
                    duration = max(0.0, time.perf_counter() - start_time)
                    route = resolve_request_route_template(request)
                    record_api_request(
                        http_method=request.method,
                        route=route,
                        status_code=status_code,
                        duration_seconds=duration,
                    )
                except Exception:
                    pass

                try:
                    created_tid = getattr(request.state, "created_task_id", None)
                    if created_tid:
                        record_diagnostic_event(
                            "api_request_received",
                            task_id=created_tid,
                            t_mono_ns=mono_start_ns,
                            t_epoch_utc=epoch_start,
                        )
                        record_diagnostic_event(
                            "api_response_sent",
                            task_id=created_tid,
                        )
                except Exception:
                    pass

        try:
            response = await call_next(request)
        except Exception:
            observe_once(500)
            raise

        original_iterator = getattr(response, "body_iterator", None)
        if original_iterator is not None:
            if hasattr(original_iterator, "__aiter__"):
                async def async_streaming_wrapper():
                    try:
                        async for chunk in original_iterator:
                            yield chunk
                    except Exception:
                        observe_once(500)
                        raise
                    else:
                        observe_once(response.status_code)
                    finally:
                        observe_once(response.status_code)

                response.body_iterator = async_streaming_wrapper()
            else:
                def sync_streaming_wrapper():
                    try:
                        for chunk in original_iterator:
                            yield chunk
                    except Exception:
                        observe_once(500)
                        raise
                    else:
                        observe_once(response.status_code)
                    finally:
                        observe_once(response.status_code)

                response.body_iterator = sync_streaming_wrapper()
        else:
            observe_once(response.status_code)

        return response
