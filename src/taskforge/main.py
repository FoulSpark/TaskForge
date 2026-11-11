import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from taskforge.api.errors import register_error_handlers
from taskforge.api.middleware import CorrelationIdMiddleware, PrometheusHttpMiddleware
from taskforge.api.routes import api_v1_router
from taskforge.api.routes.metrics import router as metrics_router
from taskforge.core.config import get_settings
from taskforge.core.logging import configure_logging, get_logger
from taskforge.core.rate_limiter import RateLimitMiddleware
from taskforge.database.session import engine, init_db
from taskforge.monitoring.collector import close_dedicated_telemetry_engine
from taskforge.monitoring.health import (
    ReadinessProbeCoordinator,
    ReadinessProbeSaturationError,
    ReadinessProbeShuttingDownError,
    check_database as health_check_database,
    check_redis as health_check_redis,
    close_probe_redis,
    get_readiness_coordinator,
    perform_readiness_checks,
)
from taskforge.queue.broker import get_broker
from taskforge.queue.outbox import OutboxPublisherWorker
from taskforge.queue.redis_client import create_redis_client
from taskforge.workers.dispatcher import WorkerDispatcher

settings = get_settings()
configure_logging(settings.log_level)

logger = get_logger(__name__)

# Background daemon references
outbox_publisher: OutboxPublisherWorker | None = None
worker_dispatcher: WorkerDispatcher | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global outbox_publisher, worker_dispatcher

    # Initialize dedicated readiness probe coordinator (max 2 workers)
    app.state.readiness_coordinator = ReadinessProbeCoordinator(max_workers=2)

    # Initialize DB schema
    try:
        init_db()
        logger.info("database_schema_initialized")
    except Exception as exc:
        logger.warning("database_schema_init_error", error=str(exc))

    try:
        app.state.redis = create_redis_client()
    except Exception as exc:
        logger.warning("redis_initialization_error", error=str(exc))
        app.state.redis = None

    broker = get_broker(getattr(app.state, "redis", None))

    # Start background Outbox Publisher and Worker Dispatcher for active live execution
    if settings.start_embedded_workers and os.getenv("TASKFORGE_TESTING") != "1":
        outbox_publisher = OutboxPublisherWorker(poll_interval=0.2, broker=broker)
        outbox_publisher.start()

        worker_dispatcher = WorkerDispatcher(
            worker_id="taskforge-worker-main",
            concurrency=4,
            broker=broker,
            poll_interval=0.1,
            enable_capacity_wakeup=True,
        )
        worker_dispatcher.start()

    logger.info(
        "application_starting",
        app_name=settings.app_name,
        environment=settings.app_env,
        worker_concurrency=4,
    )

    yield

    # Clean shutdown: enforce ingress quiescence first (fast-fail new probes)
    if hasattr(app.state, "readiness_coordinator") and app.state.readiness_coordinator:
        app.state.readiness_coordinator.shutdown(wait=False)

    if worker_dispatcher:
        worker_dispatcher.stop()
    if outbox_publisher:
        outbox_publisher.stop()

    if getattr(app.state, "redis", None) is not None:
        try:
            app.state.redis.close()
        except Exception:
            pass

    close_probe_redis()
    close_dedicated_telemetry_engine()

    engine.dispose()
    logger.info("application_stopping")


app = FastAPI(
    title=settings.app_name,
    description="A locally deployable asynchronous task processing platform.",
    version="0.1.0",
    debug=settings.debug,
    lifespan=lifespan,
)

# Middleware
app.add_middleware(CorrelationIdMiddleware)
app.add_middleware(RateLimitMiddleware, requests_per_minute=settings.rate_limit_per_minute)
app.add_middleware(PrometheusHttpMiddleware)

# Standardized Error Handlers
register_error_handlers(app)

# Include Routers
app.include_router(metrics_router)
app.include_router(api_v1_router)

STATIC_DIR = Path(__file__).parent / "static"
FRONTEND_DIST_DIR = Path(__file__).resolve().parents[2] / "frontend" / "dist"
FRONTEND_ASSETS_DIR = FRONTEND_DIST_DIR / "assets"
FRONTEND_CSP = (
    "default-src 'self'; "
    "script-src 'self'; "
    "style-src 'self' 'unsafe-inline'; "
    "object-src 'none'; "
    "base-uri 'self'; "
    "frame-ancestors 'none'"
)

if FRONTEND_ASSETS_DIR.is_dir():
    app.mount("/assets", StaticFiles(directory=FRONTEND_ASSETS_DIR), name="frontend-assets")


def check_database() -> bool:
    return health_check_database()


def check_redis(redis_client=None) -> bool:
    return health_check_redis(redis_client)


@app.get("/health/live", tags=["Health"])
@app.get("/healthz/live", tags=["Health"])
async def liveness():
    return {
        "status": "alive",
        "application": settings.app_name,
    }


@app.get("/health/ready", tags=["Health"])
@app.get("/healthz/ready", tags=["Health"])
async def readiness(request: Request):
    coordinator = get_readiness_coordinator(request.app)
    redis_client = getattr(request.app.state, "redis", None)

    try:
        future = coordinator.submit_probe(perform_readiness_checks, redis_client)
    except (ReadinessProbeShuttingDownError, ReadinessProbeSaturationError):
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "application": settings.app_name,
                "checks": {
                    "database": "unavailable",
                    "redis": "unavailable",
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

    try:
        asyncio_future = asyncio.wrap_future(future)
        ready, checks = await asyncio.wait_for(asyncio_future, timeout=1.0)
    except asyncio.TimeoutError:
        # 1.0s operation deadline exceeded; future slot remains occupied until completion
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "application": settings.app_name,
                "checks": {
                    "database": "unavailable",
                    "redis": "unavailable",
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )
    except Exception:
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "application": settings.app_name,
                "checks": {
                    "database": "unavailable",
                    "redis": "unavailable",
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

    content: dict[str, Any] = {
        "status": "ready" if ready else "not_ready",
        "application": settings.app_name,
        "checks": checks,
    }
    if not ready:
        content["timestamp"] = datetime.now(timezone.utc).isoformat()

    return JSONResponse(
        status_code=200 if ready else 503,
        content=content,
    )


def _resolve_ui_index() -> Path | None:
    dist_index = FRONTEND_DIST_DIR / "index.html"
    if dist_index.exists():
        return dist_index
    legacy_index = STATIC_DIR / "index.html"
    if legacy_index.exists():
        return legacy_index
    return None


def _serve_ui_response() -> FileResponse:
    index_path = _resolve_ui_index()
    if index_path is None:
        return FileResponse(STATIC_DIR / "index.html", media_type="text/html")
    return FileResponse(
        index_path,
        media_type="text/html",
        headers={"Content-Security-Policy": FRONTEND_CSP},
    )


@app.get("/", tags=["UI"])
async def serve_dashboard(request: Request):
    """Serves the TaskForge Web Console for browsers, or API JSON for non-HTML clients."""
    accept = request.headers.get("accept", "")
    if "text/html" in accept and _resolve_ui_index() is not None:
        return _serve_ui_response()

    return JSONResponse(
        content={
            "application": settings.app_name,
            "version": "0.1.0",
            "message": "TaskForge API is running.",
        }
    )


@app.get("/ui", tags=["UI"], response_class=HTMLResponse)
@app.get("/submit", tags=["UI"], response_class=HTMLResponse)
@app.get("/tasks", tags=["UI"], response_class=HTMLResponse)
@app.get("/tasks/{task_id}", tags=["UI"], response_class=HTMLResponse)
@app.get("/monitoring", tags=["UI"], response_class=HTMLResponse)
async def serve_ui_spa_routes(task_id: str | None = None):
    """Serve the built React SPA entrypoint with strict Content-Security-Policy."""
    _ = task_id
    return _serve_ui_response()
