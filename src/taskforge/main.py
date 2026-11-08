import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from taskforge.api.errors import register_error_handlers
from taskforge.api.middleware import CorrelationIdMiddleware
from taskforge.api.routes import api_v1_router
from taskforge.api.routes.metrics import router as metrics_router
from taskforge.core.config import get_settings
from taskforge.core.logging import configure_logging, get_logger
from taskforge.core.rate_limiter import RateLimitMiddleware
from taskforge.database.session import engine, init_db
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
            worker_id="taskforge-worker-main", concurrency=4, broker=broker, poll_interval=0.1
        )
        worker_dispatcher.start()

    logger.info(
        "application_starting",
        app_name=settings.app_name,
        environment=settings.app_env,
        worker_concurrency=4,
    )

    yield

    # Clean shutdown
    if worker_dispatcher:
        worker_dispatcher.stop()
    if outbox_publisher:
        outbox_publisher.stop()

    if getattr(app.state, "redis", None) is not None:
        try:
            app.state.redis.close()
        except Exception:
            pass

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

# Standardized Error Handlers
register_error_handlers(app)

# Include Routers
app.include_router(metrics_router)
app.include_router(api_v1_router)

STATIC_DIR = Path(__file__).parent / "static"


def check_database() -> bool:
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return True
    except SQLAlchemyError:
        logger.exception("database_health_check_failed")
        return False


def check_redis(redis_client) -> bool:
    if redis_client is None:
        return False
    try:
        return bool(redis_client.ping())
    except RedisError:
        logger.exception("redis_health_check_failed")
        return False


@app.get("/health/live", tags=["Health"])
async def liveness():
    return {
        "status": "alive",
        "application": settings.app_name,
    }


@app.get("/health/ready", tags=["Health"])
def readiness(request: Request):
    database_ok = check_database()
    redis_ok = check_redis(getattr(request.app.state, "redis", None))

    checks = {
        "database": "healthy" if database_ok else "unavailable",
        "redis": "healthy" if redis_ok else "unavailable",
    }

    ready = database_ok and redis_ok

    return JSONResponse(
        status_code=200 if ready else 503,
        content={
            "status": "ready" if ready else "not_ready",
            "application": settings.app_name,
            "checks": checks,
        },
    )


@app.get("/", tags=["UI"])
async def serve_dashboard(request: Request):
    """Serves the interactive TaskForge Mission Control Dashboard for browsers, or API JSON for clients."""
    accept = request.headers.get("accept", "")
    if "text/html" in accept:
        index_path = STATIC_DIR / "index.html"
        if index_path.exists():
            return FileResponse(index_path, media_type="text/html")

    return JSONResponse(
        content={
            "application": settings.app_name,
            "version": "0.1.0",
            "message": "TaskForge API is running.",
        }
    )


@app.get("/ui", tags=["UI"], response_class=HTMLResponse)
async def serve_ui_alias():
    index_path = STATIC_DIR / "index.html"
    return FileResponse(index_path, media_type="text/html")
