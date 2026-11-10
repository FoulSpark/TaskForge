import asyncio
import concurrent.futures
from datetime import datetime, timezone
import threading
from typing import Any, Callable

from redis import Redis
from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from taskforge.core.config import get_settings
from taskforge.core.logging import get_logger
from taskforge.database.session import engine

logger = get_logger(__name__)
settings = get_settings()

_probe_redis: Redis | None = None


def get_probe_redis() -> Redis:
    global _probe_redis
    if _probe_redis is None:
        _probe_redis = Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_connect_timeout=0.5,
            socket_timeout=0.5,
        )
    return _probe_redis


def close_probe_redis() -> None:
    global _probe_redis
    if _probe_redis is not None:
        try:
            _probe_redis.close()
        except Exception:
            pass
        _probe_redis = None


def check_database() -> bool:
    """
    Executes PostgreSQL check using contracted driver deadlines:
    connect_timeout=1s (in engine connect_args) and statement_timeout=500ms.
    """
    try:
        with engine.connect() as connection:
            if connection.dialect.name == "postgresql":
                connection.execute(text("SET statement_timeout = 500"))
            connection.execute(text("SELECT 1"))
        return True
    except SQLAlchemyError:
        logger.exception("database_health_check_failed")
        return False
    except Exception:
        logger.exception("database_health_check_failed")
        return False


def check_redis(redis_client: Any) -> bool:
    """
    Executes Redis check using contracted driver deadlines:
    socket_connect_timeout=0.5s and socket_timeout=0.5s.
    """
    if redis_client is None:
        return False
    try:
        if isinstance(redis_client, Redis):
            return bool(get_probe_redis().ping())
        return bool(redis_client.ping())
    except (RedisError, Exception):
        logger.exception("redis_health_check_failed")
        return False


def perform_readiness_checks(redis_client: Any = None) -> tuple[bool, dict[str, str]]:
    """
    Executes sequential downstream health checks:
    1. PostgreSQL (connect_timeout=1, statement_timeout=500ms)
    2. Redis (socket_connect_timeout=0.5s, socket_timeout=0.5s)
    """
    import taskforge.main as main_mod

    db_ok = False
    try:
        db_ok = bool(main_mod.check_database())
    except Exception:
        logger.exception("database_health_check_failed")
        db_ok = False

    redis_ok = False
    try:
        redis_ok = bool(main_mod.check_redis(redis_client))
    except Exception:
        logger.exception("redis_health_check_failed")
        redis_ok = False

    ready = db_ok and redis_ok
    checks = {
        "database": "healthy" if db_ok else "unavailable",
        "redis": "healthy" if redis_ok else "unavailable",
    }
    return ready, checks


class ReadinessProbeError(Exception):
    """Base exception for readiness probe failures."""
    pass


class ReadinessProbeSaturationError(ReadinessProbeError):
    """Raised when probe concurrency capacity (max 2 slots) is saturated."""
    pass


class ReadinessProbeShuttingDownError(ReadinessProbeError):
    """Raised when readiness probe is rejected due to application shutdown."""
    pass


class ReadinessProbeCoordinator:
    """
    Coordinates readiness probe execution with bounded concurrency (max 2 worker threads),
    future-completion-based slot release, and atomic lifecycle shutdown coordination.
    """

    def __init__(self, max_workers: int = 2):
        self._max_workers = max_workers
        self._lock = threading.RLock()
        self._inflight_count = 0
        self._is_shutting_down = False
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="readiness-probe",
        )

    @property
    def inflight_count(self) -> int:
        with self._lock:
            return self._inflight_count

    @property
    def is_shutting_down(self) -> bool:
        with self._lock:
            return self._is_shutting_down

    def submit_probe(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> concurrent.futures.Future:
        """
        Atomically checks lifecycle state, reserves an execution slot, and submits work.
        The permit slot is released strictly via future completion callback.
        """
        with self._lock:
            if self._is_shutting_down:
                raise ReadinessProbeShuttingDownError("Application shutdown has begun")
            if self._inflight_count >= self._max_workers:
                raise ReadinessProbeSaturationError("Readiness probe capacity saturated")

            self._inflight_count += 1
            try:
                future = self._executor.submit(fn, *args, **kwargs)
            except Exception:
                self._inflight_count -= 1
                raise

            future.add_done_callback(self._on_future_done)
            return future

    def _on_future_done(self, future: concurrent.futures.Future) -> None:
        with self._lock:
            self._inflight_count = max(0, self._inflight_count - 1)

    def shutdown(self, wait: bool = False) -> None:
        """
        Transitions lifecycle state to shutting down and cancels pending unstarted futures.
        Running threads drain naturally under driver-level timeouts.
        """
        with self._lock:
            self._is_shutting_down = True
        self._executor.shutdown(wait=wait, cancel_futures=True)


_default_coordinator: ReadinessProbeCoordinator | None = None


def get_readiness_coordinator(app: Any = None) -> ReadinessProbeCoordinator:
    """
    Returns the active ReadinessProbeCoordinator from app.state or a shared default.
    """
    if app is not None and hasattr(app.state, "readiness_coordinator"):
        coord = app.state.readiness_coordinator
        if coord is not None:
            return coord

    global _default_coordinator
    if _default_coordinator is None:
        _default_coordinator = ReadinessProbeCoordinator(max_workers=2)

    if app is not None:
        app.state.readiness_coordinator = _default_coordinator

    return _default_coordinator
