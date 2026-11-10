from dataclasses import dataclass
from datetime import datetime, timezone
import threading
import time
from typing import Any, Callable

from redis import Redis
from sqlalchemy import create_engine, text

from taskforge.core.config import get_settings
from taskforge.core.logging import get_logger
from taskforge.monitoring.metrics import (
    ACTIVE_WORKERS,
    DB_POOL_CONNECTIONS,
    OUTBOX_LAG_SECONDS,
    OUTBOX_PENDING_EVENTS,
    QUEUE_DEPTH,
    TELEMETRY_SCRAPE_ERRORS_TOTAL,
    record_db_pool_active_connections,
    record_db_pool_overflow_connections,
)

logger = get_logger(__name__)
settings = get_settings()


@dataclass(frozen=True)
class InfrastructureSnapshot:
    queue_depth: dict[str, int]
    outbox_pending_events: int
    outbox_lag_seconds: float
    active_workers: int
    collected_at: float


class GaugeSnapshotCache:
    """
    Manages cached infrastructure gauge snapshots with 5.0-second TTL,
    single-flight refresh execution, failed refresh preservation,
    and cold-start omission.
    """

    def __init__(
        self,
        ttl_seconds: float = 5.0,
        telemetry_engine=None,
        redis_client_factory: Callable[[], Any] | None = None,
    ):
        self.ttl_seconds = ttl_seconds
        self._custom_telemetry_engine = telemetry_engine
        self._redis_client_factory = redis_client_factory
        self._refresh_lock = threading.Lock()
        self._snapshot: InfrastructureSnapshot | None = None
        self._last_refresh_time: float = 0.0
        self._refresh_operation_count: int = 0

    @property
    def snapshot(self) -> InfrastructureSnapshot | None:
        return self._snapshot

    @property
    def refresh_operation_count(self) -> int:
        return self._refresh_operation_count

    def get_telemetry_engine(self):
        if self._custom_telemetry_engine is not None:
            return self._custom_telemetry_engine
        return get_dedicated_telemetry_engine()

    def get_redis_client(self):
        if self._redis_client_factory is not None:
            return self._redis_client_factory()
        from taskforge.monitoring.health import get_probe_redis
        return get_probe_redis()

    def update_db_pool_connections(self) -> None:
        """
        Reads application SQLAlchemy pool statistics directly in memory
        WITHOUT borrowing or checking out connections from the pool.
        """
        try:
            from taskforge.database.session import engine as app_engine
            pool = app_engine.pool
            checked_out = pool.checkedout() if hasattr(pool, "checkedout") else 0
            idle = pool.checkedin() if hasattr(pool, "checkedin") else 0
            overflow = pool.overflow() if hasattr(pool, "overflow") else 0

            DB_POOL_CONNECTIONS.labels(pool_state="checked_out").set(checked_out)
            DB_POOL_CONNECTIONS.labels(pool_state="idle").set(idle)
            DB_POOL_CONNECTIONS.labels(pool_state="overflow").set(overflow)

            record_db_pool_active_connections(checked_out, pool_name="primary")
            record_db_pool_overflow_connections(max(0, overflow), pool_name="primary")
        except Exception as exc:
            logger.debug("Failed to read application DB pool stats: %s", exc)

    def _execute_refresh_operation(self) -> InfrastructureSnapshot:
        """
        Executes the backend queries across PostgreSQL and Redis.
        Constitutes exactly ONE refresh operation.
        """
        # 1. PostgreSQL Outbox queries on dedicated telemetry engine
        pending_count, lag_seconds = self._fetch_outbox_telemetry()

        # 2. Redis Queue Depth and Active Worker queries
        redis_client = self.get_redis_client()
        queue_depths = self._fetch_queue_depths(redis_client)
        active_workers = self._fetch_active_workers(redis_client)

        return InfrastructureSnapshot(
            queue_depth=queue_depths,
            outbox_pending_events=pending_count,
            outbox_lag_seconds=lag_seconds,
            active_workers=active_workers,
            collected_at=time.time(),
        )

    def _fetch_outbox_telemetry(self) -> tuple[int, float]:
        engine = self.get_telemetry_engine()
        with engine.connect() as conn:
            if conn.dialect.name == "postgresql":
                stmt = text("""
                    SELECT
                        count(*),
                        COALESCE(EXTRACT(EPOCH FROM (now() - MIN(created_at))), 0.0)
                    FROM outbox_events
                    WHERE status = 'PENDING'
                """)
                row = conn.execute(stmt).one()
                pending_count = int(row[0])
                lag_seconds = float(row[1]) if pending_count > 0 else 0.0
                return pending_count, lag_seconds
            else:
                stmt = text("SELECT count(*), MIN(created_at) FROM outbox_events WHERE status = 'PENDING'")
                row = conn.execute(stmt).one()
                pending_count = int(row[0])
                min_created = row[1]
                lag_seconds = 0.0
                if pending_count > 0 and min_created:
                    if isinstance(min_created, str):
                        min_dt = datetime.fromisoformat(min_created)
                    else:
                        min_dt = min_created
                    if min_dt.tzinfo is None:
                        min_dt = min_dt.replace(tzinfo=timezone.utc)
                    lag_seconds = max(0.0, (datetime.now(timezone.utc) - min_dt).total_seconds())
                return pending_count, lag_seconds

    def _fetch_queue_depths(self, redis_client: Any) -> dict[str, int]:
        if redis_client is None:
            raise RuntimeError("Redis client is not available")
        queue_key = "taskforge:queue:tasks"
        pipe = redis_client.pipeline()
        pipe.zcount(queue_key, 3000000000001, 3999999999999)  # critical: [3*10^12 + 1, 4*10^12 - 1]
        pipe.zcount(queue_key, 2000000000001, 2999999999999)  # high:     [2*10^12 + 1, 3*10^12 - 1]
        pipe.zcount(queue_key, 1000000000001, 1999999999999)  # medium:   [10^12 + 1, 2*10^12 - 1]
        pipe.zcount(queue_key, 1, 999999999999)              # low:      [1, 10^12 - 1]
        results = pipe.execute()
        return {
            "critical": int(results[0]),
            "high": int(results[1]),
            "medium": int(results[2]),
            "low": int(results[3]),
        }

    def _fetch_active_workers(self, redis_client: Any) -> int:
        if redis_client is None:
            raise RuntimeError("Redis client is not available")
        workers_key = "taskforge:workers:active"
        cutoff = time.time() - 15.0
        pipe = redis_client.pipeline()
        pipe.zremrangebyscore(workers_key, "-inf", cutoff)
        pipe.zcard(workers_key)
        _, count = pipe.execute()
        return int(count)

    def _apply_snapshot(self, snapshot: InfrastructureSnapshot) -> None:
        for prio, depth in snapshot.queue_depth.items():
            QUEUE_DEPTH.labels(priority=prio).set(depth)
        OUTBOX_PENDING_EVENTS.set(snapshot.outbox_pending_events)
        OUTBOX_LAG_SECONDS.set(snapshot.outbox_lag_seconds)
        ACTIVE_WORKERS.set(snapshot.active_workers)

    def collect_and_update(self) -> bool:
        """
        Coordinates single-flight refresh with 5.0s TTL.
        Returns:
            True if a valid snapshot is available and applied.
            False if cold-start failure (no valid snapshot exists).
        """
        now = time.monotonic()

        # Update in-memory DB pool connections
        self.update_db_pool_connections()

        # Warm cache check
        if self._snapshot is not None and (now - self._last_refresh_time) < self.ttl_seconds:
            self._apply_snapshot(self._snapshot)
            return True

        # Cache expired (or cold-start): attempt single-flight lock
        acquired = self._refresh_lock.acquire(blocking=False)
        if acquired:
            try:
                # Double-check inside lock
                now = time.monotonic()
                if self._snapshot is not None and (now - self._last_refresh_time) < self.ttl_seconds:
                    self._apply_snapshot(self._snapshot)
                    return True

                self._refresh_operation_count += 1
                new_snapshot = self._execute_refresh_operation()
                self._snapshot = new_snapshot
                self._last_refresh_time = time.monotonic()
                self._apply_snapshot(new_snapshot)
                return True
            except Exception as exc:
                logger.warning("telemetry_refresh_failed", error=str(exc))
                # Increment error counter exactly once per failed refresh attempt
                TELEMETRY_SCRAPE_ERRORS_TOTAL.inc()
                self._last_refresh_time = time.monotonic()

                if self._snapshot is not None:
                    # Preserve last successful snapshot
                    self._apply_snapshot(self._snapshot)
                    return True
                else:
                    # Cold-start failure: do NOT fabricate zeros!
                    return False
            finally:
                self._refresh_lock.release()
        else:
            # Another thread is actively executing the refresh operation
            if self._snapshot is not None:
                self._apply_snapshot(self._snapshot)
                return True
            else:
                return False


_dedicated_telemetry_engine = None


def get_dedicated_telemetry_engine():
    """Returns dedicated DB engine with pool_size=1, max_overflow=0 for telemetry scrapes."""
    global _dedicated_telemetry_engine
    if _dedicated_telemetry_engine is None:
        url = settings.database_url
        connect_args = {}
        if not url.startswith("sqlite"):
            connect_args["connect_timeout"] = 1
        _dedicated_telemetry_engine = create_engine(
            url,
            connect_args=connect_args,
            pool_size=1,
            max_overflow=0,
            pool_pre_ping=True,
            pool_timeout=2.0,
        )
    return _dedicated_telemetry_engine


def close_dedicated_telemetry_engine() -> None:
    global _dedicated_telemetry_engine
    if _dedicated_telemetry_engine is not None:
        try:
            _dedicated_telemetry_engine.dispose()
        except Exception:
            pass
        _dedicated_telemetry_engine = None


_default_gauge_cache: GaugeSnapshotCache | None = None


def get_gauge_snapshot_cache() -> GaugeSnapshotCache:
    global _default_gauge_cache
    if _default_gauge_cache is None:
        _default_gauge_cache = GaugeSnapshotCache()
    return _default_gauge_cache


def reset_gauge_snapshot_cache() -> None:
    """Helper for test isolation to reset the default cache."""
    global _default_gauge_cache
    _default_gauge_cache = None
