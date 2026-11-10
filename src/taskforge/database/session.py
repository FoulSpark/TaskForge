from collections.abc import Generator
import time

from sqlalchemy import create_engine, event, text, exc as sa_exc
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy.pool import QueuePool

from taskforge.core.config import get_settings
from taskforge.core.logging import get_logger
from taskforge.monitoring.metrics import (
    DB_POOL_CONNECTIONS,
    record_db_pool_active_connections,
    record_db_pool_checkout_duration,
    record_db_pool_checkout_timeout,
    record_db_pool_overflow_connections,
)

logger = get_logger(__name__)
settings = get_settings()


class Base(DeclarativeBase):
    pass


class InstrumentedQueuePool(QueuePool):
    """SQLAlchemy QueuePool subclass instrumented with G9.4 connection acquisition telemetry.

    Measures checkout wait latency and increments timeout counter on pool exhaustion
    while preserving strict failure isolation and identical underlying QueuePool semantics.
    """

    def __init__(self, *args, **kwargs):
        self._pool_name = kwargs.pop("pool_name", "primary")
        super().__init__(*args, **kwargs)

        @event.listens_for(self, "checkin")
        def _on_checkin(dbapi_conn, conn_record):
            try:
                # Inside checkin event, the returning connection is still counted in checkedout()
                post_checkedout = max(0, self.checkedout() - 1)
                active_overflow = max(0, self.overflow())
                record_db_pool_active_connections(post_checkedout, pool_name=self._pool_name)
                record_db_pool_overflow_connections(active_overflow, pool_name=self._pool_name)
                if self._pool_name == "primary":
                    DB_POOL_CONNECTIONS.labels(pool_state="checked_out").set(post_checkedout)
                    DB_POOL_CONNECTIONS.labels(pool_state="overflow").set(active_overflow)
            except Exception as exc:
                logger.debug("Telemetry isolation: failed to record checkin pool telemetry: %s", exc)

    def connect(self):
        t0 = time.perf_counter()
        try:
            conn = super().connect()
            duration = time.perf_counter() - t0
            record_db_pool_checkout_duration(duration, pool_name=self._pool_name)
            checked_out = self.checkedout()
            active_overflow = max(0, self.overflow())
            record_db_pool_active_connections(checked_out, pool_name=self._pool_name)
            record_db_pool_overflow_connections(active_overflow, pool_name=self._pool_name)
            if self._pool_name == "primary":
                DB_POOL_CONNECTIONS.labels(pool_state="checked_out").set(checked_out)
                DB_POOL_CONNECTIONS.labels(pool_state="overflow").set(active_overflow)
            return conn
        except sa_exc.TimeoutError:
            duration = time.perf_counter() - t0
            record_db_pool_checkout_timeout(pool_name=self._pool_name)
            checked_out = self.checkedout()
            active_overflow = max(0, self.overflow())
            record_db_pool_active_connections(checked_out, pool_name=self._pool_name)
            record_db_pool_overflow_connections(active_overflow, pool_name=self._pool_name)
            if self._pool_name == "primary":
                DB_POOL_CONNECTIONS.labels(pool_state="checked_out").set(checked_out)
                DB_POOL_CONNECTIONS.labels(pool_state="overflow").set(active_overflow)
            raise


def build_engine(
    database_url: str | None = None,
    pool_size: int | None = None,
    max_overflow: int | None = None,
    pool_timeout: int | float | None = None,
    pool_name: str = "primary",
    poolclass=None,
):
    url = database_url or settings.database_url
    connect_args = {}
    if url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
        return create_engine(url, connect_args=connect_args)
    else:
        connect_args["connect_timeout"] = 1
        effective_poolclass = poolclass or InstrumentedQueuePool
        effective_pool_size = pool_size if pool_size is not None else getattr(settings, "db_pool_size", 10)
        effective_max_overflow = max_overflow if max_overflow is not None else getattr(settings, "db_max_overflow", 20)
        effective_pool_timeout = pool_timeout if pool_timeout is not None else getattr(settings, "db_pool_timeout", 30)

        eng = create_engine(
            url,
            connect_args=connect_args,
            poolclass=effective_poolclass,
            pool_pre_ping=True,
            pool_size=effective_pool_size,
            max_overflow=effective_max_overflow,
            pool_timeout=effective_pool_timeout,
        )
        if hasattr(eng.pool, "_pool_name"):
            eng.pool._pool_name = pool_name
        return eng



def init_db(engine_override=None) -> None:
    """Create all tables in the database."""
    target_engine = engine_override or engine
    from taskforge.database.models import OutboxEvent, Task, TaskAttempt, TaskEvent, TaskResult  # noqa: F401

    Base.metadata.create_all(bind=target_engine)
    try:
        with target_engine.connect() as conn:
            conn.execute(
                text(
                    "ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS available_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_outbox_status_available ON outbox_events (status, available_at);"
                )
            )
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_task_attempts_task_attempt ON task_attempts (task_id, attempt_number);"
                )
            )
            conn.commit()
    except Exception:
        pass


def sanitize_db_url(url: str) -> str:
    """Mask credentials in database URL for safe logging."""
    try:
        return make_url(url).render_as_string(hide_password=True)
    except Exception:
        return "<sanitized-db-url>"


def get_working_engine():
    """Returns configured engine, falling back to SQLite only in non-production environments when allowed."""
    primary_url = settings.database_url
    safe_url = sanitize_db_url(primary_url)
    try:
        eng = build_engine(primary_url)
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        return eng
    except Exception as exc:
        is_production = settings.app_env.lower() in ("production", "prod")
        allow_fallback = getattr(settings, "allow_sqlite_fallback", False)

        if is_production or not allow_fallback:
            logger.critical(
                "primary_database_connection_failed_aborting",
                error=str(exc),
                url=safe_url,
                environment=settings.app_env,
            )
            raise RuntimeError(
                f"FATAL: Unable to connect to primary database '{safe_url}' in '{settings.app_env}' environment. "
                "Silent SQLite fallback is strictly prohibited."
            ) from exc

        logger.warning(
            "primary_database_unreachable_falling_back_to_sqlite",
            error=str(exc),
            url=safe_url,
            environment=settings.app_env,
        )
        fallback_url = "sqlite:///./taskforge_dev.db"
        eng = build_engine(fallback_url)
        init_db(eng)
        return eng


engine = get_working_engine()

SessionLocal = sessionmaker(
    bind=engine,
    class_=Session,
    autoflush=False,
    expire_on_commit=False,
)


def set_engine(new_engine) -> None:
    """Reconfigures global engine and SessionLocal binding (e.g. for test isolation)."""
    global engine, SessionLocal
    engine = new_engine
    SessionLocal.configure(bind=engine)


def set_session_factory(factory) -> None:
    """Allows test fixtures to provide a custom SessionLocal factory."""
    global SessionLocal
    SessionLocal = factory


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
