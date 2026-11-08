from collections.abc import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from taskforge.core.config import get_settings
from taskforge.core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


class Base(DeclarativeBase):
    pass


def build_engine(database_url: str | None = None):
    url = database_url or settings.database_url
    connect_args = {}
    if url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
        return create_engine(url, connect_args=connect_args)
    else:
        connect_args["connect_timeout"] = 1
        return create_engine(
            url,
            connect_args=connect_args,
            pool_pre_ping=True,
            pool_size=getattr(settings, "db_pool_size", 10),
            max_overflow=getattr(settings, "db_max_overflow", 20),
            pool_timeout=getattr(settings, "db_pool_timeout", 30),
        )


def init_db(engine_override=None) -> None:
    """Create all tables in the database."""
    target_engine = engine_override or engine
    from taskforge.database.models import OutboxEvent, Task, TaskAttempt, TaskEvent, TaskResult  # noqa: F401

    Base.metadata.create_all(bind=target_engine)


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
