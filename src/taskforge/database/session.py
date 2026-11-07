
from collections.abc import Generator
import os

from sqlalchemy import create_engine, text
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
            pool_size=settings.db_pool_size if hasattr(settings, "db_pool_size") else 5,
            max_overflow=settings.db_max_overflow if hasattr(settings, "db_max_overflow") else 10,
            pool_timeout=5,
        )


def init_db(engine_override=None) -> None:
    """Create all tables in the database."""
    target_engine = engine_override or engine
    from taskforge.database.models import Task, TaskAttempt, TaskResult, TaskEvent, OutboxEvent  # noqa: F401
    Base.metadata.create_all(bind=target_engine)


def get_working_engine():
    """Returns configured engine, falling back to SQLite if PostgreSQL is unreachable."""
    primary_url = settings.database_url
    try:
        eng = build_engine(primary_url)
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        return eng
    except Exception as exc:
        logger.warning(
            "primary_database_unreachable_falling_back_to_sqlite",
            error=str(exc),
            url=primary_url,
        )
        fallback_url = "sqlite:///./taskforge_dev.db"
        eng = build_engine(fallback_url)
        init_db(eng)
        return eng


engine = get_working_engine()
init_db(engine)

SessionLocal = sessionmaker(
    bind=engine,
    class_=Session,
    autoflush=False,
    expire_on_commit=False,
)


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
