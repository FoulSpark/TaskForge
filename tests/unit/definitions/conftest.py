"""Shared SQLite fixtures for Phase 11 unit tests.

Uses a module-level SQLite engine so tests run without PostgreSQL.
FK enforcement is enabled via the connect event.
Schema teardown disables FKs to avoid drop-order conflicts.
"""

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from taskforge.database.session import Base

# ---------------------------------------------------------------------------
# Module-level in-memory engine — shared across all G11 unit tests
# ---------------------------------------------------------------------------
_definitions_engine = create_engine(
    "sqlite:///:memory:",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)


@event.listens_for(_definitions_engine, "connect")
def _enable_fk(dbapi_conn, _record):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


_DefinitionsSession = sessionmaker(
    bind=_definitions_engine,
    autoflush=False,
    expire_on_commit=False,
)


@pytest.fixture(autouse=True)
def _reset_schema():
    """Create all tables before each test, drop all after (FK-safe)."""
    Base.metadata.create_all(bind=_definitions_engine)
    yield
    with _definitions_engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys=OFF"))
        Base.metadata.drop_all(bind=conn)
        conn.execute(text("PRAGMA foreign_keys=ON"))


@pytest.fixture()
def db():
    """Yield an open SQLAlchemy session backed by the in-memory SQLite engine."""
    with _DefinitionsSession() as session:
        yield session
