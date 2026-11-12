import os

import pytest

# Ensure test mode flag is active for the test suite
os.environ["TASKFORGE_TESTING"] = "1"
os.environ["APP_ENV"] = "test"
os.environ["DATABASE_URL"] = (
    "postgresql+psycopg://taskforge:taskforge_dev@localhost:5432/taskforge_test"
)

from taskforge.database.session import SessionLocal, engine


@pytest.fixture(autouse=True)
def verify_test_database_isolation():
    """CRITICAL SAFEGUARD: Prevent accidental test execution against development or production DB."""
    url_str = str(engine.url)
    assert "taskforge_test" in url_str, (
        f"CRITICAL SAFEGUARD: Tests running against non-test DB: {url_str}"
    )


@pytest.fixture(scope="session", autouse=True)
def purge_stale_outbox_on_session_start():
    """Ensure uncleaned PENDING outbox events from historical aborted test runs do not leak into audit tests."""
    from sqlalchemy import text

    try:
        with SessionLocal() as db:
            db.execute(text("DELETE FROM outbox_events WHERE status = 'PENDING'"))
            db.commit()
    except Exception:
        pass


@pytest.fixture
def db_session():
    """Provides a database session for tests, closing it upon test teardown."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
