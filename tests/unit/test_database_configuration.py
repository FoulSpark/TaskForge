"""Unit tests for database configuration and production fail-closed policies."""

import os
import tempfile

import pytest
from pydantic import ValidationError

import taskforge.database.session as session_module
from taskforge.core.config import Settings


def test_production_rejects_sqlite_fallback_in_settings():
    """Pydantic model validator strictly rejects allow_sqlite_fallback=True in production."""
    with pytest.raises(ValidationError, match="allow_sqlite_fallback is strictly prohibited"):
        Settings(
            app_env="production",
            allow_sqlite_fallback=True,
        )


def test_production_database_failure_fails_startup(monkeypatch):
    """
    Regardless of runtime monkeypatching, get_working_engine must fail-closed
    with RuntimeError if PostgreSQL is unavailable in production.
    """
    monkeypatch.setattr(session_module.settings, "app_env", "production")
    monkeypatch.setattr(session_module.settings, "allow_sqlite_fallback", True)

    def fail_connection(_url=None):
        raise ConnectionError("simulated_database_unavailable")

    monkeypatch.setattr(session_module, "build_engine", fail_connection)

    with pytest.raises(RuntimeError, match="Silent SQLite fallback is strictly prohibited"):
        session_module.get_working_engine()


def test_development_fallback_requires_explicit_setting(monkeypatch):
    """
    In development, fallback fails-closed when allow_sqlite_fallback=False,
    and succeeds only when allow_sqlite_fallback=True.
    """
    monkeypatch.setattr(session_module.settings, "app_env", "development")

    # Case 1: allow_sqlite_fallback = False -> Must raise RuntimeError
    monkeypatch.setattr(session_module.settings, "allow_sqlite_fallback", False)

    def fail_connection(_url=None):
        raise ConnectionError("simulated_database_unavailable")

    monkeypatch.setattr(session_module, "build_engine", fail_connection)

    with pytest.raises(RuntimeError, match="Silent SQLite fallback is strictly prohibited"):
        session_module.get_working_engine()

    # Case 2: allow_sqlite_fallback = True -> Returns SQLite engine
    monkeypatch.setattr(session_module.settings, "allow_sqlite_fallback", True)
    temp_db = os.path.join(tempfile.gettempdir(), f"taskforge_dev_test_{os.getpid()}.db")

    def fallback_builder(url=None):
        if url and "sqlite" in url:
            from sqlalchemy import create_engine

            return create_engine(f"sqlite:///{temp_db}", connect_args={"check_same_thread": False})
        raise ConnectionError("simulated_database_unavailable")

    monkeypatch.setattr(session_module, "build_engine", fallback_builder)
    eng = session_module.get_working_engine()
    assert str(eng.url).startswith("sqlite")
    eng.dispose()
    if os.path.exists(temp_db):
        try:
            os.remove(temp_db)
        except Exception:
            pass
