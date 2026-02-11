"""Shared pytest fixtures for the variance analysis agent test suite."""

from __future__ import annotations

from pathlib import Path

import pytest

from va_agent.config import Settings
from va_agent.sql.executor import SQLExecutor


@pytest.fixture(scope="session")
def test_settings(tmp_path_factory) -> Settings:
    """Settings configured for testing."""
    tmp = tmp_path_factory.mktemp("va_test")
    settings = Settings(
        db_path=tmp / "warehouse.db",
        runs_dir=tmp / "runs",
        cache_dir=tmp / "cache",
        temperature=0.0,
    )
    settings.ensure_dirs()
    return settings


@pytest.fixture(scope="session")
def test_db(test_settings: Settings) -> Path:
    """Generate the test warehouse database once per session."""
    from va_agent.data.seed_generator import seed_database

    db_path, counts, checksum = seed_database(test_settings, force=True)
    return db_path


@pytest.fixture
def executor(test_db: Path, test_settings: Settings) -> SQLExecutor:
    """Fresh SQLExecutor for each test."""
    exe = SQLExecutor(
        db_path=test_db,
        max_rows=test_settings.max_rows,
        query_timeout=test_settings.query_timeout,
    )
    yield exe
    exe.close()


@pytest.fixture
def settings(test_settings: Settings) -> Settings:
    """Alias for test_settings with function scope."""
    return test_settings
