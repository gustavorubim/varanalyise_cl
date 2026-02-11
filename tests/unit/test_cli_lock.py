"""Unit tests for analyze lock helpers in CLI."""

from pathlib import Path

import pytest

from va_agent.cli import _acquire_analysis_lock, _release_analysis_lock


class TestAnalyzeLock:
    def test_acquire_and_release(self, tmp_path: Path):
        lock_path = tmp_path / ".analysis.lock"

        _acquire_analysis_lock(lock_path)
        assert lock_path.exists()

        _release_analysis_lock(lock_path)
        assert not lock_path.exists()

    def test_second_acquire_fails(self, tmp_path: Path):
        lock_path = tmp_path / ".analysis.lock"
        _acquire_analysis_lock(lock_path)

        try:
            with pytest.raises(RuntimeError):
                _acquire_analysis_lock(lock_path)
        finally:
            _release_analysis_lock(lock_path)
