"""Unit tests for the SQL executor."""

import pytest

from va_agent.sql.executor import SQLExecutor


class TestSQLExecutor:
    def test_valid_query(self, executor):
        result = executor.execute("SELECT COUNT(*) as cnt FROM mart_pnl_report")
        assert result.error is None
        assert result.row_count == 1
        assert result.rows[0]["cnt"] > 0

    def test_returns_columns(self, executor):
        result = executor.execute("SELECT department, period FROM mart_pnl_report LIMIT 1")
        assert "department" in result.columns
        assert "period" in result.columns

    def test_max_rows_truncation(self, test_db):
        exe = SQLExecutor(test_db, max_rows=5)
        result = exe.execute("SELECT * FROM raw_ledger_entries")
        assert result.row_count == 5
        assert result.truncated is True
        exe.close()

    def test_guard_blocks_insert(self, executor):
        result = executor.execute("INSERT INTO mart_pnl_report VALUES (1,2,3)")
        assert result.error is not None
        assert "not allowed" in result.error.lower() or "INSERT" in result.error

    def test_guard_blocks_multi_statement(self, executor):
        result = executor.execute("SELECT 1; DROP TABLE x")
        assert result.error is not None

    def test_audit_log_populated(self, executor):
        executor.execute("SELECT 1")
        executor.execute("SELECT 2")
        entries = executor.get_audit_entries()
        assert len(entries) == 2

    def test_audit_log_records_errors(self, executor):
        executor.execute("INSERT INTO x VALUES (1)")
        entries = executor.get_audit_entries()
        assert entries[-1]["error"] is not None

    def test_get_table_names(self, executor):
        names = executor.get_table_names()
        assert "mart_pnl_report" in names
        assert "raw_ledger_entries" in names
        assert "seed_manifest" in names

    def test_execution_time_recorded(self, executor):
        result = executor.execute("SELECT COUNT(*) FROM mart_pnl_report")
        assert result.execution_time_ms > 0

    def test_invalid_table_returns_error(self, executor):
        result = executor.execute("SELECT * FROM nonexistent_table")
        assert result.error is not None
