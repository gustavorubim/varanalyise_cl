"""Integration tests for the guard → executor → connection pipeline."""

import pytest

from va_agent.sql.executor import SQLExecutor
from va_agent.sql.guard import SQLGuardError


class TestFullSQLPipeline:
    """End-to-end tests through all 4 layers."""

    def test_valid_query_through_full_stack(self, executor):
        result = executor.execute(
            "SELECT department, SUM(variance_usd) as total_var "
            "FROM mart_pnl_report GROUP BY department"
        )
        assert result.error is None
        assert result.row_count > 0

    def test_cte_through_full_stack(self, executor):
        result = executor.execute(
            "WITH dept_totals AS ("
            "  SELECT department, SUM(actual_usd) as total "
            "  FROM mart_pnl_report GROUP BY department"
            ") SELECT * FROM dept_totals ORDER BY total DESC"
        )
        assert result.error is None
        assert result.row_count > 0

    def test_window_function_through_full_stack(self, executor):
        result = executor.execute(
            "SELECT period, rate_to_usd, "
            "LAG(rate_to_usd) OVER (ORDER BY period) as prev "
            "FROM fct_fx_rates WHERE currency = 'EUR'"
        )
        assert result.error is None
        assert result.row_count > 0

    def test_insert_blocked_at_guard_level(self, executor):
        result = executor.execute("INSERT INTO mart_pnl_report VALUES (1,2,3)")
        assert result.error is not None

    def test_drop_blocked_at_guard_level(self, executor):
        result = executor.execute("DROP TABLE mart_pnl_report")
        assert result.error is not None

    def test_multi_statement_blocked(self, executor):
        result = executor.execute("SELECT 1; SELECT 2")
        assert result.error is not None

    def test_attach_blocked(self, executor):
        result = executor.execute("ATTACH DATABASE ':memory:' AS evil")
        assert result.error is not None

    def test_audit_trail_complete(self, executor):
        executor.execute("SELECT 1")
        executor.execute("INSERT INTO x VALUES (1)")  # will fail
        executor.execute("SELECT 2")

        log = executor.get_audit_entries()
        assert len(log) == 3
        assert log[0]["error"] is None
        assert log[1]["error"] is not None
        assert log[2]["error"] is None

    def test_join_across_tables(self, executor):
        result = executor.execute(
            "SELECT r.entry_id, m.account_type "
            "FROM raw_ledger_entries r "
            "JOIN stg_account_mapping m ON r.account_code = m.account_code "
            "LIMIT 10"
        )
        assert result.error is None
        assert result.row_count == 10
