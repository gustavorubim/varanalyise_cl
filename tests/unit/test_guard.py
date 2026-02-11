"""Unit tests for SQL guard (Layer 4 defense)."""

import pytest

from va_agent.sql.guard import SQLGuardError, validate_query


class TestValidSelectQueries:
    """Queries that should PASS validation."""

    def test_simple_select(self):
        result = validate_query("SELECT 1")
        assert "SELECT" in result

    def test_select_from_table(self):
        result = validate_query("SELECT * FROM mart_pnl_report")
        assert "mart_pnl_report" in result

    def test_select_with_where(self):
        result = validate_query(
            "SELECT department, actual_usd FROM mart_pnl_report WHERE period = '2024-01'"
        )
        assert "department" in result

    def test_select_with_join(self):
        result = validate_query(
            "SELECT a.*, m.account_type FROM fct_actuals_monthly a "
            "JOIN stg_account_mapping m ON a.account_code = m.account_code"
        )
        assert "JOIN" in result

    def test_select_with_subquery(self):
        result = validate_query(
            "SELECT * FROM mart_pnl_report WHERE department IN "
            "(SELECT department FROM stg_cost_center_mapping)"
        )
        assert "IN" in result

    def test_select_with_cte(self):
        result = validate_query(
            "WITH cte AS (SELECT department, SUM(actual_usd) as total "
            "FROM mart_pnl_report GROUP BY department) "
            "SELECT * FROM cte WHERE total > 1000"
        )
        assert "WITH" in result

    def test_select_with_window_function(self):
        result = validate_query(
            "SELECT period, amount_usd, "
            "LAG(amount_usd) OVER (ORDER BY period) as prev "
            "FROM int_actuals_usd"
        )
        assert "LAG" in result

    def test_select_with_aggregates(self):
        result = validate_query(
            "SELECT department, COUNT(*), SUM(variance_usd), AVG(variance_pct) "
            "FROM mart_pnl_report GROUP BY department"
        )
        assert "COUNT" in result

    def test_select_with_trailing_semicolon(self):
        result = validate_query("SELECT 1;")
        assert "SELECT" in result

    def test_select_with_replace_function(self):
        """REPLACE() as a function should be allowed."""
        result = validate_query(
            "SELECT REPLACE(department, 'Sales', 'Revenue') FROM mart_pnl_report"
        )
        assert "REPLACE" in result

    def test_select_with_case(self):
        result = validate_query(
            "SELECT CASE WHEN variance_pct > 10 THEN 'HIGH' ELSE 'LOW' END "
            "FROM mart_pnl_report"
        )
        assert "CASE" in result


class TestBlockedQueries:
    """Queries that should FAIL validation."""

    def test_empty_query(self):
        with pytest.raises(SQLGuardError, match="Empty"):
            validate_query("")

    def test_whitespace_only(self):
        with pytest.raises(SQLGuardError, match="Empty"):
            validate_query("   ")

    def test_insert(self):
        with pytest.raises(SQLGuardError):
            validate_query("INSERT INTO mart_pnl_report VALUES (1,2,3)")

    def test_update(self):
        with pytest.raises(SQLGuardError):
            validate_query("UPDATE mart_pnl_report SET actual_usd = 0")

    def test_delete(self):
        with pytest.raises(SQLGuardError):
            validate_query("DELETE FROM mart_pnl_report")

    def test_drop_table(self):
        with pytest.raises(SQLGuardError):
            validate_query("DROP TABLE mart_pnl_report")

    def test_create_table(self):
        with pytest.raises(SQLGuardError):
            validate_query("CREATE TABLE evil (id INT)")

    def test_alter_table(self):
        with pytest.raises(SQLGuardError):
            validate_query("ALTER TABLE mart_pnl_report ADD COLUMN evil TEXT")

    def test_attach_database(self):
        with pytest.raises(SQLGuardError):
            validate_query("ATTACH DATABASE '/tmp/evil.db' AS evil")

    def test_pragma(self):
        with pytest.raises(SQLGuardError):
            validate_query("PRAGMA table_info(mart_pnl_report)")

    def test_load_extension(self):
        with pytest.raises(SQLGuardError):
            validate_query("SELECT LOAD_EXTENSION('/tmp/evil.so')")

    def test_multi_statement_injection(self):
        with pytest.raises(SQLGuardError):
            validate_query("SELECT 1; DROP TABLE mart_pnl_report")

    def test_select_into(self):
        with pytest.raises(SQLGuardError):
            validate_query("SELECT * INTO evil FROM mart_pnl_report")

    def test_vacuum(self):
        with pytest.raises(SQLGuardError):
            validate_query("VACUUM")

    def test_grant(self):
        with pytest.raises(SQLGuardError):
            validate_query("GRANT ALL ON mart_pnl_report TO evil")

    def test_begin_transaction(self):
        with pytest.raises(SQLGuardError):
            validate_query("BEGIN TRANSACTION")
