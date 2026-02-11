"""Integration tests for agent tools against a real database."""

import pytest

from va_agent.sql.executor import SQLExecutor
from va_agent.tools import sql_tools
from va_agent.tools.lineage_tools import get_all_tables, get_table_lineage
from va_agent.tools.report_tools import (
    get_findings,
    reset_state,
    write_finding,
    write_report_section,
)


@pytest.fixture(autouse=True)
def wire_executor(executor):
    """Wire the executor for sql_tools before each test."""
    sql_tools.set_executor(executor)
    yield
    reset_state()


class TestSQLTools:
    def test_run_sql_query(self):
        result = sql_tools.run_sql_query("SELECT COUNT(*) as cnt FROM mart_pnl_report")
        assert result["error"] is None
        assert result["rows"][0]["cnt"] > 0

    def test_run_sql_query_blocked(self):
        result = sql_tools.run_sql_query("DROP TABLE mart_pnl_report")
        assert result["error"] is not None

    def test_run_sql_template_variance_summary(self):
        result = sql_tools.run_sql_template("variance_summary")
        assert result["error"] is None
        assert result["row_count"] > 0

    def test_run_sql_template_unknown(self):
        result = sql_tools.run_sql_template("nonexistent")
        assert result["error"] is not None

    def test_run_sql_query_reports_true_cardinality(self, executor):
        result = sql_tools.run_sql_query("SELECT * FROM raw_ledger_entries")
        expected = executor.execute("SELECT COUNT(*) as cnt FROM raw_ledger_entries")

        assert result["error"] is None
        assert result["truncated"] is True
        assert result["row_count"] == 20
        assert result["total_available_exact"] is True
        assert result["total_available"] == expected.rows[0]["cnt"]

    def test_get_table_schema(self):
        result = sql_tools.get_table_schema("mart_pnl_report")
        assert result["table_name"] == "mart_pnl_report"
        assert result["row_count"] > 0
        assert len(result["sample_rows"]) > 0


class TestLineageTools:
    def test_get_all_tables(self):
        result = get_all_tables()
        assert result["total"] == 8
        names = [t["name"] for t in result["tables"]]
        assert "mart_pnl_report" in names

    def test_get_table_lineage(self):
        result = get_table_lineage("mart_pnl_report")
        assert result["table_name"] == "mart_pnl_report"
        assert "int_actuals_usd" in result["direct_upstream"]
        assert "raw_ledger_entries" in result["full_upstream_chain"]

    def test_lineage_unknown_table(self):
        result = get_table_lineage("nonexistent")
        assert "error" in result


class TestReportTools:
    def test_write_finding(self):
        result = write_finding(
            title="Test Finding",
            category="COGS_ANOMALY",
            direction="UNFAVORABLE",
            variance_amount=10000,
            variance_pct=25.0,
            root_cause="Test cause",
            evidence=["Evidence 1"],
            affected_tables=["mart_pnl_report"],
            confidence_evidence_breadth=0.8,
            confidence_lineage_depth=0.7,
        )
        assert result["finding_id"] == "F-001"
        assert result["confidence_score"] > 0

        findings = get_findings()
        assert len(findings) == 1
        assert findings[0].id == "F-001"

    def test_write_report_section(self):
        result = write_report_section(
            title="Executive Summary",
            content="This is a test summary.",
            finding_ids=["F-001"],
        )
        assert result["section_number"] == 1

    def test_write_finding_invalid_category(self):
        """Invalid category should fall back to OTHER."""
        result = write_finding(
            title="Test",
            category="INVALID",
            direction="FAVORABLE",
            variance_amount=100,
            variance_pct=1.0,
            root_cause="Test",
            evidence=["E1"],
            affected_tables=["t1"],
        )
        assert result["finding_id"] is not None
        findings = get_findings()
        # The last finding should have category OTHER
        assert findings[-1].category.value == "OTHER"
