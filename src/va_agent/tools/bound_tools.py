"""Factory that creates agent tool functions bound to a RunContext.

Each tool closure captures the RunContext, eliminating the need for
module-level mutable state and enabling thread-safe concurrent runs.
"""

from __future__ import annotations

from typing import Any

from va_agent.data.lineage_registry import LINEAGE
from va_agent.models import (
    ConfidenceFactors,
    ConfidenceLevel,
    ConfidenceScore,
    Finding,
    FindingCategory,
    ReportSection,
    VarianceDirection,
)
from va_agent.sql.templates import TEMPLATES
from va_agent.tools.run_context import RunContext


def create_bound_tools(ctx: RunContext) -> dict[str, Any]:
    """Return a dict of tool-name -> callable, each bound to *ctx*.

    The returned callables have the same signatures and docstrings as
    the module-level functions in sql_tools.py / report_tools.py /
    lineage_tools.py so that LangChain infers identical tool schemas.
    """

    # -- SQL tools ----------------------------------------------------------

    def run_sql_query(sql: str) -> dict[str, Any]:
        """Execute a read-only SQL SELECT query against the warehouse database.

        Use this tool to run any valid SELECT query. The query is validated
        for safety (only SELECTs allowed, no DDL/DML). Results are limited
        to 500 rows.

        IMPORTANT GUIDELINES:
        - Only SELECT statements are allowed (and WITH/CTE)
        - No INSERT, UPDATE, DELETE, DROP, CREATE, ATTACH, PRAGMA
        - Use JOINs freely — all tables can be joined
        - Use window functions (LAG, LEAD, etc.) for period comparisons
        - Use aggregates (SUM, AVG, COUNT) for summarization
        - Always include relevant dimension columns for context

        Args:
            sql: A valid SQL SELECT query string.

        Returns:
            Dict with keys: columns, rows, row_count, total_available,
            total_available_exact, truncated, error.
            If error is not None, the query failed.
        """
        exe = ctx.executor
        result = exe.execute(sql)
        if result.error:
            return {
                "columns": result.columns,
                "rows": result.rows,
                "row_count": len(result.rows),
                "total_available": len(result.rows),
                "total_available_exact": True,
                "truncated": result.truncated,
                "error": result.error,
            }

        rows = result.rows[:20] if len(result.rows) > 20 else result.rows
        total_available = result.row_count
        total_available_exact = True

        if result.truncated:
            counted_rows, count_error = exe.get_total_row_count(result.sql)
            if count_error is None and counted_rows is not None:
                total_available = counted_rows
            else:
                total_available_exact = False

        return {
            "columns": result.columns,
            "rows": rows,
            "row_count": len(rows),
            "truncated": result.truncated or len(result.rows) > 20,
            "total_available": total_available,
            "total_available_exact": total_available_exact,
            "error": result.error,
        }

    def run_sql_template(
        template_name: str,
        params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Execute a pre-built SQL template for common analysis patterns.

        Available templates and their parameters:
        - variance_summary(period?): P&L variance by department/account_type
        - account_detail(account_code, period?): Detailed actuals for an account
        - fx_rate_history(currency?): FX rate history with period-over-period changes
        - cost_center_drill(cost_center, period?): Raw ledger entries for a cost center
        - budget_vs_actual(department?): Budget vs actual comparison
        - period_over_period(table?): Period-over-period for any fact table
        - classification_check(): Detect account misclassifications

        Args:
            template_name: Name of the template to execute.
            params: Template parameters as a dict, e.g. {"period": "2024-03"}
                    or {"account_code": "4010", "period": "2024-06"}.

        Returns:
            Dict with query results or error.
        """
        if template_name not in TEMPLATES:
            return {
                "error": f"Unknown template '{template_name}'. Available: {', '.join(TEMPLATES.keys())}"
            }

        try:
            sql = TEMPLATES[template_name](**(params or {}))
        except TypeError as e:
            return {"error": f"Invalid parameters for '{template_name}': {e}"}

        return run_sql_query(sql)

    def get_table_schema(table_name: str) -> dict[str, Any]:
        """Get the schema (column definitions) for a database table.

        Use this to understand a table's structure before querying it.

        Available tables:
        - raw_ledger_entries: Raw journal entries (entry_id, period, account_code, account_type, department, cost_center, currency, segment, country, product, amount_local, description, posted_date)
        - stg_account_mapping: Account code -> type/name mapping (account_code, account_type, account_name)
        - stg_cost_center_mapping: Cost center -> department/region (cost_center, department, region)
        - fct_actuals_monthly: Monthly actuals aggregated (account_code, cost_center, currency, period, department, amount_local, entry_count)
        - fct_budget_monthly: Monthly budget by dept (department, account_type, period, budget_amount)
        - fct_fx_rates: Monthly FX rates (currency, period, rate_to_usd)
        - int_actuals_usd: Actuals converted to USD (account_code, cost_center, department, period, amount_usd)
        - mart_pnl_report: P&L report mart (department, account_type, period, actual_usd, budget_usd, variance_usd, variance_pct)

        Args:
            table_name: Name of the table to inspect.

        Returns:
            Dict with CREATE TABLE SQL and sample data.
        """
        # Validate table_name against known tables to prevent SQL injection
        allowed = set(LINEAGE.keys()) | {"seed_manifest"}
        if table_name not in allowed:
            return {
                "error": (
                    f"Unknown table '{table_name}'. "
                    f"Available: {', '.join(sorted(LINEAGE.keys()))}"
                )
            }

        exe = ctx.executor
        schema_result = exe.execute(
            f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        )
        sample_result = exe.execute(f"SELECT * FROM [{table_name}] LIMIT 5")
        count_result = exe.execute(f"SELECT COUNT(*) as cnt FROM [{table_name}]")

        return {
            "table_name": table_name,
            "create_sql": (
                schema_result.rows[0].get("sql", "") if schema_result.rows else "Table not found"
            ),
            "row_count": count_result.rows[0]["cnt"] if count_result.rows else 0,
            "sample_rows": sample_result.rows,
            "columns": sample_result.columns,
        }

    # -- Lineage tools ------------------------------------------------------

    def get_table_lineage(table_name: str) -> dict[str, Any]:
        """Get data lineage information for a table in the warehouse.

        Shows upstream (source) and downstream (dependent) tables, key columns,
        measure columns, and transformations applied. Use this to understand
        data flow and trace anomalies to their root causes.

        Args:
            table_name: Name of the table to inspect lineage for.

        Returns:
            Dict with lineage metadata, upstream/downstream chains, columns.
        """
        from va_agent.data.lineage_registry import get_downstream_chain, get_upstream_chain

        if table_name not in LINEAGE:
            return {
                "error": f"Unknown table '{table_name}'. Available: {', '.join(sorted(LINEAGE.keys()))}"
            }

        meta = LINEAGE[table_name]
        return {
            "table_name": table_name,
            "description": meta.description,
            "grain": meta.grain,
            "direct_upstream": meta.upstream,
            "full_upstream_chain": get_upstream_chain(table_name),
            "downstream": get_downstream_chain(table_name),
            "key_columns": meta.key_columns,
            "measure_columns": meta.measure_columns,
            "transformations": meta.transformations,
        }

    def get_all_tables() -> dict[str, Any]:
        """List all tables in the warehouse with their descriptions and lineage.

        Call this first to orient yourself on what data is available.

        Returns:
            Dict with total count and list of table summaries.
        """
        tables = []
        for name, meta in LINEAGE.items():
            tables.append(
                {
                    "name": name,
                    "description": meta.description,
                    "grain": meta.grain,
                    "upstream_count": len(meta.upstream),
                    "key_columns": meta.key_columns,
                    "measure_columns": meta.measure_columns,
                }
            )
        return {"total": len(tables), "tables": tables}

    # -- Report tools -------------------------------------------------------

    def write_finding(
        title: str,
        category: str,
        direction: str,
        variance_amount: float,
        variance_pct: float,
        root_cause: str,
        evidence: list[str],
        affected_tables: list[str],
        affected_dimensions: dict[str, Any] | None = None,
        recommendations: list[str] | None = None,
        sql_queries_used: list[str] | None = None,
        confidence_evidence_breadth: float = 0.5,
        confidence_lineage_depth: float = 0.5,
        confidence_variance_explanation: float = 0.5,
        confidence_hypothesis_exclusion: float = 0.5,
        confidence_data_quality: float = 0.5,
        confidence_temporal_consistency: float = 0.5,
    ) -> dict[str, Any]:
        """Record a variance finding with root cause analysis and confidence scoring.

        Call this tool when you have identified a specific variance anomaly,
        determined its root cause through upstream lineage traversal, and have
        supporting evidence from SQL queries.

        CONFIDENCE SCORING: Provide scores 0.0-1.0 for each factor:
        - evidence_breadth: How many independent data points support the finding?
        - lineage_depth: How far upstream did you trace? (0=mart only, 1=raw layer)
        - variance_explanation: How much of the variance does this root cause explain?
        - hypothesis_exclusion: How many alternative hypotheses were ruled out?
        - data_quality: Quality/completeness of underlying data (default 0.5)
        - temporal_consistency: Is the pattern consistent across time periods?

        Weighted formula: EB*25% + LD*20% + VE*25% + HE*15% + DQ*10% + TC*5%

        Args:
            title: Short descriptive title (e.g., "COGS spike in Engineering Q1").
            category: One of: COGS_ANOMALY, REVENUE_ANOMALY, FX_ANOMALY,
                      BUDGET_MISALIGNMENT, CLASSIFICATION_ERROR, OTHER.
            direction: FAVORABLE or UNFAVORABLE.
            variance_amount: Absolute variance in USD.
            variance_pct: Variance as percentage of baseline.
            root_cause: Clear explanation of the identified root cause.
            evidence: List of evidence statements supporting the finding.
            affected_tables: Tables in the lineage involved.
            affected_dimensions: Dimension values (cost_center, period, etc.).
            recommendations: Suggested corrective actions.
            sql_queries_used: SQL queries that produced evidence.
            confidence_*: Scores for each confidence factor (0.0-1.0).

        Returns:
            Dict with the finding ID and confidence score.
        """
        finding_count = ctx.finding_count
        finding_id = f"F-{finding_count + 1:03d}"

        factors = ConfidenceFactors(
            evidence_breadth=confidence_evidence_breadth,
            lineage_depth=confidence_lineage_depth,
            variance_explanation=confidence_variance_explanation,
            hypothesis_exclusion=confidence_hypothesis_exclusion,
            data_quality=confidence_data_quality,
            temporal_consistency=confidence_temporal_consistency,
        )
        weighted = (
            factors.evidence_breadth * 0.25
            + factors.lineage_depth * 0.20
            + factors.variance_explanation * 0.25
            + factors.hypothesis_exclusion * 0.15
            + factors.data_quality * 0.10
            + factors.temporal_consistency * 0.05
        )
        level = (
            ConfidenceLevel.HIGH
            if weighted >= 0.7
            else ConfidenceLevel.MEDIUM
            if weighted >= 0.4
            else ConfidenceLevel.LOW
        )
        confidence = ConfidenceScore(score=round(weighted, 3), level=level, factors=factors)

        try:
            cat = FindingCategory(category)
        except ValueError:
            cat = FindingCategory.OTHER
        try:
            dirn = VarianceDirection(direction)
        except ValueError:
            dirn = VarianceDirection.UNFAVORABLE

        finding = Finding(
            id=finding_id,
            title=title,
            category=cat,
            direction=dirn,
            variance_amount=variance_amount,
            variance_pct=variance_pct,
            root_cause=root_cause,
            evidence=evidence,
            affected_tables=affected_tables,
            affected_dimensions=affected_dimensions or {},
            confidence=confidence,
            recommendations=recommendations or [],
            sql_queries_used=sql_queries_used or [],
        )
        ctx.add_finding(finding)

        return {
            "finding_id": finding_id,
            "confidence_score": confidence.score,
            "confidence_level": confidence.level.value,
            "message": (
                f"Finding '{title}' recorded as {finding_id} "
                f"with {confidence.level.value} confidence ({confidence.score:.1%})"
            ),
        }

    def write_report_section(
        title: str,
        content: str,
        finding_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        """Add a section to the variance analysis report.

        Use this to structure the final report. Typical sections:
        1. Executive Summary
        2. Methodology
        3. Key Findings (references finding IDs)
        4. Detailed Analysis (per finding category)
        5. Recommendations
        6. Data Quality Notes

        Args:
            title: Section title.
            content: Section content in markdown format.
            finding_ids: List of finding IDs referenced in this section.

        Returns:
            Dict confirming the section was added.
        """
        section = ReportSection(
            title=title,
            content=content,
            findings=finding_ids or [],
        )
        count = ctx.add_section(section)

        return {
            "section_number": count,
            "title": title,
            "message": f"Section '{title}' added to report (#{count})",
        }

    return {
        "run_sql_query": run_sql_query,
        "run_sql_template": run_sql_template,
        "get_table_schema": get_table_schema,
        "get_table_lineage": get_table_lineage,
        "get_all_tables": get_all_tables,
        "write_finding": write_finding,
        "write_report_section": write_report_section,
    }
