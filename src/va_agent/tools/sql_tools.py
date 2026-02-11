"""SQL tools exposed to the deep agent for variance analysis.

Each function is a plain callable with a rich docstring so the LLM
understands when and how to use it.
"""

from __future__ import annotations

from typing import Any

from va_agent.sql.executor import SQLExecutor
from va_agent.sql.templates import TEMPLATES

# Module-level executor — set by graph/build.py before agent starts
_executor: SQLExecutor | None = None


def set_executor(executor: SQLExecutor) -> None:
    """Wire the shared SQLExecutor instance (called during agent setup)."""
    global _executor
    _executor = executor


def _get_executor() -> SQLExecutor:
    if _executor is None:
        raise RuntimeError("SQLExecutor not initialized — call set_executor() first")
    return _executor


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
        Dict with keys: columns, rows, row_count, truncated, error.
        If error is not None, the query failed.
    """
    exe = _get_executor()
    result = exe.execute(sql)
    return {
        "columns": result.columns,
        "rows": result.rows[:20] if len(result.rows) > 20 else result.rows,
        "row_count": result.row_count,
        "truncated": result.truncated or result.row_count > 20,
        "total_available": result.row_count,
        "error": result.error,
    }


def run_sql_template(
    template_name: str,
    **kwargs: str,
) -> dict[str, Any]:
    """Execute a pre-built SQL template for common analysis patterns.

    Available templates:
    - variance_summary(period?): P&L variance by department/account_type
    - account_detail(account_code, period?): Detailed actuals for an account
    - fx_rate_history(currency?): FX rate history with period-over-period changes
    - cost_center_drill(cost_center, period?): Raw ledger entries for a cost center
    - budget_vs_actual(department?): Budget vs actual comparison
    - period_over_period(table?): Period-over-period for any fact table
    - classification_check(): Detect account misclassifications

    Args:
        template_name: Name of the template to execute.
        **kwargs: Template parameters (e.g., period="2024-03").

    Returns:
        Dict with query results or error.
    """
    if template_name not in TEMPLATES:
        return {
            "error": f"Unknown template '{template_name}'. "
            f"Available: {', '.join(TEMPLATES.keys())}"
        }

    try:
        sql = TEMPLATES[template_name](**kwargs)
    except TypeError as e:
        return {"error": f"Invalid parameters for '{template_name}': {e}"}

    return run_sql_query(sql)


def get_table_schema(table_name: str) -> dict[str, Any]:
    """Get the schema (column definitions) for a database table.

    Use this to understand a table's structure before querying it.

    Available tables:
    - raw_ledger_entries: Raw journal entries (entry_id, period, account_code, account_type, department, cost_center, currency, segment, country, product, amount_local, description, posted_date)
    - stg_account_mapping: Account code → type/name mapping (account_code, account_type, account_name)
    - stg_cost_center_mapping: Cost center → department/region (cost_center, department, region)
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
    exe = _get_executor()

    # Get CREATE TABLE statement
    schema_result = exe.execute(
        f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    )

    # Get sample rows
    sample_result = exe.execute(f"SELECT * FROM [{table_name}] LIMIT 5")

    # Get row count
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
