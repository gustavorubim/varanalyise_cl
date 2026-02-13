"""Parameterized SQL templates for common variance analysis patterns.

Each template is a function that returns a SQL string. Parameters are
validated against known patterns before interpolation to prevent SQL
injection from LLM-generated values.
"""

from __future__ import annotations

import re
from typing import Callable

from va_agent.data.lineage_registry import LINEAGE

# Pattern for period values: YYYY-MM format
_PERIOD_RE = re.compile(r"^\d{4}-\d{2}$")

# Pattern for simple identifiers (account codes, cost centers, departments, currencies)
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def _validate_table(table: str) -> str:
    """Validate table name against known lineage."""
    if table not in LINEAGE and table != "seed_manifest":
        raise ValueError(f"Unknown table: {table}")
    return table


def _validate_period(period: str) -> str:
    """Validate period matches YYYY-MM format."""
    if not _PERIOD_RE.match(period):
        raise ValueError(f"Invalid period format: {period!r} (expected YYYY-MM)")
    return period


def _validate_identifier(value: str, param_name: str) -> str:
    """Validate a simple identifier (no SQL-special characters)."""
    if not _IDENTIFIER_RE.match(value):
        raise ValueError(
            f"Invalid {param_name}: {value!r} (only alphanumeric, hyphens, underscores allowed)"
        )
    return value


# ── Templates ─────────────────────────────────────────────────────────────────


def variance_summary(period: str | None = None) -> str:
    """P&L variance summary by department and account type."""
    if period:
        _validate_period(period)
    where = f"WHERE period = '{period}'" if period else ""
    return f"""
SELECT
    department,
    account_type,
    period,
    ROUND(SUM(actual_usd), 2) AS total_actual,
    ROUND(SUM(budget_usd), 2) AS total_budget,
    ROUND(SUM(variance_usd), 2) AS total_variance,
    ROUND(AVG(variance_pct), 2) AS avg_variance_pct
FROM mart_pnl_report
{where}
GROUP BY department, account_type, period
ORDER BY ABS(SUM(variance_usd)) DESC
""".strip()


def account_detail(account_code: str, period: str | None = None) -> str:
    """Detailed actuals for a specific account code."""
    _validate_identifier(account_code, "account_code")
    if period:
        _validate_period(period)
    where_parts = [f"a.account_code = '{account_code}'"]
    if period:
        where_parts.append(f"a.period = '{period}'")
    where = "WHERE " + " AND ".join(where_parts)
    return f"""
SELECT
    a.account_code,
    m.account_type,
    m.account_name,
    a.cost_center,
    a.currency,
    a.period,
    a.amount_local,
    a.entry_count
FROM fct_actuals_monthly a
JOIN stg_account_mapping m ON a.account_code = m.account_code
{where}
ORDER BY a.period, a.cost_center
""".strip()


def fx_rate_history(currency: str | None = None) -> str:
    """FX rate history, optionally filtered by currency."""
    if currency:
        _validate_identifier(currency, "currency")
    where = f"WHERE currency = '{currency}'" if currency else ""
    return f"""
SELECT
    currency,
    period,
    rate_to_usd,
    LAG(rate_to_usd) OVER (PARTITION BY currency ORDER BY period) AS prev_rate,
    ROUND(
        (rate_to_usd - LAG(rate_to_usd) OVER (PARTITION BY currency ORDER BY period))
        / LAG(rate_to_usd) OVER (PARTITION BY currency ORDER BY period) * 100,
        2
    ) AS pct_change
FROM fct_fx_rates
{where}
ORDER BY currency, period
""".strip()


def cost_center_drill(cost_center: str, period: str | None = None) -> str:
    """Drill into a specific cost center's entries."""
    _validate_identifier(cost_center, "cost_center")
    if period:
        _validate_period(period)
    where_parts = [f"r.cost_center = '{cost_center}'"]
    if period:
        where_parts.append(f"r.period = '{period}'")
    where = "WHERE " + " AND ".join(where_parts)
    return f"""
SELECT
    r.entry_id,
    r.period,
    r.account_code,
    r.account_type,
    r.department,
    r.currency,
    r.amount_local,
    r.description
FROM raw_ledger_entries r
{where}
ORDER BY r.period, r.account_code
""".strip()


def budget_vs_actual(department: str | None = None) -> str:
    """Budget vs actual comparison by department."""
    if department:
        _validate_identifier(department, "department")
    where = f"WHERE department = '{department}'" if department else ""
    return f"""
SELECT
    department,
    account_type,
    period,
    actual_usd,
    budget_usd,
    variance_usd,
    variance_pct
FROM mart_pnl_report
{where}
ORDER BY department, account_type, period
""".strip()


def period_over_period(table: str = "fct_actuals_monthly") -> str:
    """Period-over-period comparison for any fact table."""
    _validate_table(table)
    if table == "fct_actuals_monthly":
        return """
SELECT
    account_code,
    cost_center,
    period,
    amount_local,
    LAG(amount_local) OVER (
        PARTITION BY account_code, cost_center ORDER BY period
    ) AS prev_period_amount,
    ROUND(
        amount_local - LAG(amount_local) OVER (
            PARTITION BY account_code, cost_center ORDER BY period
        ),
        2
    ) AS period_change
FROM fct_actuals_monthly
ORDER BY account_code, cost_center, period
""".strip()
    elif table == "int_actuals_usd":
        return """
SELECT
    account_code,
    cost_center,
    period,
    amount_usd,
    LAG(amount_usd) OVER (
        PARTITION BY account_code, cost_center ORDER BY period
    ) AS prev_period_amount,
    ROUND(
        amount_usd - LAG(amount_usd) OVER (
            PARTITION BY account_code, cost_center ORDER BY period
        ),
        2
    ) AS period_change
FROM int_actuals_usd
ORDER BY account_code, cost_center, period
""".strip()
    else:
        return f"SELECT * FROM {table} ORDER BY period"


def classification_check() -> str:
    """Check for potential account misclassification."""
    return """
SELECT
    r.account_code,
    r.account_type AS ledger_type,
    m.account_type AS mapping_type,
    COUNT(*) AS entry_count,
    ROUND(SUM(r.amount_local), 2) AS total_amount
FROM raw_ledger_entries r
JOIN stg_account_mapping m ON r.account_code = m.account_code
WHERE r.account_type != m.account_type
GROUP BY r.account_code, r.account_type, m.account_type
ORDER BY entry_count DESC
""".strip()


# Registry of all templates
TEMPLATES: dict[str, Callable[..., str]] = {
    "variance_summary": variance_summary,
    "account_detail": account_detail,
    "fx_rate_history": fx_rate_history,
    "cost_center_drill": cost_center_drill,
    "budget_vs_actual": budget_vs_actual,
    "period_over_period": period_over_period,
    "classification_check": classification_check,
}
