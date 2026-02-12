"""Static data lineage registry for the 8-table warehouse model.

Provides upstream/downstream traversal for root-cause tracing.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class TableMeta:
    """Metadata for a single table in the warehouse lineage."""

    name: str
    description: str
    grain: str  # what each row represents
    upstream: list[str] = field(default_factory=list)
    key_columns: list[str] = field(default_factory=list)
    measure_columns: list[str] = field(default_factory=list)
    transformations: list[str] = field(default_factory=list)


# ── Lineage DAG ──────────────────────────────────────────────────────────────
# Layer 0 (raw):  raw_ledger_entries
# Layer 1 (staging): stg_account_mapping, stg_cost_center_mapping
# Layer 2 (facts): fct_actuals_monthly, fct_budget_monthly, fct_fx_rates
# Layer 3 (intermediate): int_actuals_usd
# Layer 4 (mart): mart_pnl_report

LINEAGE: dict[str, TableMeta] = {
    "raw_ledger_entries": TableMeta(
        name="raw_ledger_entries",
        description="Raw journal entries from source ledgers (Financial Sample + General Ledger)",
        grain="One row per journal entry line",
        upstream=[],
        key_columns=["entry_id", "account_code", "period", "cost_center", "department"],
        measure_columns=["amount", "amount_local"],
        transformations=[],
    ),
    "stg_account_mapping": TableMeta(
        name="stg_account_mapping",
        description="Account code to account type/name mapping",
        grain="One row per account code",
        upstream=["raw_ledger_entries"],
        key_columns=["account_code"],
        measure_columns=[],
        transformations=["SELECT DISTINCT account_code, account_type, account_name FROM raw_ledger_entries"],
    ),
    "stg_cost_center_mapping": TableMeta(
        name="stg_cost_center_mapping",
        description="Cost center to department/region mapping",
        grain="One row per cost center",
        upstream=["raw_ledger_entries"],
        key_columns=["cost_center"],
        measure_columns=[],
        transformations=["SELECT DISTINCT cost_center, department, region FROM raw_ledger_entries"],
    ),
    "fct_actuals_monthly": TableMeta(
        name="fct_actuals_monthly",
        description="Monthly actual amounts by account, cost center, and currency",
        grain="One row per account × cost_center × currency × month",
        upstream=["raw_ledger_entries", "stg_account_mapping", "stg_cost_center_mapping"],
        key_columns=["account_code", "cost_center", "currency", "period"],
        measure_columns=["amount_local", "entry_count"],
        transformations=[
            "GROUP BY account_code, cost_center, currency, period, department",
            "SUM(amount_local) -> amount_local",
            "COUNT(entry_id) -> entry_count",
        ],
    ),
    "fct_budget_monthly": TableMeta(
        name="fct_budget_monthly",
        description="Monthly budget amounts by department (quarterly budgets spread to monthly)",
        grain="One row per department × account_type × month",
        upstream=[],
        key_columns=["department", "account_type", "period"],
        measure_columns=["budget_amount"],
        transformations=[],
    ),
    "fct_fx_rates": TableMeta(
        name="fct_fx_rates",
        description="Monthly FX rates to USD (synthesized from currency mix)",
        grain="One row per currency × month",
        upstream=[],
        key_columns=["currency", "period"],
        measure_columns=["rate_to_usd"],
        transformations=[],
    ),
    "int_actuals_usd": TableMeta(
        name="int_actuals_usd",
        description="Actuals converted to USD using FX rates",
        grain="One row per account × cost_center × month",
        upstream=["fct_actuals_monthly", "fct_fx_rates"],
        key_columns=["account_code", "cost_center", "period"],
        measure_columns=["amount_usd"],
        transformations=[
            "JOIN fct_fx_rates ON (currency, period)",
            "amount_usd = amount_local × rate_to_usd",
            "GROUP BY account_code, cost_center, department, period; SUM(amount_usd)",
            "Currency dimension dropped (consolidated to USD)",
        ],
    ),
    "mart_pnl_report": TableMeta(
        name="mart_pnl_report",
        description="P&L report pivoted by account type with budget comparison",
        grain="One row per department × account_type × month",
        upstream=["int_actuals_usd", "fct_budget_monthly", "stg_account_mapping"],
        key_columns=["department", "account_type", "period"],
        measure_columns=["actual_usd", "budget_usd", "variance_usd", "variance_pct"],
        transformations=[
            "JOIN stg_account_mapping ON account_code -> account_type",
            "GROUP BY department, account_type, period; SUM(amount_usd) -> actual_usd",
            "JOIN fct_budget_monthly ON (department, account_type, period)",
            "variance_usd = actual_usd - budget_usd",
            "variance_pct = (variance_usd / budget_usd) × 100",
        ],
    ),
}


def get_upstream_chain(table: str, visited: set[str] | None = None) -> list[str]:
    """Return all upstream tables (transitive) in topological order.

    Args:
        table: Starting table name.
        visited: Internal set for cycle detection.

    Returns:
        List of upstream table names, deepest first.
    """
    if visited is None:
        visited = set()
    if table in visited or table not in LINEAGE:
        return []
    visited.add(table)
    result: list[str] = []
    for parent in LINEAGE[table].upstream:
        result.extend(get_upstream_chain(parent, visited))
        if parent not in result:
            result.append(parent)
    return result


def get_downstream_chain(table: str, visited: set[str] | None = None) -> list[str]:
    """Return all downstream tables (transitive).

    Args:
        table: Starting table name.
        visited: Internal set for cycle detection.

    Returns:
        List of downstream table names, nearest first.
    """
    if visited is None:
        visited = set()
    if table in visited or table not in LINEAGE:
        return []
    visited.add(table)
    result: list[str] = []
    for name, meta in LINEAGE.items():
        if table in meta.upstream and name not in visited:
            result.append(name)
            result.extend(get_downstream_chain(name, visited))
    return result
