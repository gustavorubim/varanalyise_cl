"""Lineage tools exposed to the deep agent for upstream/downstream traversal."""

from __future__ import annotations

from typing import Any

from va_agent.data.lineage_registry import (
    LINEAGE,
    get_downstream_chain,
    get_upstream_chain,
)


def get_table_lineage(table_name: str) -> dict[str, Any]:
    """Get the full lineage for a table: upstream sources and downstream consumers.

    Use this to understand data flow when tracing a variance to its root cause.
    Start from the report layer (mart_pnl_report) and traverse upstream to find
    where an anomaly originates.

    Lineage layers:
    - Layer 0 (raw): raw_ledger_entries
    - Layer 1 (staging): stg_account_mapping, stg_cost_center_mapping
    - Layer 2 (facts): fct_actuals_monthly, fct_budget_monthly, fct_fx_rates
    - Layer 3 (intermediate): int_actuals_usd
    - Layer 4 (mart): mart_pnl_report

    Args:
        table_name: The table to get lineage for.

    Returns:
        Dict with table metadata, upstream chain, and downstream chain.
    """
    if table_name not in LINEAGE:
        return {"error": f"Unknown table '{table_name}'. Available: {list(LINEAGE.keys())}"}

    meta = LINEAGE[table_name]
    return {
        "table_name": meta.name,
        "description": meta.description,
        "grain": meta.grain,
        "direct_upstream": meta.upstream,
        "direct_downstream": [name for name, m in LINEAGE.items() if table_name in m.upstream],
        "full_upstream_chain": get_upstream_chain(table_name),
        "full_downstream_chain": get_downstream_chain(table_name),
        "key_columns": meta.key_columns,
        "measure_columns": meta.measure_columns,
        "transformations": meta.transformations,
    }


def get_all_tables() -> dict[str, Any]:
    """Get a summary of all tables in the warehouse with their lineage relationships.

    Use this as your first orientation step to understand the data model
    before diving into specific tables.

    Returns:
        Dict with table summaries including name, description, grain, and connections.
    """
    tables = []
    for name, meta in LINEAGE.items():
        tables.append(
            {
                "name": meta.name,
                "description": meta.description,
                "grain": meta.grain,
                "upstream": meta.upstream,
                "downstream": [n for n, m in LINEAGE.items() if name in m.upstream],
                "key_columns": meta.key_columns,
                "measure_columns": meta.measure_columns,
                "transformations": meta.transformations,
            }
        )
    return {"tables": tables, "total": len(tables)}
