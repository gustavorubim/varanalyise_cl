"""Unit tests for the lineage registry."""

from va_agent.data.lineage_registry import (
    LINEAGE,
    get_downstream_chain,
    get_upstream_chain,
)


class TestLineageRegistry:
    def test_all_8_tables_present(self):
        expected = {
            "raw_ledger_entries",
            "stg_account_mapping",
            "stg_cost_center_mapping",
            "fct_actuals_monthly",
            "fct_budget_monthly",
            "fct_fx_rates",
            "int_actuals_usd",
            "mart_pnl_report",
        }
        assert set(LINEAGE.keys()) == expected

    def test_raw_ledger_has_no_upstream(self):
        assert LINEAGE["raw_ledger_entries"].upstream == []

    def test_mart_pnl_has_upstream(self):
        upstream = LINEAGE["mart_pnl_report"].upstream
        assert "int_actuals_usd" in upstream
        assert "fct_budget_monthly" in upstream


class TestUpstreamChain:
    def test_raw_has_no_upstream(self):
        chain = get_upstream_chain("raw_ledger_entries")
        assert chain == []

    def test_mart_full_chain(self):
        chain = get_upstream_chain("mart_pnl_report")
        # Should include everything upstream of mart_pnl_report
        assert "raw_ledger_entries" in chain
        assert "fct_actuals_monthly" in chain
        assert "int_actuals_usd" in chain
        assert "fct_fx_rates" in chain

    def test_chain_is_topological(self):
        """Upstream tables should appear before their dependents."""
        chain = get_upstream_chain("mart_pnl_report")
        if "raw_ledger_entries" in chain and "fct_actuals_monthly" in chain:
            assert chain.index("raw_ledger_entries") < chain.index("fct_actuals_monthly")

    def test_unknown_table(self):
        chain = get_upstream_chain("nonexistent")
        assert chain == []


class TestDownstreamChain:
    def test_mart_has_no_downstream(self):
        chain = get_downstream_chain("mart_pnl_report")
        assert chain == []

    def test_raw_has_downstream(self):
        chain = get_downstream_chain("raw_ledger_entries")
        assert len(chain) > 0
        assert "fct_actuals_monthly" in chain

    def test_fx_rates_downstream(self):
        chain = get_downstream_chain("fct_fx_rates")
        assert "int_actuals_usd" in chain
        assert "mart_pnl_report" in chain

    def test_unknown_table(self):
        chain = get_downstream_chain("nonexistent")
        assert chain == []
