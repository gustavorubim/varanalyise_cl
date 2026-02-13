"""Unit tests for variance decomposition and Pareto analysis."""

from va_agent.analysis.decomposition import decompose_variance


class TestDecomposeVariance:
    def test_empty_rows(self):
        result = decompose_variance([], "department")
        assert result.total_variance == 0.0
        assert result.drivers == []
        assert result.pareto_drivers == []

    def test_single_driver(self):
        rows = [
            {"department": "Sales", "variance_usd": 1000, "budget_usd": 10000},
        ]
        result = decompose_variance(rows, "department")
        assert result.total_variance == 1000.0
        assert len(result.drivers) == 1
        assert result.drivers[0].contribution_pct == 100.0
        assert len(result.pareto_drivers) == 1

    def test_pareto_80_20(self):
        """Top driver explaining >80% should be the only Pareto driver."""
        rows = [
            {"department": "Sales", "variance_usd": 9000, "budget_usd": 100000},
            {"department": "Marketing", "variance_usd": 500, "budget_usd": 50000},
            {"department": "Engineering", "variance_usd": 300, "budget_usd": 40000},
            {"department": "Finance", "variance_usd": 200, "budget_usd": 30000},
        ]
        result = decompose_variance(rows, "department")
        assert len(result.pareto_drivers) == 1
        assert result.pareto_drivers[0].value == "Sales"
        assert result.pareto_coverage_pct >= 80.0

    def test_pareto_needs_multiple(self):
        """When no single driver explains 80%, multiple are needed."""
        rows = [
            {"department": "Sales", "variance_usd": 400, "budget_usd": 10000},
            {"department": "Marketing", "variance_usd": 350, "budget_usd": 10000},
            {"department": "Engineering", "variance_usd": 150, "budget_usd": 10000},
            {"department": "Finance", "variance_usd": 100, "budget_usd": 10000},
        ]
        result = decompose_variance(rows, "department")
        assert len(result.pareto_drivers) >= 2
        assert result.pareto_coverage_pct >= 80.0

    def test_drivers_sorted_by_absolute_variance(self):
        rows = [
            {"department": "A", "variance_usd": -5000, "budget_usd": 50000},
            {"department": "B", "variance_usd": 3000, "budget_usd": 30000},
            {"department": "C", "variance_usd": 8000, "budget_usd": 80000},
        ]
        result = decompose_variance(rows, "department")
        amounts = [abs(d.variance_amount) for d in result.drivers]
        assert amounts == sorted(amounts, reverse=True)

    def test_aggregates_by_dimension(self):
        """Multiple rows for same dimension value should be summed."""
        rows = [
            {"department": "Sales", "variance_usd": 1000, "budget_usd": 5000},
            {"department": "Sales", "variance_usd": 2000, "budget_usd": 5000},
            {"department": "Marketing", "variance_usd": 500, "budget_usd": 3000},
        ]
        result = decompose_variance(rows, "department")
        sales_driver = next(d for d in result.drivers if d.value == "Sales")
        assert sales_driver.variance_amount == 3000.0

    def test_zero_variance(self):
        rows = [
            {"department": "Sales", "variance_usd": 0, "budget_usd": 10000},
        ]
        result = decompose_variance(rows, "department")
        assert result.total_variance == 0.0

    def test_custom_columns(self):
        rows = [
            {"region": "US", "diff": 5000, "base": 100000},
        ]
        result = decompose_variance(rows, "region", variance_col="diff", baseline_col="base")
        assert result.drivers[0].value == "US"
        assert result.drivers[0].variance_amount == 5000.0

    def test_negative_variances(self):
        rows = [
            {"department": "Sales", "variance_usd": -5000, "budget_usd": 50000},
            {"department": "Marketing", "variance_usd": -3000, "budget_usd": 30000},
        ]
        result = decompose_variance(rows, "department")
        assert result.total_variance == 8000.0  # sum of absolutes
        assert result.drivers[0].variance_amount == -5000.0
