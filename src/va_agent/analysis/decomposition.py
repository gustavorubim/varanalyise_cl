"""Variance decomposition across dimensions with Pareto driver identification."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class VarianceDriver:
    """A single driver contributing to a variance."""

    dimension: str
    value: str
    variance_amount: float
    variance_pct: float
    contribution_pct: float  # % of total variance explained by this driver


@dataclass
class DecompositionResult:
    """Result of variance decomposition across a dimension."""

    dimension: str
    total_variance: float
    drivers: list[VarianceDriver]
    pareto_drivers: list[VarianceDriver]  # top drivers explaining 80%
    pareto_coverage_pct: float  # actual % explained by pareto drivers


def decompose_variance(
    rows: list[dict],
    dimension_col: str,
    variance_col: str = "variance_usd",
    baseline_col: str = "budget_usd",
    pareto_threshold: float = 80.0,
) -> DecompositionResult:
    """Decompose total variance across a dimension and identify Pareto drivers.

    Takes query result rows and identifies which dimension values contribute
    most to the total variance (the ~20% of items causing ~80% of variance).

    Args:
        rows: List of dicts from a SQL query result.
        dimension_col: Column name for the dimension to decompose by.
        variance_col: Column name for the variance amount.
        baseline_col: Column name for the baseline (for pct calculation).
        pareto_threshold: Cumulative % threshold for Pareto drivers (default 80%).

    Returns:
        DecompositionResult with all drivers and Pareto subset.
    """
    if not rows:
        return DecompositionResult(
            dimension=dimension_col,
            total_variance=0.0,
            drivers=[],
            pareto_drivers=[],
            pareto_coverage_pct=0.0,
        )

    # Aggregate by dimension value
    agg: dict[str, dict[str, float]] = {}
    for row in rows:
        key = str(row.get(dimension_col, "Unknown"))
        if key not in agg:
            agg[key] = {"variance": 0.0, "baseline": 0.0}
        agg[key]["variance"] += float(row.get(variance_col, 0) or 0)
        agg[key]["baseline"] += float(row.get(baseline_col, 0) or 0)

    total_variance = sum(abs(v["variance"]) for v in agg.values())

    if total_variance == 0:
        drivers = [
            VarianceDriver(
                dimension=dimension_col,
                value=k,
                variance_amount=0.0,
                variance_pct=0.0,
                contribution_pct=0.0,
            )
            for k in agg
        ]
        return DecompositionResult(
            dimension=dimension_col,
            total_variance=0.0,
            drivers=drivers,
            pareto_drivers=[],
            pareto_coverage_pct=0.0,
        )

    # Build driver list
    drivers = []
    for key, vals in agg.items():
        baseline = vals["baseline"]
        variance = vals["variance"]
        pct = (variance / abs(baseline) * 100) if baseline != 0 else 0.0
        contribution = abs(variance) / total_variance * 100 if total_variance else 0.0
        drivers.append(
            VarianceDriver(
                dimension=dimension_col,
                value=key,
                variance_amount=round(variance, 2),
                variance_pct=round(pct, 2),
                contribution_pct=round(contribution, 2),
            )
        )

    # Sort by absolute contribution (descending)
    drivers.sort(key=lambda d: abs(d.variance_amount), reverse=True)

    # Identify Pareto drivers (cumulative contribution >= threshold)
    pareto_drivers = []
    cumulative = 0.0
    for driver in drivers:
        pareto_drivers.append(driver)
        cumulative += driver.contribution_pct
        if cumulative >= pareto_threshold:
            break

    return DecompositionResult(
        dimension=dimension_col,
        total_variance=round(total_variance, 2),
        drivers=drivers,
        pareto_drivers=pareto_drivers,
        pareto_coverage_pct=round(cumulative, 2),
    )
