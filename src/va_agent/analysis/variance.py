"""Variance computation utilities."""

from __future__ import annotations


def compute_variance(actual: float, budget: float) -> tuple[float, float]:
    """Compute variance amount and percentage.

    Args:
        actual: Actual amount.
        budget: Budget amount.

    Returns:
        Tuple of (variance_amount, variance_pct).
        Variance pct is None-safe: returns 0.0 if budget is zero.
    """
    variance = actual - budget
    if budget == 0:
        pct = 0.0 if actual == 0 else float("inf")
    else:
        pct = (variance / abs(budget)) * 100
    return round(variance, 2), round(pct, 2)


def materiality_threshold(
    total_budget: float,
    pct_threshold: float = 5.0,
    abs_threshold: float = 1000.0,
) -> tuple[float, float]:
    """Compute materiality thresholds for variance analysis.

    A variance is material if it exceeds BOTH:
    - Absolute threshold (default $1,000)
    - Percentage threshold (default 5% of budget)

    Args:
        total_budget: Total budget for the entity/period.
        pct_threshold: Percentage threshold.
        abs_threshold: Absolute dollar threshold.

    Returns:
        Tuple of (absolute_threshold, pct_of_budget_threshold).
    """
    pct_amount = abs(total_budget) * (pct_threshold / 100)
    return max(abs_threshold, pct_amount), pct_threshold


def is_material(
    variance_amount: float,
    variance_pct: float,
    abs_threshold: float = 1000.0,
    pct_threshold: float = 5.0,
) -> bool:
    """Check if a variance exceeds materiality thresholds.

    Args:
        variance_amount: Absolute variance amount.
        variance_pct: Variance as percentage.
        abs_threshold: Minimum absolute amount.
        pct_threshold: Minimum percentage.

    Returns:
        True if the variance is material.
    """
    return abs(variance_amount) >= abs_threshold and abs(variance_pct) >= pct_threshold
