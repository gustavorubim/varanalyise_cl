"""Unit tests for standalone deep spike helpers."""

from __future__ import annotations

from va_agent.graph.deep_engine import (
    compute_consistency,
    evaluate_findings,
)
from va_agent.models import (
    ConfidenceFactors,
    ConfidenceLevel,
    ConfidenceScore,
    Finding,
    FindingCategory,
    VarianceDirection,
)


def _make_finding(
    finding_id: str,
    title: str,
    category: FindingCategory,
    evidence: list[str],
    affected_dimensions: dict[str, str],
    affected_tables: list[str] | None = None,
) -> Finding:
    return Finding(
        id=finding_id,
        title=title,
        category=category,
        direction=VarianceDirection.UNFAVORABLE,
        variance_amount=1000.0,
        variance_pct=10.0,
        root_cause=title,
        evidence=evidence,
        affected_tables=affected_tables or ["mart_pnl_report", "fct_actuals_monthly"],
        affected_dimensions=affected_dimensions,
        confidence=ConfidenceScore(
            score=0.8,
            level=ConfidenceLevel.HIGH,
            factors=ConfidenceFactors(
                evidence_breadth=0.8,
                lineage_depth=0.8,
                variance_explanation=0.8,
                hypothesis_exclusion=0.8,
                data_quality=0.8,
                temporal_consistency=0.8,
            ),
        ),
    )


def test_evaluate_findings_covers_all_seeded_anomalies():
    findings = [
        _make_finding(
            "F-001",
            "COGS spike CC-300 2024-03",
            FindingCategory.COGS_ANOMALY,
            ["cc-300", "2024-03", "upstream in raw_ledger_entries"],
            {"cost_center": "CC-300", "period": "2024-03"},
        ),
        _make_finding(
            "F-002",
            "Sales revenue drop 2024-06",
            FindingCategory.REVENUE_ANOMALY,
            ["sales", "2024-06", "revenue is zero"],
            {"department": "Sales", "period": "2024-06"},
        ),
        _make_finding(
            "F-003",
            "EUR FX jump 2024-07",
            FindingCategory.FX_ANOMALY,
            ["eur", "2024-07", "fx rate deviation"],
            {"currency": "EUR", "period": "2024-07"},
        ),
        _make_finding(
            "F-004",
            "Finance budget doubled in Q3",
            FindingCategory.BUDGET_MISALIGNMENT,
            ["finance", "2024-08", "q3"],
            {"department": "Finance", "period": "2024-08"},
        ),
        _make_finding(
            "F-005",
            "Classification mapping mismatch",
            FindingCategory.CLASSIFICATION_ERROR,
            ["classification", "mapping", "account_type mismatch"],
            {"account_code": "4010"},
        ),
    ]

    result = evaluate_findings(findings)
    assert result["matched_anomalies"] == 5
    assert result["metrics"]["anomaly_recall"] == 1.0
    assert result["metrics"]["precision_proxy"] == 1.0
    assert result["metrics"]["root_cause_depth"] == 1.0


def test_compute_consistency_returns_stddev_metrics():
    evaluations = [
        {"metrics": {"anomaly_recall": 0.8, "precision_proxy": 0.7}},
        {"metrics": {"anomaly_recall": 1.0, "precision_proxy": 0.9}},
        {"metrics": {"anomaly_recall": 0.6, "precision_proxy": 0.8}},
    ]
    result = compute_consistency(evaluations)
    assert result["recall_stddev"] > 0
    assert result["precision_stddev"] > 0
