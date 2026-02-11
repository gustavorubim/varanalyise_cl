"""Report-writing tools exposed to the deep agent.

These tools let the agent incrementally build findings and report sections
during analysis. Findings are stored in-memory and included in the final report.
"""

from __future__ import annotations

from typing import Any

from va_agent.models import (
    ConfidenceFactors,
    ConfidenceLevel,
    ConfidenceScore,
    Finding,
    FindingCategory,
    ReportSection,
    VarianceDirection,
)

# Module-level storage — accumulated during agent execution
_findings: list[Finding] = []
_sections: list[ReportSection] = []


def reset_state() -> None:
    """Clear accumulated findings and sections (called between runs)."""
    global _findings, _sections
    _findings = []
    _sections = []


def get_findings() -> list[Finding]:
    """Return all accumulated findings."""
    return list(_findings)


def get_sections() -> list[ReportSection]:
    """Return all accumulated report sections."""
    return list(_sections)


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

    Weighted formula: EB×25% + LD×20% + VE×25% + HE×15% + DQ×10% + TC×5%

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
    finding_id = f"F-{len(_findings) + 1:03d}"

    # Compute weighted confidence score
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

    # Validate enums
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
    _findings.append(finding)

    return {
        "finding_id": finding_id,
        "confidence_score": confidence.score,
        "confidence_level": confidence.level.value,
        "message": f"Finding '{title}' recorded as {finding_id} with {confidence.level.value} confidence ({confidence.score:.1%})",
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
    _sections.append(section)

    return {
        "section_number": len(_sections),
        "title": title,
        "message": f"Section '{title}' added to report (#{len(_sections)})",
    }
