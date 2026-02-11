"""Confidence scoring for variance findings.

Implements the weighted scoring formula:
  evidence_breadth × 25% +
  lineage_depth × 20% +
  variance_explanation × 25% +
  hypothesis_exclusion × 15% +
  data_quality × 10% +
  temporal_consistency × 5%
"""

from __future__ import annotations

from va_agent.models import ConfidenceFactors, ConfidenceLevel, ConfidenceScore

# Weight vector (must sum to 1.0)
WEIGHTS = {
    "evidence_breadth": 0.25,
    "lineage_depth": 0.20,
    "variance_explanation": 0.25,
    "hypothesis_exclusion": 0.15,
    "data_quality": 0.10,
    "temporal_consistency": 0.05,
}

# Thresholds
HIGH_THRESHOLD = 0.7
MEDIUM_THRESHOLD = 0.4


def compute_confidence(factors: ConfidenceFactors) -> ConfidenceScore:
    """Compute weighted confidence score from individual factor scores.

    Args:
        factors: Per-factor scores, each 0.0 to 1.0.

    Returns:
        ConfidenceScore with weighted composite and level.
    """
    weighted = (
        factors.evidence_breadth * WEIGHTS["evidence_breadth"]
        + factors.lineage_depth * WEIGHTS["lineage_depth"]
        + factors.variance_explanation * WEIGHTS["variance_explanation"]
        + factors.hypothesis_exclusion * WEIGHTS["hypothesis_exclusion"]
        + factors.data_quality * WEIGHTS["data_quality"]
        + factors.temporal_consistency * WEIGHTS["temporal_consistency"]
    )

    weighted = round(weighted, 3)

    if weighted >= HIGH_THRESHOLD:
        level = ConfidenceLevel.HIGH
    elif weighted >= MEDIUM_THRESHOLD:
        level = ConfidenceLevel.MEDIUM
    else:
        level = ConfidenceLevel.LOW

    return ConfidenceScore(score=weighted, level=level, factors=factors)


def score_from_qualitative(
    evidence_breadth: float = 0.5,
    lineage_depth: float = 0.5,
    variance_explanation: float = 0.5,
    hypothesis_exclusion: float = 0.5,
    data_quality: float = 0.5,
    temporal_consistency: float = 0.5,
) -> ConfidenceScore:
    """Convenience function: compute confidence from individual float scores.

    Args:
        evidence_breadth: 0-1, how many independent data points (1 = 3+ queries).
        lineage_depth: 0-1, how far upstream traced (1 = reached raw layer).
        variance_explanation: 0-1, what % of variance explained (1 = fully).
        hypothesis_exclusion: 0-1, how many alternatives ruled out (1 = 2+).
        data_quality: 0-1, quality of underlying data (1 = no concerns).
        temporal_consistency: 0-1, consistency across time (1 = fully consistent).

    Returns:
        ConfidenceScore with level and breakdown.
    """
    factors = ConfidenceFactors(
        evidence_breadth=evidence_breadth,
        lineage_depth=lineage_depth,
        variance_explanation=variance_explanation,
        hypothesis_exclusion=hypothesis_exclusion,
        data_quality=data_quality,
        temporal_consistency=temporal_consistency,
    )
    return compute_confidence(factors)
