"""Unit tests for confidence scoring."""

from va_agent.analysis.confidence import (
    HIGH_THRESHOLD,
    MEDIUM_THRESHOLD,
    WEIGHTS,
    compute_confidence,
    score_from_qualitative,
)
from va_agent.models import ConfidenceFactors, ConfidenceLevel


class TestWeights:
    def test_weights_sum_to_one(self):
        assert abs(sum(WEIGHTS.values()) - 1.0) < 1e-10

    def test_all_factors_have_weights(self):
        expected = {
            "evidence_breadth",
            "lineage_depth",
            "variance_explanation",
            "hypothesis_exclusion",
            "data_quality",
            "temporal_consistency",
        }
        assert set(WEIGHTS.keys()) == expected


class TestComputeConfidence:
    def test_all_zeros_gives_low(self):
        factors = ConfidenceFactors(
            evidence_breadth=0,
            lineage_depth=0,
            variance_explanation=0,
            hypothesis_exclusion=0,
            data_quality=0,
            temporal_consistency=0,
        )
        score = compute_confidence(factors)
        assert score.score == 0.0
        assert score.level == ConfidenceLevel.LOW

    def test_all_ones_gives_high(self):
        factors = ConfidenceFactors(
            evidence_breadth=1,
            lineage_depth=1,
            variance_explanation=1,
            hypothesis_exclusion=1,
            data_quality=1,
            temporal_consistency=1,
        )
        score = compute_confidence(factors)
        assert score.score == 1.0
        assert score.level == ConfidenceLevel.HIGH

    def test_medium_range(self):
        factors = ConfidenceFactors(
            evidence_breadth=0.5,
            lineage_depth=0.5,
            variance_explanation=0.5,
            hypothesis_exclusion=0.5,
            data_quality=0.5,
            temporal_consistency=0.5,
        )
        score = compute_confidence(factors)
        assert score.score == 0.5
        assert score.level == ConfidenceLevel.MEDIUM

    def test_boundary_high(self):
        """Score exactly at HIGH_THRESHOLD should be HIGH."""
        # Need to find factor values that produce exactly 0.7
        # 0.7 / 1.0 = 0.7 for all factors
        score = score_from_qualitative(0.7, 0.7, 0.7, 0.7, 0.7, 0.7)
        assert score.score == HIGH_THRESHOLD
        assert score.level == ConfidenceLevel.HIGH

    def test_boundary_medium(self):
        """Score exactly at MEDIUM_THRESHOLD should be MEDIUM."""
        score = score_from_qualitative(0.4, 0.4, 0.4, 0.4, 0.4, 0.4)
        assert score.score == MEDIUM_THRESHOLD
        assert score.level == ConfidenceLevel.MEDIUM

    def test_just_below_medium(self):
        score = score_from_qualitative(0.3, 0.3, 0.3, 0.3, 0.3, 0.3)
        assert score.score < MEDIUM_THRESHOLD
        assert score.level == ConfidenceLevel.LOW

    def test_weighted_correctly(self):
        """Verify specific weighted calculation."""
        factors = ConfidenceFactors(
            evidence_breadth=1.0,  # × 0.25 = 0.25
            lineage_depth=0.0,  # × 0.20 = 0.00
            variance_explanation=1.0,  # × 0.25 = 0.25
            hypothesis_exclusion=0.0,  # × 0.15 = 0.00
            data_quality=0.0,  # × 0.10 = 0.00
            temporal_consistency=0.0,  # × 0.05 = 0.00
        )
        score = compute_confidence(factors)
        assert score.score == 0.5
        assert score.level == ConfidenceLevel.MEDIUM

    def test_factors_preserved(self):
        factors = ConfidenceFactors(
            evidence_breadth=0.8,
            lineage_depth=0.6,
            variance_explanation=0.9,
            hypothesis_exclusion=0.7,
            data_quality=0.5,
            temporal_consistency=0.4,
        )
        score = compute_confidence(factors)
        assert score.factors.evidence_breadth == 0.8
        assert score.factors.lineage_depth == 0.6

    def test_deterministic(self):
        """Same inputs always produce same output."""
        s1 = score_from_qualitative(0.6, 0.7, 0.8, 0.5, 0.9, 0.3)
        s2 = score_from_qualitative(0.6, 0.7, 0.8, 0.5, 0.9, 0.3)
        assert s1.score == s2.score
        assert s1.level == s2.level
