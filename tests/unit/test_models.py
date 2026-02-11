"""Unit tests for Pydantic models."""

import pytest
from pydantic import ValidationError

from va_agent.models import (
    ConfidenceFactors,
    ConfidenceLevel,
    ConfidenceScore,
    Finding,
    FindingCategory,
    QueryResult,
    VarianceDirection,
    VarianceReport,
)


class TestConfidenceFactors:
    def test_valid_factors(self):
        f = ConfidenceFactors(
            evidence_breadth=0.5,
            lineage_depth=0.8,
            variance_explanation=0.7,
            hypothesis_exclusion=0.6,
            data_quality=0.9,
            temporal_consistency=0.4,
        )
        assert f.evidence_breadth == 0.5

    def test_rejects_negative(self):
        with pytest.raises(ValidationError):
            ConfidenceFactors(
                evidence_breadth=-0.1,
                lineage_depth=0.5,
                variance_explanation=0.5,
                hypothesis_exclusion=0.5,
                data_quality=0.5,
                temporal_consistency=0.5,
            )

    def test_rejects_over_one(self):
        with pytest.raises(ValidationError):
            ConfidenceFactors(
                evidence_breadth=1.1,
                lineage_depth=0.5,
                variance_explanation=0.5,
                hypothesis_exclusion=0.5,
                data_quality=0.5,
                temporal_consistency=0.5,
            )


class TestFinding:
    def test_valid_finding(self):
        f = Finding(
            id="F-001",
            title="Test Finding",
            category=FindingCategory.COGS_ANOMALY,
            direction=VarianceDirection.UNFAVORABLE,
            variance_amount=10000,
            variance_pct=25.0,
            root_cause="Test cause",
            evidence=["Evidence 1"],
            affected_tables=["mart_pnl_report"],
            confidence=ConfidenceScore(
                score=0.75,
                level=ConfidenceLevel.HIGH,
                factors=ConfidenceFactors(
                    evidence_breadth=0.8,
                    lineage_depth=0.7,
                    variance_explanation=0.8,
                    hypothesis_exclusion=0.6,
                    data_quality=0.8,
                    temporal_consistency=0.7,
                ),
            ),
        )
        assert f.id == "F-001"
        assert f.category == FindingCategory.COGS_ANOMALY

    def test_finding_json_roundtrip(self):
        f = Finding(
            id="F-001",
            title="Test",
            category=FindingCategory.OTHER,
            direction=VarianceDirection.FAVORABLE,
            variance_amount=500,
            variance_pct=5.0,
            root_cause="Cause",
            evidence=["E1"],
            affected_tables=["t1"],
            confidence=ConfidenceScore(
                score=0.5,
                level=ConfidenceLevel.MEDIUM,
                factors=ConfidenceFactors(
                    evidence_breadth=0.5,
                    lineage_depth=0.5,
                    variance_explanation=0.5,
                    hypothesis_exclusion=0.5,
                    data_quality=0.5,
                    temporal_consistency=0.5,
                ),
            ),
        )
        json_str = f.model_dump_json()
        f2 = Finding.model_validate_json(json_str)
        assert f2.id == f.id
        assert f2.confidence.score == f.confidence.score


class TestVarianceReport:
    def test_empty_report(self):
        r = VarianceReport(executive_summary="No findings")
        assert r.title == "Variance Analysis Report"
        assert len(r.findings) == 0

    def test_report_json_schema(self):
        schema = VarianceReport.model_json_schema()
        assert "properties" in schema
        assert "findings" in schema["properties"]


class TestQueryResult:
    def test_successful_result(self):
        r = QueryResult(sql="SELECT 1", columns=["x"], rows=[{"x": 1}], row_count=1)
        assert r.error is None

    def test_error_result(self):
        r = QueryResult(sql="BAD SQL", error="Syntax error")
        assert r.error is not None
        assert r.row_count == 0
