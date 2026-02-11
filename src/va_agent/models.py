"""Pydantic models for variance analysis domain objects."""

from __future__ import annotations

import ast
import json
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator


# --- Enums ---


class ConfidenceLevel(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class FindingCategory(str, Enum):
    COGS_ANOMALY = "COGS_ANOMALY"
    REVENUE_ANOMALY = "REVENUE_ANOMALY"
    FX_ANOMALY = "FX_ANOMALY"
    BUDGET_MISALIGNMENT = "BUDGET_MISALIGNMENT"
    CLASSIFICATION_ERROR = "CLASSIFICATION_ERROR"
    OTHER = "OTHER"


class VarianceDirection(str, Enum):
    FAVORABLE = "FAVORABLE"
    UNFAVORABLE = "UNFAVORABLE"


def _parse_stringified(value: str) -> Any | None:
    """Best-effort parse for JSON/Python-literal strings from model output."""
    text = value.strip()
    if not text:
        return None

    for parser in (json.loads, ast.literal_eval):
        try:
            return parser(text)
        except Exception:
            continue
    return None


def _coerce_str_list(value: Any) -> list[str]:
    """Coerce scalar or stringified collections into list[str]."""
    if value is None:
        return []

    if isinstance(value, str):
        parsed = _parse_stringified(value)
        if isinstance(parsed, (list, tuple, set)):
            return [str(v) for v in parsed]
        text = value.strip()
        return [text] if text else []

    if isinstance(value, (list, tuple, set)):
        return [str(v) for v in value]

    return [str(value)]


def _coerce_dimensions(value: Any) -> dict[str, Any]:
    """Coerce common LLM output shapes into dict form."""
    if value is None:
        return {}

    if isinstance(value, dict):
        return {str(k): v for k, v in value.items()}

    if isinstance(value, str):
        parsed = _parse_stringified(value)
        if isinstance(parsed, dict):
            return {str(k): v for k, v in parsed.items()}
        if isinstance(parsed, (list, tuple, set)):
            return {"values": list(parsed)}
        text = value.strip()
        return {"scope": text} if text else {}

    if isinstance(value, (list, tuple, set)):
        return {"values": list(value)}

    return {"value": str(value)}


# --- Confidence ---


class ConfidenceFactors(BaseModel):
    """Per-factor breakdown for confidence scoring."""

    evidence_breadth: float = Field(ge=0, le=1, description="Breadth of supporting evidence (0-1)")
    lineage_depth: float = Field(ge=0, le=1, description="Depth of lineage traversal (0-1)")
    variance_explanation: float = Field(
        ge=0, le=1, description="How well variance is explained (0-1)"
    )
    hypothesis_exclusion: float = Field(
        ge=0, le=1, description="Alternative hypotheses ruled out (0-1)"
    )
    data_quality: float = Field(ge=0, le=1, description="Quality of underlying data (0-1)")
    temporal_consistency: float = Field(
        ge=0, le=1, description="Consistency across time periods (0-1)"
    )


class ConfidenceScore(BaseModel):
    """Weighted confidence score with factor breakdown."""

    score: float = Field(ge=0, le=1, description="Weighted composite score (0-1)")
    level: ConfidenceLevel
    factors: ConfidenceFactors


# --- Findings ---


class Finding(BaseModel):
    """A single variance finding with root cause analysis."""

    id: str = Field(description="Unique finding identifier, e.g. F-001")
    title: str = Field(description="Short descriptive title")
    category: FindingCategory
    direction: VarianceDirection
    variance_amount: float = Field(description="Absolute variance amount in reporting currency")
    variance_pct: float = Field(description="Variance as percentage of baseline")
    root_cause: str = Field(description="Identified root cause explanation")
    evidence: list[str] = Field(description="List of supporting evidence statements")
    affected_tables: list[str] = Field(description="Tables in the lineage involved")
    affected_dimensions: dict[str, Any] = Field(
        default_factory=dict,
        description="Dimension values identifying the scope (e.g. cost_center, period)",
    )
    confidence: ConfidenceScore
    recommendations: list[str] = Field(
        default_factory=list, description="Suggested corrective actions"
    )
    sql_queries_used: list[str] = Field(
        default_factory=list, description="SQL queries that produced evidence"
    )

    @field_validator(
        "evidence",
        "affected_tables",
        "recommendations",
        "sql_queries_used",
        mode="before",
    )
    @classmethod
    def _normalize_list_fields(cls, value: Any) -> list[str]:
        return _coerce_str_list(value)

    @field_validator("affected_dimensions", mode="before")
    @classmethod
    def _normalize_affected_dimensions(cls, value: Any) -> dict[str, Any]:
        return _coerce_dimensions(value)


# --- Report ---


class ReportSection(BaseModel):
    """A section of the variance analysis report."""

    title: str
    content: str
    findings: list[str] = Field(
        default_factory=list, description="Finding IDs referenced in this section"
    )

    @field_validator("findings", mode="before")
    @classmethod
    def _normalize_findings(cls, value: Any) -> list[str]:
        return _coerce_str_list(value)


class ExecutionMetadata(BaseModel):
    """Metadata about the analysis execution."""

    started_at: datetime | None = None
    completed_at: datetime | None = None
    model_name: str = ""
    total_queries: int = 0
    total_tokens: int = 0
    run_dir: str = ""


class VarianceReport(BaseModel):
    """Complete variance analysis report — used as agent response_format."""

    title: str = Field(default="Variance Analysis Report")
    executive_summary: str = Field(description="High-level summary of findings")
    sections: list[ReportSection] = Field(default_factory=list)
    findings: list[Finding] = Field(default_factory=list)
    metadata: ExecutionMetadata = Field(default_factory=ExecutionMetadata)

    @field_validator("sections", "findings", mode="before")
    @classmethod
    def _normalize_collection_fields(cls, value: Any) -> Any:
        if isinstance(value, str):
            parsed = _parse_stringified(value)
            if isinstance(parsed, list):
                return parsed
            return []
        return value

    @field_validator("metadata", mode="before")
    @classmethod
    def _normalize_metadata(cls, value: Any) -> Any:
        if isinstance(value, str):
            parsed = _parse_stringified(value)
            if isinstance(parsed, dict):
                return parsed
            return {}
        return value


# --- Query Result ---


class QueryResult(BaseModel):
    """Result from a SQL query execution."""

    sql: str
    columns: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0
    truncated: bool = False
    execution_time_ms: float = 0.0
    error: str | None = None
