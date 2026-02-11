"""Auxiliary state types for the variance analysis agent graph."""

from __future__ import annotations

from typing import Any, TypedDict


class AnalysisState(TypedDict, total=False):
    """Extended state tracked during an analysis run.

    This augments the LangGraph message state with analysis-specific fields.
    """

    # Phases completed
    orientation_done: bool
    hypothesis_count: int
    findings_count: int

    # Current investigation context
    current_table: str | None
    current_hypothesis: str | None

    # Accumulated results
    investigated_variances: list[dict[str, Any]]
    ruled_out_hypotheses: list[str]
