"""JSON Schema export and validation helpers for output models."""

from __future__ import annotations

import json
from typing import Any

from va_agent.models import Finding, VarianceReport


def get_report_schema() -> dict[str, Any]:
    """Export the VarianceReport JSON Schema."""
    return VarianceReport.model_json_schema()


def get_finding_schema() -> dict[str, Any]:
    """Export the Finding JSON Schema."""
    return Finding.model_json_schema()


def validate_report_json(data: dict | str) -> VarianceReport:
    """Validate and parse a JSON object or string as a VarianceReport.

    Args:
        data: Dict or JSON string to validate.

    Returns:
        Validated VarianceReport instance.

    Raises:
        pydantic.ValidationError: If validation fails.
    """
    if isinstance(data, str):
        return VarianceReport.model_validate_json(data)
    return VarianceReport.model_validate(data)


def export_schemas(output_dir: str | None = None) -> dict[str, str]:
    """Export all JSON schemas to files or return as dict.

    Args:
        output_dir: If provided, write schema files here.

    Returns:
        Dict mapping schema name to JSON string.
    """
    schemas = {
        "variance_report.schema.json": json.dumps(get_report_schema(), indent=2),
        "finding.schema.json": json.dumps(get_finding_schema(), indent=2),
    }

    if output_dir:
        from pathlib import Path

        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        for name, content in schemas.items():
            (out / name).write_text(content, encoding="utf-8")

    return schemas
