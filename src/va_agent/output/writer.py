"""Report writer — produces all output artifacts from an analysis run."""

from __future__ import annotations

import json
from pathlib import Path

from va_agent.config import Settings
from va_agent.models import VarianceReport


class ReportWriter:
    """Writes analysis artifacts to a run directory.

    Artifacts produced:
    - report.json: Full structured report
    - report.md: Human-readable markdown report
    - findings.json: Findings array only
    - executed_queries.sql: All SQL queries from the audit log
    - run_log.json: Execution metadata and audit trail
    """

    def __init__(self, settings: Settings):
        self.settings = settings

    def _resolve_run_dir(self, run_dir: Path | None = None) -> Path:
        """Resolve the run directory from explicit path or latest pointer."""
        if run_dir:
            return run_dir

        # Prefer deep-run pointer first (deep-only runtime), then legacy pointer.
        pointer_files = [
            self.settings.runs_dir / "spikes" / "deep" / "latest_run",
            self.settings.runs_dir / "latest_run",
        ]
        for pointer_file in pointer_files:
            if not pointer_file.exists():
                continue
            candidate = Path(pointer_file.read_text(encoding="utf-8").strip())
            if candidate.exists() and (candidate / "report.json").exists():
                return candidate

        # Fall back to latest run directory with report.json.
        candidates: list[Path] = []
        for d in self.settings.runs_dir.iterdir():
            if d.is_dir() and (d / "report.json").exists():
                candidates.append(d)

        deep_root = self.settings.runs_dir / "spikes" / "deep"
        if deep_root.exists():
            for d in deep_root.iterdir():
                if d.is_dir() and (d / "report.json").exists():
                    candidates.append(d)

        if candidates:
            return max(candidates, key=lambda p: p.stat().st_mtime)

        raise FileNotFoundError("No analysis runs found. Run 'va analyze' first.")

    def _load_report(self, run_dir: Path) -> VarianceReport:
        """Load the report from a run directory."""
        report_path = run_dir / "report.json"
        if not report_path.exists():
            raise FileNotFoundError(f"No report.json in {run_dir}")
        return VarianceReport.model_validate_json(report_path.read_text(encoding="utf-8"))

    def _load_audit_log(self, run_dir: Path) -> list[dict]:
        """Load the audit log from a run directory."""
        audit_path = run_dir / "audit_log.json"
        if audit_path.exists():
            return json.loads(audit_path.read_text(encoding="utf-8"))
        return []

    def write_all(self, run_dir: Path | None = None) -> dict[str, Path]:
        """Write all output artifacts.

        Args:
            run_dir: Explicit run directory, or None for latest.

        Returns:
            Dict mapping artifact name to file path.
        """
        run_dir = self._resolve_run_dir(run_dir)
        report = self._load_report(run_dir)
        audit_log = self._load_audit_log(run_dir)

        artifacts = {}

        # report.json already exists from the analysis run
        artifacts["report.json"] = run_dir / "report.json"

        # report.md
        md_path = run_dir / "report.md"
        md_path.write_text(self._render_markdown(report), encoding="utf-8")
        artifacts["report.md"] = md_path

        # findings.json
        findings_path = run_dir / "findings.json"
        findings_data = [f.model_dump() for f in report.findings]
        findings_path.write_text(
            json.dumps(findings_data, indent=2, default=str), encoding="utf-8"
        )
        artifacts["findings.json"] = findings_path

        # executed_queries.sql
        sql_path = run_dir / "executed_queries.sql"
        queries = [
            f"-- Query #{i+1} ({entry.get('execution_time_ms', 0):.0f}ms, "
            f"{entry.get('row_count', 0)} rows)\n{entry.get('sql', '')}\n"
            for i, entry in enumerate(audit_log)
        ]
        sql_path.write_text("\n".join(queries), encoding="utf-8")
        artifacts["executed_queries.sql"] = sql_path

        # run_log.json
        log_path = run_dir / "run_log.json"
        log_data = {
            "metadata": report.metadata.model_dump(mode="json"),
            "findings_count": len(report.findings),
            "sections_count": len(report.sections),
            "queries_count": len(audit_log),
            "audit_log": audit_log,
        }
        log_path.write_text(
            json.dumps(log_data, indent=2, default=str), encoding="utf-8"
        )
        artifacts["run_log.json"] = log_path

        return artifacts

    def get_audit_log(self, run_dir: Path | None = None) -> list[dict]:
        """Get the audit log for display."""
        run_dir = self._resolve_run_dir(run_dir)
        return self._load_audit_log(run_dir)

    def _render_markdown(self, report: VarianceReport) -> str:
        """Render the report as markdown."""
        lines = [f"# {report.title}", ""]

        # Executive Summary
        lines.extend(["## Executive Summary", "", report.executive_summary, ""])

        # Sections
        for section in report.sections:
            lines.extend([f"## {section.title}", "", section.content, ""])
            if section.findings:
                lines.append(f"*Related findings: {', '.join(section.findings)}*")
                lines.append("")

        # Findings
        if report.findings:
            lines.extend(["## Detailed Findings", ""])
            for finding in report.findings:
                lines.extend(
                    [
                        f"### {finding.id}: {finding.title}",
                        "",
                        f"**Category:** {finding.category.value}",
                        f"**Direction:** {finding.direction.value}",
                        f"**Variance:** ${finding.variance_amount:,.2f} ({finding.variance_pct:+.1f}%)",
                        f"**Confidence:** {finding.confidence.level.value} ({finding.confidence.score:.1%})",
                        "",
                        f"**Root Cause:** {finding.root_cause}",
                        "",
                        "**Evidence:**",
                    ]
                )
                for ev in finding.evidence:
                    lines.append(f"- {ev}")
                lines.append("")

                if finding.recommendations:
                    lines.append("**Recommendations:**")
                    for rec in finding.recommendations:
                        lines.append(f"- {rec}")
                    lines.append("")

                lines.append("---")
                lines.append("")

        # Metadata
        if report.metadata:
            lines.extend(
                [
                    "## Execution Metadata",
                    "",
                    f"- **Model:** {report.metadata.model_name}",
                    f"- **Total Queries:** {report.metadata.total_queries}",
                    f"- **Started:** {report.metadata.started_at}",
                    f"- **Completed:** {report.metadata.completed_at}",
                    "",
                ]
            )

        return "\n".join(lines)
