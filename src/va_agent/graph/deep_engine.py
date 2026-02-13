"""Deep Agents runtime and benchmark orchestration."""

from __future__ import annotations

import json
import re
import statistics
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from rich.console import Console

from va_agent.config import Settings
from va_agent.models import (
    ConfidenceLevel,
    ExecutionMetadata,
    Finding,
    VarianceDirection,
    VarianceReport,
)
from va_agent.output.writer import ReportWriter
from va_agent.sql.executor import SQLExecutor
from va_agent.tools.bound_tools import create_bound_tools
from va_agent.tools.run_context import RunContext

_console = Console(stderr=True)
_DEFAULT_DEEP_REQUEST_TIMEOUT_S = 45.0
_DEFAULT_DEEP_RETRIES = 2
_DEFAULT_DEEP_RECURSION_LIMIT = 40
_MAX_SPIKE_TOOL_CALLS_HINT = 10

_ANOMALY_RULES = {
    "A-001": {
        "category": "COGS_ANOMALY",
        "all_tokens": ["cc-300"],
        "any_tokens": ["2024-03", "2024-04"],
        "description": "COGS spike in CC-300 (2024-03/2024-04)",
    },
    "A-002": {
        "category": "REVENUE_ANOMALY",
        "all_tokens": ["sales", "2024-06"],
        "description": "Sales revenue zeroed in 2024-06",
    },
    "A-003": {
        "category": "FX_ANOMALY",
        "all_tokens": ["eur", "2024-07"],
        "description": "EUR FX rate anomaly in 2024-07",
    },
    "A-004": {
        "category": "BUDGET_MISALIGNMENT",
        "all_tokens": ["finance"],
        "any_tokens": ["2024-07", "2024-08", "2024-09", "q3"],
        "description": "Finance budget misalignment in Q3 2024",
    },
    "A-005": {
        "category": "CLASSIFICATION_ERROR",
        "any_tokens": ["misclass", "classification", "mapping", "account_type"],
        "description": "Revenue/OpEx classification mismatch",
    },
}


def _load_spike_prompt() -> str:
    prompts_dir = Path(__file__).parent.parent / "prompts"
    parts: list[str] = []
    for name in ["system.md", "hypothesis.md", "synthesis.md", "deep_spike_agents.md"]:
        path = prompts_dir / name
        if path.exists():
            parts.append(path.read_text(encoding="utf-8"))
    return "\n\n---\n\n".join(parts)


def _vlog(settings: Settings, message: str) -> None:
    if settings.verbose:
        _console.print(f"[dim]{message}[/dim]")


def _slugify(text: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "-", text).strip("-").lower()
    return slug[:32]


def _now_iso() -> str:
    return datetime.now().isoformat()


def _make_run_dir(settings: Settings, run_label: str | None = None) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = ""
    if run_label:
        cleaned = _slugify(run_label)
        if cleaned:
            suffix = f"_{cleaned}"
    run_dir = settings.runs_dir / "spikes" / "deep" / f"{timestamp}{suffix}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _coerce_jsonable(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _coerce_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_coerce_jsonable(v) for v in value]
    if hasattr(value, "model_dump"):
        return _coerce_jsonable(value.model_dump())
    if hasattr(value, "dict"):
        return _coerce_jsonable(value.dict())
    if hasattr(value, "__dict__"):
        return _coerce_jsonable(vars(value))
    return str(value)


def _extract_content(message: Any) -> Any:
    if isinstance(message, dict):
        return message.get("content")
    return getattr(message, "content", None)


def _content_to_text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                text_value = item.get("text")
                if text_value:
                    parts.append(str(text_value))
                else:
                    parts.append(str(item))
            else:
                parts.append(str(item))
        return "\n".join(p for p in parts if p)
    return str(content)


def _parse_json_maybe(text: str) -> Any:
    stripped = text.strip()
    if not stripped:
        return text
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return text


def _extract_tool_calls(message: Any) -> list[dict[str, Any]]:
    calls = None
    if isinstance(message, dict):
        calls = message.get("tool_calls")
        if not calls:
            addl = message.get("additional_kwargs", {})
            calls = addl.get("tool_calls")
    else:
        calls = getattr(message, "tool_calls", None)
        if not calls:
            addl = getattr(message, "additional_kwargs", {})
            if isinstance(addl, dict):
                calls = addl.get("tool_calls")
    if not calls:
        return []

    normalized: list[dict[str, Any]] = []
    for call in calls:
        if isinstance(call, dict):
            function_block = call.get("function", {})
            name = call.get("name") or function_block.get("name") or "unknown_tool"
            args = call.get("args")
            if args is None:
                args = function_block.get("arguments", {})
            if isinstance(args, str):
                args = _parse_json_maybe(args)
        else:
            name = getattr(call, "name", "unknown_tool")
            args = getattr(call, "args", {})

        if not isinstance(args, dict):
            args = {"value": _coerce_jsonable(args)}
        normalized.append({"name": name, "args": _coerce_jsonable(args)})
    return normalized


def _extract_tool_result(message: Any) -> dict[str, Any]:
    if isinstance(message, dict):
        name = message.get("name") or message.get("tool_name") or "tool"
        content = message.get("content")
    else:
        name = getattr(message, "name", None) or getattr(message, "tool_name", "tool")
        content = getattr(message, "content", None)

    text = _content_to_text(content)
    parsed = _parse_json_maybe(text)
    if isinstance(parsed, dict):
        result = parsed
    else:
        result = {"content": parsed}
    return {"name": name, "result": _coerce_jsonable(result)}


def _message_type(message: Any) -> str:
    if isinstance(message, dict):
        raw = message.get("type") or message.get("role") or "unknown"
    else:
        raw = getattr(message, "type", None) or getattr(message, "role", "unknown")
    return str(raw).lower()


def _extract_messages(agent_result: Any) -> list[Any]:
    if isinstance(agent_result, dict):
        messages = agent_result.get("messages")
        if isinstance(messages, list):
            return messages
    messages = getattr(agent_result, "messages", None)
    if isinstance(messages, list):
        return messages
    return []


def _normalize_trace(messages: list[Any], user_message: str, started_perf: float) -> list[dict]:
    trace: list[dict] = [
        {
            "step": 0,
            "timestamp": _now_iso(),
            "elapsed_s": 0.0,
            "request": user_message,
            "response_text": None,
            "function_calls": None,
            "tool_results": None,
        }
    ]

    step = 0
    last_callable_step: int | None = None

    for message in messages:
        mtype = _message_type(message)
        elapsed = round(time.perf_counter() - started_perf, 2)

        if mtype in {"human", "user"}:
            continue

        if mtype in {"ai", "assistant"}:
            step += 1
            calls = _extract_tool_calls(message)
            entry = {
                "step": step,
                "timestamp": _now_iso(),
                "elapsed_s": elapsed,
                "request": None,
                "response_text": _content_to_text(_extract_content(message)) or None,
                "function_calls": calls or None,
                "tool_results": None,
            }
            trace.append(entry)
            last_callable_step = len(trace) - 1 if calls else None
            continue

        if mtype == "tool":
            if last_callable_step is None:
                step += 1
                trace.append(
                    {
                        "step": step,
                        "timestamp": _now_iso(),
                        "elapsed_s": elapsed,
                        "request": None,
                        "response_text": None,
                        "function_calls": None,
                        "tool_results": [],
                    }
                )
                last_callable_step = len(trace) - 1

            if trace[last_callable_step]["tool_results"] is None:
                trace[last_callable_step]["tool_results"] = []
            trace[last_callable_step]["tool_results"].append(_extract_tool_result(message))
            continue

        step += 1
        trace.append(
            {
                "step": step,
                "timestamp": _now_iso(),
                "elapsed_s": elapsed,
                "request": None,
                "response_text": _content_to_text(_extract_content(message)) or str(message),
                "function_calls": None,
                "tool_results": None,
            }
        )

    return trace


def _trace_metrics(trace_payload: dict[str, Any]) -> dict[str, Any]:
    steps = trace_payload.get("steps", [])
    tool_calls = 0
    tool_errors = 0
    for step in steps:
        for call in step.get("function_calls") or []:
            if call:
                tool_calls += 1
        for tool_result in step.get("tool_results") or []:
            result = tool_result.get("result")
            if isinstance(result, dict) and result.get("error"):
                tool_errors += 1
    return {
        "steps": len(steps),
        "tool_calls": tool_calls,
        "tool_errors": tool_errors,
    }


def _normalize_text(text: str) -> str:
    """Normalize text for fuzzy token matching.

    Strips hyphens/underscores, lowercases, and collapses whitespace
    so that 'CC-300', 'cc_300', 'cc300', 'cost center 300' all match.
    """
    lowered = text.lower()
    # Replace hyphens and underscores with spaces for matching
    normalized = lowered.replace("-", " ").replace("_", " ")
    # Collapse multiple spaces
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def _finding_blob(finding: Finding) -> str:
    parts: list[str] = [
        finding.id,
        finding.title,
        finding.root_cause,
        finding.category.value,
    ]
    parts.extend(finding.evidence)
    parts.extend(finding.affected_tables)
    for key, value in finding.affected_dimensions.items():
        parts.append(str(key))
        parts.append(str(value))
    raw = " ".join(parts)
    return _normalize_text(raw)


def _matches_anomaly(finding: Finding, rule: dict[str, Any]) -> bool:
    if finding.category.value != rule["category"]:
        return False

    blob = _finding_blob(finding)

    for token in rule.get("all_tokens", []):
        normalized_token = _normalize_text(token)
        if normalized_token not in blob:
            return False

    any_tokens = rule.get("any_tokens")
    if any_tokens and not any(_normalize_text(token) in blob for token in any_tokens):
        return False

    return True


def evaluate_findings(findings: list[Finding]) -> dict[str, Any]:
    matched_by_anomaly: dict[str, list[str]] = {}
    matched_findings: set[str] = set()

    for anomaly_id, rule in _ANOMALY_RULES.items():
        hits: list[str] = []
        for finding in findings:
            if _matches_anomaly(finding, rule):
                hits.append(finding.id)
                matched_findings.add(finding.id)
        if hits:
            matched_by_anomaly[anomaly_id] = sorted(set(hits))

    total_anomalies = len(_ANOMALY_RULES)
    recall = len(matched_by_anomaly) / total_anomalies if total_anomalies else 0.0
    precision_proxy = len(matched_findings) / len(findings) if findings else 0.0

    high_conf = [f for f in findings if f.confidence.level == ConfidenceLevel.HIGH]
    high_conf_with_enough_evidence = [f for f in high_conf if len(f.evidence) >= 3]
    evidence_sufficiency = (
        len(high_conf_with_enough_evidence) / len(high_conf) if high_conf else 1.0
    )

    depth_hits = [
        f for f in findings if any(table != "mart_pnl_report" for table in f.affected_tables)
    ]
    root_cause_depth = len(depth_hits) / len(findings) if findings else 0.0

    unmatched_anomalies = sorted(set(_ANOMALY_RULES) - set(matched_by_anomaly))
    unmatched_findings = sorted({f.id for f in findings} - matched_findings)

    return {
        "anomaly_catalog": _coerce_jsonable(_ANOMALY_RULES),
        "matches": matched_by_anomaly,
        "matched_anomalies": len(matched_by_anomaly),
        "unmatched_anomalies": unmatched_anomalies,
        "matched_findings": sorted(matched_findings),
        "unmatched_findings": unmatched_findings,
        "metrics": {
            "anomaly_recall": round(recall, 4),
            "precision_proxy": round(precision_proxy, 4),
            "evidence_sufficiency": round(evidence_sufficiency, 4),
            "root_cause_depth": round(root_cause_depth, 4),
            "findings_count": len(findings),
        },
    }


def compute_consistency(evaluations: list[dict[str, Any]]) -> dict[str, float]:
    if not evaluations:
        return {"recall_stddev": 0.0, "precision_stddev": 0.0}

    recalls = [e["metrics"]["anomaly_recall"] for e in evaluations]
    precisions = [e["metrics"]["precision_proxy"] for e in evaluations]

    return {
        "recall_stddev": round(statistics.pstdev(recalls), 6) if len(recalls) > 1 else 0.0,
        "precision_stddev": (
            round(statistics.pstdev(precisions), 6) if len(precisions) > 1 else 0.0
        ),
    }


def _deep_summary_from_evals(evaluations: list[dict[str, Any]]) -> dict[str, Any]:
    if not evaluations:
        return {
            "runs": 0,
            "avg_metrics": {
                "anomaly_recall": 0.0,
                "precision_proxy": 0.0,
                "evidence_sufficiency": 0.0,
                "root_cause_depth": 0.0,
            },
            "consistency": {"recall_stddev": 0.0, "precision_stddev": 0.0},
            "trace_metrics": {"steps": 0.0, "tool_calls": 0.0, "tool_errors": 0.0},
            "run_dirs": [],
            "unmatched_anomalies_union": sorted(_ANOMALY_RULES),
        }

    metric_keys = ("anomaly_recall", "precision_proxy", "evidence_sufficiency", "root_cause_depth")
    avg_metrics = {
        key: round(sum(e["metrics"][key] for e in evaluations) / len(evaluations), 4)
        for key in metric_keys
    }

    trace_keys = ("steps", "tool_calls", "tool_errors")
    trace_metrics = {
        key: round(
            sum(e.get("trace_metrics", {}).get(key, 0.0) for e in evaluations) / len(evaluations),
            2,
        )
        for key in trace_keys
    }

    unmatched_union = sorted(
        {
            anomaly
            for evaluation in evaluations
            for anomaly in evaluation.get("unmatched_anomalies", [])
        }
    )

    return {
        "runs": len(evaluations),
        "avg_metrics": avg_metrics,
        "consistency": compute_consistency(evaluations),
        "trace_metrics": trace_metrics,
        "run_dirs": [e["run"]["run_dir"] for e in evaluations if e.get("run")],
        "unmatched_anomalies_union": unmatched_union,
    }


def _build_comparison_markdown(deep_summary: dict[str, Any]) -> str:
    lines: list[str] = [
        "# Deep Spike Benchmark Summary",
        "",
        "This summary is generated from measured deep-run artifacts, not manual judgment.",
        "",
        "## Scope",
        "",
        "- Deep path only: standalone spike runs in `runs/spikes/deep/...`",
        "",
    ]

    deep_metrics = deep_summary.get("avg_metrics", {})
    deep_trace = deep_summary.get("trace_metrics", {})
    lines.extend(
        [
            "## Detection Quality",
            "",
            f"- Deep anomaly recall: {deep_metrics.get('anomaly_recall', 0.0):.2%}",
            f"- Deep precision proxy: {deep_metrics.get('precision_proxy', 0.0):.2%}",
            f"- Deep evidence sufficiency: {deep_metrics.get('evidence_sufficiency', 0.0):.2%}",
            f"- Deep root-cause depth: {deep_metrics.get('root_cause_depth', 0.0):.2%}",
            f"- Deep run count: {deep_summary.get('runs', 0)}",
            "",
            "## Consistency",
            "",
            f"- Recall stddev: {deep_summary.get('consistency', {}).get('recall_stddev', 0.0):.6f}",
            (
                f"- Precision stddev: "
                f"{deep_summary.get('consistency', {}).get('precision_stddev', 0.0):.6f}"
            ),
            "",
            "## Trace Observability",
            "",
            f"- Deep avg steps: {deep_trace.get('steps', 0.0)}",
            f"- Deep avg tool calls: {deep_trace.get('tool_calls', 0.0)}",
            f"- Deep avg tool errors: {deep_trace.get('tool_errors', 0.0)}",
            "",
        ]
    )

    unmatched = deep_summary.get("unmatched_anomalies_union", [])
    lines.extend(["## Deep Failure Cases", ""])
    if unmatched:
        for anomaly in unmatched:
            desc = _ANOMALY_RULES.get(anomaly, {}).get("description", "Unknown anomaly")
            lines.append(f"- {anomaly}: {desc}")
    else:
        lines.append("- None detected in benchmark runs.")
    lines.append("")

    lines.extend(
        [
            "## Recommendation Inputs",
            "",
            ("- Tune prompt/tool limits if anomaly recall is low or if tool error rate is high."),
            "- Track recall and precision proxy across repeated runs before changing defaults.",
            "- Investigate unmatched anomalies in `evaluation.json` and `trace.json`.",
            "",
        ]
    )
    return "\n".join(lines)


def _ensure_minimum_finding(tools: dict[str, Any]) -> None:
    run_sql_query = tools["run_sql_query"]
    write_finding = tools["write_finding"]

    fallback_sql = (
        "SELECT department, account_type, period, variance_usd, variance_pct "
        "FROM mart_pnl_report ORDER BY ABS(variance_usd) DESC LIMIT 1"
    )
    query_result = run_sql_query(fallback_sql)
    rows = query_result.get("rows", [])
    if not rows:
        return

    row = rows[0]
    variance = float(row.get("variance_usd", 0.0))
    direction = (
        VarianceDirection.UNFAVORABLE.value if variance >= 0 else VarianceDirection.FAVORABLE.value
    )
    period = row.get("period", "unknown")
    department = row.get("department", "unknown")
    account_type = row.get("account_type", "unknown")

    write_finding(
        title=f"Fallback finding for {department} {account_type} ({period})",
        category="OTHER",
        direction=direction,
        variance_amount=abs(variance),
        variance_pct=float(row.get("variance_pct", 0.0)),
        root_cause=(
            "Deep spike fallback: largest material variance captured to ensure a valid finding "
            "artifact when tool-calling synthesis is incomplete."
        ),
        evidence=[
            f"Largest mart variance row selected for {department}/{account_type}/{period}.",
            f"Observed variance_usd={variance:.2f}.",
            "This fallback should be replaced by agent-authored findings in stable runs.",
        ],
        affected_tables=["mart_pnl_report"],
        affected_dimensions={
            "department": department,
            "account_type": account_type,
            "period": period,
        },
        recommendations=[
            "Rerun the deep spike and inspect trace.json for missing write_finding tool calls.",
        ],
        sql_queries_used=[fallback_sql],
        confidence_evidence_breadth=0.4,
        confidence_lineage_depth=0.2,
        confidence_variance_explanation=0.5,
        confidence_hypothesis_exclusion=0.2,
        confidence_data_quality=0.7,
        confidence_temporal_consistency=0.3,
    )


def _ensure_minimum_section(ctx: RunContext, tools: dict[str, Any]) -> None:
    if ctx.sections:
        return
    finding_ids = [finding.id for finding in ctx.findings]
    tools["write_report_section"](
        title="Deep Spike Summary",
        content=(
            "Standalone Deep Agents spike execution complete. "
            "Use evaluation.json and comparison.md for decision support."
        ),
        finding_ids=finding_ids,
    )


def _build_report(
    settings: Settings, ctx: RunContext, started_at: datetime, run_dir: Path
) -> VarianceReport:
    completed_at = datetime.now()
    findings = ctx.findings
    sections = ctx.sections

    if findings:
        bullets = [f"- {finding.title} ({finding.confidence.level.value})" for finding in findings]
        executive_summary = f"Deep spike identified {len(findings)} finding(s):\n" + "\n".join(
            bullets
        )
    else:
        executive_summary = "Deep spike completed with no findings."

    return VarianceReport(
        title="Variance Analysis Report (Deep Spike)",
        executive_summary=executive_summary,
        sections=sections,
        findings=findings,
        metadata=ExecutionMetadata(
            started_at=started_at,
            completed_at=completed_at,
            model_name=f"deep:{settings.model_name}",
            total_queries=len(ctx.executor.audit_log),
            run_dir=str(run_dir),
        ),
    )


def _create_deep_agent(
    settings: Settings, system_prompt: str, tools: list[Any]
) -> Any:
    try:
        from deepagents import create_deep_agent
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency: deepagents. Install dependencies from requirements.txt."
        ) from exc

    try:
        from langchain_google_genai import ChatGoogleGenerativeAI
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency: langchain-google-genai. Install requirements.txt."
        ) from exc

    model = ChatGoogleGenerativeAI(
        model=settings.model_name,
        temperature=settings.temperature,
        request_timeout=_DEFAULT_DEEP_REQUEST_TIMEOUT_S,
        retries=_DEFAULT_DEEP_RETRIES,
    )
    kwargs = {"model": model, "tools": tools}

    # Compatibility shim for Deep Agents versions with different prompt kwarg names.
    # Raises instead of silently falling through to no-prompt creation.
    last_exc: TypeError | None = None
    for prompt_key in ("system_prompt", "instructions"):
        try:
            return create_deep_agent(**kwargs, **{prompt_key: system_prompt})
        except TypeError as exc:
            last_exc = exc
            continue

    raise RuntimeError(
        "Failed to pass system prompt to create_deep_agent — "
        "neither 'system_prompt' nor 'instructions' kwarg accepted. "
        f"Last error: {last_exc}"
    )


def _write_artifacts(
    settings: Settings,
    report: VarianceReport,
    run_dir: Path,
    executor: SQLExecutor,
    trace_payload: dict[str, Any],
) -> dict[str, Any]:
    _vlog(settings, f"Writing artifacts to {run_dir} ...")
    report_path = run_dir / "report.json"
    report_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")

    writer = ReportWriter(settings)
    md_path = run_dir / "report.md"
    md_path.write_text(writer._render_markdown(report), encoding="utf-8")

    audit_log = executor.get_audit_entries()
    audit_path = run_dir / "audit_log.json"
    audit_path.write_text(json.dumps(audit_log, indent=2, default=str), encoding="utf-8")

    sql_path = run_dir / "executed_queries.sql"
    queries = [
        f"-- Query #{idx + 1} ({entry.get('execution_time_ms', 0):.0f}ms, "
        f"{entry.get('row_count', 0)} rows)\n{entry.get('sql', '')}\n"
        for idx, entry in enumerate(audit_log)
    ]
    sql_path.write_text("\n".join(queries), encoding="utf-8")

    findings_path = run_dir / "findings.json"
    findings_path.write_text(
        json.dumps([finding.model_dump() for finding in report.findings], indent=2, default=str),
        encoding="utf-8",
    )

    trace_path = run_dir / "trace.json"
    trace_path.write_text(json.dumps(trace_payload, indent=2, default=str), encoding="utf-8")

    evaluation = evaluate_findings(report.findings)
    evaluation["trace_metrics"] = _trace_metrics(trace_payload)
    evaluation["run"] = {
        "run_dir": str(run_dir.resolve()),
        "engine": "deep",
        "model": settings.model_name,
        "started_at": str(report.metadata.started_at),
        "completed_at": str(report.metadata.completed_at),
    }

    eval_path = run_dir / "evaluation.json"
    eval_path.write_text(json.dumps(evaluation, indent=2, default=str), encoding="utf-8")

    deep_summary = _deep_summary_from_evals([evaluation])
    comparison_md = _build_comparison_markdown(deep_summary=deep_summary)
    comparison_path = run_dir / "comparison.md"
    comparison_path.write_text(comparison_md, encoding="utf-8")

    latest_pointer = settings.runs_dir / "spikes" / "deep" / "latest_run"
    latest_pointer.parent.mkdir(parents=True, exist_ok=True)
    latest_pointer.write_text(str(run_dir.resolve()), encoding="utf-8")
    _vlog(settings, "Artifacts written: report, findings, trace, evaluation, comparison")

    return evaluation


def run_deep_spike(settings: Settings, run_label: str | None = None) -> VarianceReport:
    """Run one standalone Deep Agents spike and write artifacts."""
    if not settings.db_path.exists():
        raise FileNotFoundError(f"Database not found at {settings.db_path}. Run `va seed` first.")

    settings.ensure_dirs()
    run_dir = _make_run_dir(settings, run_label)
    started_at = datetime.now()
    _vlog(settings, f"Deep spike run dir: {run_dir}")
    _vlog(settings, f"Model={settings.model_name} temp={settings.temperature}")

    executor = SQLExecutor(
        db_path=settings.db_path,
        max_rows=settings.max_rows,
        query_timeout=settings.query_timeout,
    )
    ctx = RunContext(executor=executor)
    bound = create_bound_tools(ctx)
    _vlog(settings, f"SQL executor wired to {settings.db_path}")

    system_prompt = _load_spike_prompt()
    user_message = (
        "Deep spike task (strict): produce EXACTLY one finding, then stop.\n"
        "Execution plan:\n"
        "1) Call get_all_tables() once.\n"
        "2) Call run_sql_template('variance_summary') once.\n"
        "3) Run up to 3 additional SQL queries to validate one material anomaly.\n"
        "4) Call write_finding() exactly once.\n"
        "5) Call write_report_section() exactly once.\n"
        "6) Return a final short text summary and STOP.\n"
        f"Hard limit: no more than {_MAX_SPIKE_TOOL_CALLS_HINT} total tool calls.\n"
        "Constraints: do not use PRAGMA; use SELECT-only SQL with known columns; "
        "if a query fails, correct it once and continue."
    )

    agent_tools = [
        bound["run_sql_query"],
        bound["run_sql_template"],
        bound["get_table_schema"],
        bound["get_all_tables"],
        bound["get_table_lineage"],
        bound["write_finding"],
        bound["write_report_section"],
    ]

    started_perf = time.perf_counter()
    error_text: str | None = None

    try:
        _vlog(settings, "Creating Deep Agents runtime...")
        agent = _create_deep_agent(
            settings=settings, system_prompt=system_prompt, tools=agent_tools
        )
        _vlog(
            settings,
            (
                "Invoking deep agent "
                f"(request_timeout={_DEFAULT_DEEP_REQUEST_TIMEOUT_S}s, "
                f"retries={_DEFAULT_DEEP_RETRIES}, "
                f"recursion_limit={_DEFAULT_DEEP_RECURSION_LIMIT})..."
            ),
        )
        result = agent.invoke(
            {"messages": [{"role": "user", "content": user_message}]},
            config={
                "configurable": {"thread_id": f"deep-spike-{int(time.time())}"},
                "recursion_limit": _DEFAULT_DEEP_RECURSION_LIMIT,
            },
        )
        messages = _extract_messages(result)
        trace_steps = _normalize_trace(messages, user_message, started_perf)
        metrics = _trace_metrics({"steps": trace_steps})
        _vlog(
            settings,
            (
                "Deep agent completed: "
                f"{metrics['steps']} trace steps, "
                f"{metrics['tool_calls']} tool calls, "
                f"{metrics['tool_errors']} tool errors"
            ),
        )
    except Exception as exc:
        error_text = str(exc)
        _vlog(settings, f"Deep agent failed; fallback path engaged: {error_text}")
        trace_steps = [
            {
                "step": 0,
                "timestamp": _now_iso(),
                "elapsed_s": 0.0,
                "request": user_message,
                "response_text": None,
                "function_calls": None,
                "tool_results": None,
            },
            {
                "step": 1,
                "timestamp": _now_iso(),
                "elapsed_s": round(time.perf_counter() - started_perf, 2),
                "request": None,
                "response_text": f"Deep spike execution failed: {exc}",
                "function_calls": None,
                "tool_results": None,
            },
        ]

    try:
        if not ctx.findings:
            _ensure_minimum_finding(bound)
        _ensure_minimum_section(ctx, bound)
        _vlog(settings, f"Findings collected: {len(ctx.findings)}")

        report = _build_report(
            settings=settings, ctx=ctx, started_at=started_at, run_dir=run_dir
        )
        trace_payload = {
            "engine": "deep",
            "model": settings.model_name,
            "temperature": settings.temperature,
            "system_instruction": system_prompt,
            "user_message": user_message,
            "error": error_text,
            "steps": _coerce_jsonable(trace_steps),
        }
        evaluation = _write_artifacts(
            settings=settings,
            report=report,
            run_dir=run_dir,
            executor=executor,
            trace_payload=trace_payload,
        )
    finally:
        executor.close()

    _vlog(settings, "Deep spike finished.")
    return report


def run_deep_benchmark(
    settings: Settings,
    repeats: int = 3,
    run_label: str | None = None,
) -> dict[str, Any]:
    """Run repeated deep spikes and write benchmark summary artifacts."""
    if repeats < 1:
        raise ValueError("repeats must be >= 1")

    original_temp = settings.temperature
    settings.temperature = 0.0
    _vlog(settings, f"Starting deep benchmark: repeats={repeats}, deterministic temp=0.0")

    benchmark_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    benchmark_dir = settings.runs_dir / "spikes" / "deep" / f"benchmark_{benchmark_stamp}"
    benchmark_dir.mkdir(parents=True, exist_ok=True)

    evaluations: list[dict[str, Any]] = []
    reports: list[VarianceReport] = []

    try:
        for idx in range(repeats):
            label_parts = [run_label or "benchmark", f"r{idx + 1:02d}"]
            label = "-".join(part for part in label_parts if part)
            _vlog(settings, f"Benchmark run {idx + 1}/{repeats}: label={label}")
            report = run_deep_spike(settings=settings, run_label=label)
            reports.append(report)

            run_dir = Path(report.metadata.run_dir)
            eval_path = run_dir / "evaluation.json"
            evaluations.append(json.loads(eval_path.read_text(encoding="utf-8")))
    finally:
        settings.temperature = original_temp

    summary = {
        "benchmark_started_at": _now_iso(),
        "repeats": repeats,
        "deep": _deep_summary_from_evals(evaluations),
        "runs": [
            {
                "run_dir": str(report.metadata.run_dir),
                "findings_count": len(report.findings),
                "model": report.metadata.model_name,
            }
            for report in reports
        ],
    }

    summary_path = benchmark_dir / "benchmark_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    comparison_md = _build_comparison_markdown(deep_summary=summary["deep"])
    comparison_path = benchmark_dir / "comparison.md"
    comparison_path.write_text(comparison_md, encoding="utf-8")
    _vlog(settings, f"Benchmark summary written to {summary_path}")
    _vlog(settings, f"Benchmark comparison written to {comparison_path}")

    return {
        "benchmark_dir": str(benchmark_dir),
        "summary_path": str(summary_path),
        "comparison_path": str(comparison_path),
    }


