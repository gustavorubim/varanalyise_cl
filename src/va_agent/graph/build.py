"""Build and run the agent for variance analysis.

Uses google-genai for LLM orchestration with function calling.
"""

from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path

from google import genai
from google.genai import types
from rich.console import Console
from rich.panel import Panel

from va_agent.config import Settings
from va_agent.models import ExecutionMetadata, VarianceReport
from va_agent.sql.executor import SQLExecutor
from va_agent.tools.lineage_tools import get_all_tables, get_table_lineage
from va_agent.tools.report_tools import (
    get_findings,
    get_sections,
    reset_state,
    write_finding,
    write_report_section,
)
from va_agent.tools.sql_tools import (
    get_table_schema,
    run_sql_query,
    run_sql_template,
    set_executor,
)

_console = Console(stderr=True)
_LLM_TIMEOUT = 120  # seconds


def _load_prompts() -> str:
    """Load and combine all prompt files."""
    prompts_dir = Path(__file__).parent.parent / "prompts"
    parts = []
    for name in ["system.md", "hypothesis.md", "synthesis.md"]:
        path = prompts_dir / name
        if path.exists():
            parts.append(path.read_text(encoding="utf-8"))
    return "\n\n---\n\n".join(parts)


# ---------------------------------------------------------------------------
# Function declarations for Gemini function calling
# ---------------------------------------------------------------------------

_TOOL_DECLARATIONS = [
    types.FunctionDeclaration(
        name="run_sql_query",
        description=(
            "Execute a read-only SQL SELECT query against the warehouse database. "
            "Only SELECT statements are allowed (and WITH/CTE). No DDL/DML. "
            "Results limited to 500 rows."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "sql": types.Schema(
                    type=types.Type.STRING,
                    description="A valid SQL SELECT query string.",
                ),
            },
            required=["sql"],
        ),
    ),
    types.FunctionDeclaration(
        name="run_sql_template",
        description=(
            "Execute a pre-built SQL template for common analysis patterns. "
            "Available templates: variance_summary(period?), "
            "account_detail(account_code, period?), fx_rate_history(currency?), "
            "cost_center_drill(cost_center, period?), budget_vs_actual(department?), "
            "period_over_period(table?), classification_check()."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "template_name": types.Schema(
                    type=types.Type.STRING,
                    description="Name of the template to execute.",
                ),
                "period": types.Schema(
                    type=types.Type.STRING,
                    description="Period filter (e.g., '2024-03').",
                ),
                "account_code": types.Schema(
                    type=types.Type.STRING,
                    description="Account code filter.",
                ),
                "currency": types.Schema(
                    type=types.Type.STRING,
                    description="Currency filter.",
                ),
                "cost_center": types.Schema(
                    type=types.Type.STRING,
                    description="Cost center filter.",
                ),
                "department": types.Schema(
                    type=types.Type.STRING,
                    description="Department filter.",
                ),
                "table": types.Schema(
                    type=types.Type.STRING,
                    description="Table name for period_over_period template.",
                ),
            },
            required=["template_name"],
        ),
    ),
    types.FunctionDeclaration(
        name="get_table_schema",
        description=(
            "Get the schema (column definitions) for a database table. "
            "Use this to understand table structure before querying."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "table_name": types.Schema(
                    type=types.Type.STRING,
                    description="Name of the table to inspect.",
                ),
            },
            required=["table_name"],
        ),
    ),
    types.FunctionDeclaration(
        name="get_table_lineage",
        description=(
            "Get the full lineage for a table: upstream sources and downstream "
            "consumers. Use this to understand data flow when tracing a variance "
            "to its root cause."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "table_name": types.Schema(
                    type=types.Type.STRING,
                    description="The table to get lineage for.",
                ),
            },
            required=["table_name"],
        ),
    ),
    types.FunctionDeclaration(
        name="get_all_tables",
        description=(
            "Get a summary of all tables in the warehouse with their lineage "
            "relationships. Use this as your first orientation step to understand "
            "the data model."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={},
        ),
    ),
    types.FunctionDeclaration(
        name="write_finding",
        description=(
            "Record a variance finding with root cause analysis and confidence "
            "scoring. Call when you have identified a specific variance anomaly, "
            "determined its root cause, and have supporting evidence."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "title": types.Schema(
                    type=types.Type.STRING,
                    description="Short descriptive title (e.g., 'COGS spike in Engineering Q1').",
                ),
                "category": types.Schema(
                    type=types.Type.STRING,
                    description=(
                        "One of: COGS_ANOMALY, REVENUE_ANOMALY, FX_ANOMALY, "
                        "BUDGET_MISALIGNMENT, CLASSIFICATION_ERROR, OTHER."
                    ),
                ),
                "direction": types.Schema(
                    type=types.Type.STRING,
                    description="FAVORABLE or UNFAVORABLE.",
                ),
                "variance_amount": types.Schema(
                    type=types.Type.NUMBER,
                    description="Absolute variance in USD.",
                ),
                "variance_pct": types.Schema(
                    type=types.Type.NUMBER,
                    description="Variance as percentage of baseline.",
                ),
                "root_cause": types.Schema(
                    type=types.Type.STRING,
                    description="Clear explanation of the identified root cause.",
                ),
                "evidence": types.Schema(
                    type=types.Type.ARRAY,
                    items=types.Schema(type=types.Type.STRING),
                    description="List of evidence statements supporting the finding.",
                ),
                "affected_tables": types.Schema(
                    type=types.Type.ARRAY,
                    items=types.Schema(type=types.Type.STRING),
                    description="Tables in the lineage involved.",
                ),
                "affected_dimensions": types.Schema(
                    type=types.Type.OBJECT,
                    description=(
                        "Dimension values such as cost_center, period, department, "
                        "currency, account_code."
                    ),
                ),
                "recommendations": types.Schema(
                    type=types.Type.ARRAY,
                    items=types.Schema(type=types.Type.STRING),
                    description="Suggested corrective actions.",
                ),
                "sql_queries_used": types.Schema(
                    type=types.Type.ARRAY,
                    items=types.Schema(type=types.Type.STRING),
                    description="SQL queries that produced evidence.",
                ),
                "confidence_evidence_breadth": types.Schema(
                    type=types.Type.NUMBER,
                    description="Evidence breadth score 0.0-1.0 (how many independent data points).",
                ),
                "confidence_lineage_depth": types.Schema(
                    type=types.Type.NUMBER,
                    description="Lineage depth score 0.0-1.0 (how far upstream traced).",
                ),
                "confidence_variance_explanation": types.Schema(
                    type=types.Type.NUMBER,
                    description="Variance explanation score 0.0-1.0 (how much variance explained).",
                ),
                "confidence_hypothesis_exclusion": types.Schema(
                    type=types.Type.NUMBER,
                    description="Hypothesis exclusion score 0.0-1.0 (alternative hypotheses ruled out).",
                ),
                "confidence_data_quality": types.Schema(
                    type=types.Type.NUMBER,
                    description="Data quality score 0.0-1.0 (quality of underlying data).",
                ),
                "confidence_temporal_consistency": types.Schema(
                    type=types.Type.NUMBER,
                    description="Temporal consistency score 0.0-1.0 (pattern consistency across periods).",
                ),
            },
            required=[
                "title", "category", "direction", "variance_amount",
                "variance_pct", "root_cause", "evidence", "affected_tables",
            ],
        ),
    ),
    types.FunctionDeclaration(
        name="write_report_section",
        description=(
            "Add a section to the variance analysis report. Typical sections: "
            "Executive Summary, Methodology, Key Findings, Detailed Analysis, "
            "Recommendations, Data Quality Notes."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "title": types.Schema(
                    type=types.Type.STRING,
                    description="Section title.",
                ),
                "content": types.Schema(
                    type=types.Type.STRING,
                    description="Section content in markdown format.",
                ),
                "finding_ids": types.Schema(
                    type=types.Type.ARRAY,
                    items=types.Schema(type=types.Type.STRING),
                    description="List of finding IDs referenced in this section.",
                ),
            },
            required=["title", "content"],
        ),
    ),
]


# ---------------------------------------------------------------------------
# Tool dispatch
# ---------------------------------------------------------------------------

def _to_python(value):
    """Convert protobuf/genai map values to plain Python types."""
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    if hasattr(value, "items"):
        return {k: _to_python(v) for k, v in value.items()}
    if hasattr(value, "__iter__"):
        return [_to_python(v) for v in value]
    return value


def _dispatch_tool(name: str, args: dict) -> dict:
    """Execute a tool by name with the given arguments."""
    try:
        if name == "run_sql_query":
            result = run_sql_query(**args)
        elif name == "run_sql_template":
            args = dict(args)  # copy — pop mutates
            template_name = args.pop("template_name")
            result = run_sql_template(template_name, **args)
        elif name == "get_table_schema":
            result = get_table_schema(**args)
        elif name == "get_table_lineage":
            result = get_table_lineage(**args)
        elif name == "get_all_tables":
            result = get_all_tables()
        elif name == "write_finding":
            result = write_finding(**args)
        elif name == "write_report_section":
            result = write_report_section(**args)
        else:
            return {"error": f"Unknown tool: {name}"}
    except Exception as e:
        return {"error": str(e)}

    if not isinstance(result, dict):
        return {"result": str(result)}
    return result


# ---------------------------------------------------------------------------
# LLM helpers
# ---------------------------------------------------------------------------

def _ping_llm(model_name: str) -> None:
    """Quick smoke-test: send a tiny request to verify the API key and model work."""
    client = genai.Client()
    try:
        client.models.generate_content(
            model=model_name, contents="Reply with the single word OK."
        )
        _console.print(f"[green]LLM connectivity OK[/green] [dim]({model_name})[/dim]")
    except Exception as e:
        raise RuntimeError(
            f"LLM connectivity check failed for {model_name}: {e}\n"
            "Verify your GOOGLE_API_KEY and model name, then try again."
        ) from e


def _ts(t0: float) -> str:
    """Format elapsed seconds since t0 as [MM:SS]."""
    elapsed = time.time() - t0
    m, s = divmod(int(elapsed), 60)
    return f"[dim][{m:02d}:{s:02d}][/dim]"


# ---------------------------------------------------------------------------
# Agent loop
# ---------------------------------------------------------------------------

def _run_agent_loop(
    settings: Settings,
    user_message: str,
    system_prompt: str,
) -> None:
    """Run the agentic function-calling loop using google-genai."""
    client = genai.Client()

    config = types.GenerateContentConfig(
        system_instruction=system_prompt,
        tools=[types.Tool(function_declarations=_TOOL_DECLARATIONS)],
        temperature=settings.temperature,
    )

    contents: list[types.Content] = [
        types.Content(role="user", parts=[types.Part.from_text(text=user_message)])
    ]

    step = 0
    t0 = time.time()

    while True:
        if settings.verbose:
            _console.print(f"  {_ts(t0)} [dim]Calling LLM...[/dim]")

        response = client.models.generate_content(
            model=settings.model_name,
            contents=contents,
            config=config,
        )

        candidate = response.candidates[0]
        parts = candidate.content.parts

        # Check for function calls
        function_calls = [p for p in parts if p.function_call]

        if not function_calls:
            # Text-only response — agent is done
            if settings.verbose and response.text:
                _console.print(
                    Panel(
                        response.text[:500],
                        title=f"{_ts(t0)} [bold cyan]Agent Response[/bold cyan]",
                        border_style="cyan",
                    )
                )
            break

        step += 1

        # Append the model's response (with function calls) to contents
        contents.append(candidate.content)

        # Execute each function call and collect responses
        function_response_parts: list[types.Part] = []
        for part in function_calls:
            fc = part.function_call
            name = fc.name
            args = _to_python(fc.args) if fc.args else {}

            if settings.verbose:
                args_short = {}
                for k, v in args.items():
                    s = str(v)
                    args_short[k] = s[:120] + "..." if len(s) > 120 else s
                _console.print(
                    f"  {_ts(t0)} [yellow]>> Tool:[/yellow] [bold]{name}[/bold]"
                    f"({', '.join(f'{k}={v!r}' for k, v in args_short.items())})"
                )

            result = _dispatch_tool(name, args)

            if settings.verbose:
                preview = str(result)[:200]
                if len(str(result)) > 200:
                    preview += "..."
                _console.print(
                    f"  {_ts(t0)} [green]<< Result[/green] [dim]({name}):[/dim] {preview}"
                )

            function_response_parts.append(
                types.Part.from_function_response(name=name, response=result)
            )

        # Append function responses back to contents
        contents.append(types.Content(role="user", parts=function_response_parts))

    if settings.verbose:
        elapsed = time.time() - t0
        m, s = divmod(int(elapsed), 60)
        _console.print(f"\n[bold]Completed in {step} steps ({m}m {s}s).[/bold]")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_and_run_agent(settings: Settings) -> VarianceReport:
    """Build the agent, run analysis, and return the report.

    Args:
        settings: Application settings.

    Returns:
        Complete VarianceReport with findings.
    """
    started_at = datetime.now()

    # Reset tool state from any previous run
    reset_state()

    # Wire the SQL executor
    executor = SQLExecutor(
        db_path=settings.db_path,
        max_rows=settings.max_rows,
        query_timeout=settings.query_timeout,
    )
    set_executor(executor)

    combined_prompt = _load_prompts()

    # Create run directory
    run_dir = settings.runs_dir / started_at.strftime("%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    user_message = (
        "Analyze the warehouse database for variance anomalies. "
        "Follow the 4-phase methodology: orientation, hypothesis formation, "
        "deep dive, and synthesis. Identify all significant variances, "
        "trace them to root causes, and produce confidence-scored findings."
    )

    if settings.verbose:
        _console.print("[dim]Checking LLM connectivity...[/dim]")
        _ping_llm(settings.model_name)
        _console.print("[bold]Running agent...[/bold]\n")

    _run_agent_loop(settings, user_message, combined_prompt)

    # Build report from accumulated tool state
    report = _extract_report(settings, executor, started_at, run_dir)

    # Save report to run directory
    report_path = run_dir / "report.json"
    report_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")

    # Save markdown report
    from va_agent.output.writer import ReportWriter

    writer = ReportWriter(settings)
    md_path = run_dir / "report.md"
    md_path.write_text(writer._render_markdown(report), encoding="utf-8")

    # Save audit log
    audit_log_data = executor.get_audit_entries()
    audit_path = run_dir / "audit_log.json"
    audit_path.write_text(
        json.dumps(audit_log_data, indent=2, default=str),
        encoding="utf-8",
    )

    # Save executed queries as SQL file
    queries = [
        f"-- Query #{i+1} ({entry.get('execution_time_ms', 0):.0f}ms, "
        f"{entry.get('row_count', 0)} rows)\n{entry.get('sql', '')}\n"
        for i, entry in enumerate(audit_log_data)
    ]
    sql_path = run_dir / "executed_queries.sql"
    sql_path.write_text("\n".join(queries), encoding="utf-8")

    # Save findings separately
    findings_path = run_dir / "findings.json"
    findings_data = [f.model_dump() for f in report.findings]
    findings_path.write_text(
        json.dumps(findings_data, indent=2, default=str), encoding="utf-8"
    )

    # Update latest pointer
    latest_file = settings.runs_dir / "latest_run"
    latest_file.write_text(str(run_dir.resolve()), encoding="utf-8")

    executor.close()

    if settings.verbose:
        _console.print(f"\n[green]Artifacts saved to:[/green] {run_dir}")
        _console.print(
            "  report.json, report.md, findings.json, executed_queries.sql, audit_log.json"
        )

    return report


def _extract_report(
    settings: Settings,
    executor: SQLExecutor,
    started_at: datetime,
    run_dir: Path,
) -> VarianceReport:
    """Build VarianceReport from accumulated tool state."""
    completed_at = datetime.now()

    findings = get_findings()
    sections = get_sections()

    # Build executive summary from findings
    if findings:
        finding_summaries = [f"- {f.title} ({f.confidence.level.value})" for f in findings]
        executive_summary = (
            f"Analysis identified {len(findings)} variance anomalies:\n"
            + "\n".join(finding_summaries)
        )
    else:
        executive_summary = "No significant variances identified."

    return VarianceReport(
        title="Variance Analysis Report",
        executive_summary=executive_summary,
        sections=sections,
        findings=findings,
        metadata=ExecutionMetadata(
            started_at=started_at,
            completed_at=completed_at,
            model_name=settings.model_name,
            total_queries=len(executor.audit_log),
            run_dir=str(run_dir),
        ),
    )
