"""Build and run the deep agent for variance analysis.

Wires tools, prompts, and configuration into a deepagents CompiledStateGraph.
"""

from __future__ import annotations

import json
import sys
import time
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FuturesTimeout
from datetime import datetime
from pathlib import Path

from langgraph.checkpoint.memory import MemorySaver
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from va_agent.config import Settings
from va_agent.models import ExecutionMetadata, VarianceReport
from va_agent.sql.executor import SQLExecutor
from va_agent.tools import report_tools, sql_tools
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
_SENTINEL = object()  # marker for StopIteration in thread


def _load_prompts() -> str:
    """Load and combine all prompt files."""
    prompts_dir = Path(__file__).parent.parent / "prompts"
    parts = []
    for name in ["system.md", "hypothesis.md", "synthesis.md"]:
        path = prompts_dir / name
        if path.exists():
            parts.append(path.read_text(encoding="utf-8"))
    return "\n\n---\n\n".join(parts)


_LLM_TIMEOUT = 120  # seconds — fail fast instead of hanging forever


def _ping_llm(model_name: str) -> None:
    """Quick smoke-test: send a tiny request to verify the API key and model work."""
    from langchain.chat_models import init_chat_model

    llm = init_chat_model(model_name, timeout=15)
    try:
        resp = llm.invoke("Reply with the single word OK.")
        _console.print(f"[green]LLM connectivity OK[/green] [dim]({model_name})[/dim]")
    except Exception as e:
        raise RuntimeError(
            f"LLM connectivity check failed for {model_name}: {e}\n"
            "Verify your GOOGLE_API_KEY and model name, then try again."
        ) from e


def build_agent(settings: Settings):
    """Build the deep agent with all tools and configuration.

    Args:
        settings: Application settings.

    Returns:
        Compiled agent graph and executor.
    """
    from deepagents import create_deep_agent
    from langchain.chat_models import init_chat_model

    # Wire the SQL executor
    executor = SQLExecutor(
        db_path=settings.db_path,
        max_rows=settings.max_rows,
        query_timeout=settings.query_timeout,
    )
    set_executor(executor)

    combined_prompt = _load_prompts()

    tools = [
        run_sql_query,
        run_sql_template,
        get_table_schema,
        get_table_lineage,
        get_all_tables,
        write_finding,
        write_report_section,
    ]

    # Build the model ourselves so we can set a timeout (deepagents doesn't
    # forward kwargs to init_chat_model for non-openai providers).
    llm = init_chat_model(
        settings.model_name,
        temperature=settings.temperature,
        timeout=_LLM_TIMEOUT,
    )

    agent = create_deep_agent(
        model=llm,
        tools=tools,
        system_prompt=combined_prompt,
        checkpointer=MemorySaver(),
        response_format=VarianceReport,
        name="variance-analyst",
    )

    return agent, executor


def _unwrap(value):
    """Unwrap LangGraph Overwrite/channel wrappers to get the raw value."""
    if hasattr(value, "value") and type(value).__name__ == "Overwrite":
        return value.value
    return value


def _extract_messages(node_data) -> list:
    """Safely extract message list from a stream node update."""
    node_data = _unwrap(node_data)
    if isinstance(node_data, dict):
        msgs = node_data.get("messages", [])
    else:
        return []
    msgs = _unwrap(msgs)
    if not isinstance(msgs, list):
        msgs = [msgs] if msgs else []
    return msgs


def _ts(t0: float) -> str:
    """Format elapsed seconds since t0 as [MM:SS]."""
    elapsed = time.time() - t0
    m, s = divmod(int(elapsed), 60)
    return f"[dim][{m:02d}:{s:02d}][/dim]"


def _print_stream_event(event: dict, step: int, t0: float) -> int:
    """Print a streaming event with timestamps.

    Returns the updated step counter.
    """
    ts = _ts(t0)
    for node_name, node_data in event.items():
        messages = _extract_messages(node_data)
        for msg in messages:
            msg_type = getattr(msg, "type", None)

            if msg_type == "ai":
                step += 1
                content = getattr(msg, "content", "")
                tool_calls = getattr(msg, "tool_calls", [])

                if content:
                    text = content if isinstance(content, str) else str(content)
                    if text.strip():
                        _console.print(
                            Panel(
                                text.strip()[:500],
                                title=f"{ts} [bold cyan]Step {step} — Agent[/bold cyan]",
                                border_style="cyan",
                            )
                        )

                if tool_calls:
                    for tc in tool_calls:
                        name = tc.get("name", "?") if isinstance(tc, dict) else getattr(tc, "name", "?")
                        args = tc.get("args", {}) if isinstance(tc, dict) else getattr(tc, "args", {})
                        args_short = {}
                        for k, v in (args.items() if isinstance(args, dict) else []):
                            s = str(v)
                            args_short[k] = s[:120] + "..." if len(s) > 120 else s
                        _console.print(
                            f"  {ts} [yellow]>> Tool:[/yellow] [bold]{name}[/bold]"
                            f"({', '.join(f'{k}={v!r}' for k, v in args_short.items())})"
                        )

            elif msg_type == "tool":
                name = getattr(msg, "name", "?")
                content = getattr(msg, "content", "")
                text = content if isinstance(content, str) else str(content)
                preview = text[:200] + "..." if len(text) > 200 else text
                _console.print(
                    f"  {ts} [green]<< Result[/green] [dim]({name}):[/dim] {preview}"
                )

    return step


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

    agent, executor = build_agent(settings)

    # Create run directory
    run_dir = settings.runs_dir / started_at.strftime("%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    # Run the agent
    user_message = (
        "Analyze the warehouse database for variance anomalies. "
        "Follow the 4-phase methodology: orientation, hypothesis formation, "
        "deep dive, and synthesis. Identify all significant variances, "
        "trace them to root causes, and produce confidence-scored findings."
    )

    invoke_config = {
        "configurable": {"thread_id": f"analysis-{started_at.isoformat()}"},
    }

    if settings.verbose:
        # Verify LLM connectivity before starting the full analysis
        _console.print("[dim]Checking LLM connectivity...[/dim]")
        _ping_llm(settings.model_name)

        _console.print("[bold]Streaming agent steps...[/bold]\n")
        result = None
        step = 0
        t0 = time.time()
        stream = agent.stream(
            {"messages": [{"role": "user", "content": user_message}]},
            config=invoke_config,
            stream_mode="updates",
        )
        stream_iter = iter(stream)

        def _next_event(it):
            """Wrapper for next() that returns _SENTINEL on StopIteration."""
            try:
                return next(it)
            except StopIteration:
                return _SENTINEL

        with ThreadPoolExecutor(max_workers=1) as pool:
            while True:
                # Submit next() to a thread so we can poll for timeout
                future: Future = pool.submit(_next_event, stream_iter)
                wait_start = time.time()

                # Poll every 5 seconds, printing elapsed time to show progress
                while not future.done():
                    try:
                        future.result(timeout=5)
                    except FuturesTimeout:
                        wait_secs = int(time.time() - wait_start)
                        total_secs = int(time.time() - t0)
                        tm, ts = divmod(total_secs, 60)
                        _console.print(
                            f"  [dim][{tm:02d}:{ts:02d}] ... waiting for LLM ({wait_secs}s)[/dim]",
                            end="\r",
                        )
                    except Exception:
                        break  # will be caught below

                # Get the result (may re-raise exceptions from the stream)
                try:
                    event = future.result(timeout=0)
                except Exception as e:
                    _console.print(f"\n[bold red]Agent error:[/bold red] {e}")
                    raise

                if event is _SENTINEL:
                    break

                # Clear the waiting line and print the event
                _console.print(" " * 60, end="\r")
                step = _print_stream_event(event, step, t0)

        # After streaming, get the final state for report extraction
        result = agent.get_state(invoke_config).values
        elapsed = time.time() - t0
        m, s = divmod(int(elapsed), 60)
        _console.print(f"\n[bold]Completed in {step} steps ({m}m {s}s).[/bold]")
    else:
        result = agent.invoke(
            {"messages": [{"role": "user", "content": user_message}]},
            config=invoke_config,
        )

    # Extract structured response or build from tools
    report = _extract_report(result, settings, executor, started_at, run_dir)

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
        _console.print(f"  report.json, report.md, findings.json, executed_queries.sql, audit_log.json")

    return report


def _extract_report(
    result: dict,
    settings: Settings,
    executor: SQLExecutor,
    started_at: datetime,
    run_dir: Path,
) -> VarianceReport:
    """Extract VarianceReport from agent result, falling back to tool state."""
    completed_at = datetime.now()

    # Try structured response first
    structured = result.get("structured_response")
    if structured and isinstance(structured, VarianceReport):
        structured.metadata = ExecutionMetadata(
            started_at=started_at,
            completed_at=completed_at,
            model_name=settings.model_name,
            total_queries=len(executor.audit_log),
            run_dir=str(run_dir),
        )
        return structured

    # Fall back: build report from accumulated tool state
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
