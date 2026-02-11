"""Typer CLI for the Variance Analysis Agent."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

# Load .env early so GOOGLE_API_KEY is available to langchain-google-genai
load_dotenv()

from va_agent.config import Settings

app = typer.Typer(
    name="va",
    help="Autonomous Variance Analysis Agent — analyze financial data anomalies.",
    no_args_is_help=True,
)
console = Console()


def _acquire_analysis_lock(lock_path: Path) -> None:
    """Acquire an exclusive on-disk lock for analysis runs."""
    payload = {
        "pid": os.getpid(),
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as e:
        details = ""
        try:
            details = lock_path.read_text(encoding="utf-8").strip()
        except OSError:
            pass

        hint = f" Existing lock details: {details}" if details else ""
        raise RuntimeError(
            f"Another analysis is already running (lock: {lock_path}).{hint}"
        ) from e

    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(json.dumps(payload))


def _release_analysis_lock(lock_path: Path) -> None:
    """Release analysis run lock."""
    try:
        lock_path.unlink(missing_ok=True)
    except OSError:
        # Best-effort cleanup; stale lock details are still inspectable.
        pass


@app.command()
def seed(
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing database"),
    db_path: Path | None = typer.Option(None, "--db-path", help="Custom database path"),
) -> None:
    """Seed the warehouse database with synthetic financial data and anomalies."""
    settings = Settings()
    if db_path:
        settings.db_path = db_path

    console.print("[bold]Seeding warehouse database...[/bold]")

    try:
        from va_agent.data.seed_generator import seed_database

        result = seed_database(settings, force=force)
        path, table_counts, checksum = result

        # Display results
        table = Table(title="Warehouse Tables")
        table.add_column("Table", style="cyan")
        table.add_column("Rows", justify="right", style="green")
        for tbl_name, count in table_counts.items():
            table.add_row(tbl_name, str(count))

        console.print(table)
        console.print(f"\n[green]Database created:[/green] {path}")
        console.print(f"[dim]Checksum: {checksum}[/dim]")

    except FileExistsError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(code=1) from e
    except Exception as e:
        console.print(f"[red]Seed failed:[/red] {e}")
        raise typer.Exit(code=1) from e


@app.command()
def analyze(
    model: str = typer.Option(None, "--model", "-m", help="Model name override"),
    deterministic: bool = typer.Option(
        True, "--deterministic/--no-deterministic", help="Use temperature=0"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    db_path: Path | None = typer.Option(None, "--db-path", help="Custom database path"),
) -> None:
    """Run the variance analysis agent against the warehouse database."""
    settings = Settings()
    if model:
        settings.model_name = model
    if deterministic:
        settings.temperature = 0.0
    settings.verbose = verbose
    if db_path:
        settings.db_path = db_path
    settings.ensure_dirs()

    if not settings.db_path.exists():
        console.print(
            "[red]Error:[/red] Database not found. Run [cyan]va seed[/cyan] first."
        )
        raise typer.Exit(code=1)

    lock_path = settings.runs_dir / ".analysis.lock"
    try:
        _acquire_analysis_lock(lock_path)
    except RuntimeError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(code=1) from e

    console.print(f"[bold]Starting analysis...[/bold] (model={settings.model_name})")

    from va_agent.graph.build import build_and_run_agent

    try:
        report = build_and_run_agent(settings)
    finally:
        _release_analysis_lock(lock_path)

    console.print(f"\n[green]Analysis complete.[/green] {len(report.findings)} findings.")
    console.print(f"Report: {report.title}")
    console.print(f"Summary: {report.executive_summary[:200]}...")


@app.command()
def report(
    run_dir: Path | None = typer.Option(None, "--run-dir", help="Specific run directory"),
) -> None:
    """Generate output artifacts from the latest analysis run."""
    settings = Settings()

    from va_agent.output.writer import ReportWriter

    writer = ReportWriter(settings)
    artifacts = writer.write_all(run_dir)

    console.print("[green]Report artifacts written:[/green]")
    for name, path in artifacts.items():
        console.print(f"  {name}: {path}")


@app.command()
def audit(
    run_dir: Path | None = typer.Option(None, "--run-dir", help="Specific run directory"),
) -> None:
    """Display the audit log of SQL queries executed during analysis."""
    settings = Settings()

    from va_agent.output.writer import ReportWriter

    writer = ReportWriter(settings)
    log = writer.get_audit_log(run_dir)

    table = Table(title="Query Audit Log")
    table.add_column("#", style="dim", justify="right")
    table.add_column("Time (ms)", justify="right")
    table.add_column("Rows", justify="right")
    table.add_column("SQL", max_width=80)

    for i, entry in enumerate(log, 1):
        table.add_row(
            str(i),
            f"{entry.get('execution_time_ms', 0):.0f}",
            str(entry.get('row_count', 0)),
            entry.get('sql', '')[:80],
        )

    console.print(table)
    console.print(f"\n[dim]Total queries: {len(log)}[/dim]")


if __name__ == "__main__":
    app()
