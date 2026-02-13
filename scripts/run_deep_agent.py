#!/usr/bin/env python3
"""Run deep-agent analysis directly from the repository.

This script avoids requiring CLI installation (`va`) by adding `src/` to
`sys.path` at runtime and calling deep-engine entry functions directly.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv
from rich.console import Console

# Resolve repository root and ensure local package imports work without install.
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

console = Console()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run deep-agent analysis without installing the `va` CLI."
    )
    parser.add_argument("--model", type=str, default=None, help="Model name override.")
    parser.add_argument("--db-path", type=Path, default=None, help="Custom warehouse DB path.")
    parser.add_argument("--run-label", type=str, default=None, help="Optional run label suffix.")
    parser.add_argument(
        "--repeats",
        type=int,
        default=1,
        help="Deep runs to execute (1 = single run, >1 = benchmark).",
    )
    parser.add_argument(
        "--deterministic",
        action="store_true",
        help="Force temperature=0.0 for reproducibility.",
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose console logging.")
    parser.add_argument(
        "--seed",
        action="store_true",
        help="Seed the database before analysis (if missing or when forcing).",
    )
    parser.add_argument(
        "--force-seed",
        action="store_true",
        help="Overwrite existing database when used with --seed.",
    )
    parser.add_argument(
        "--seed-only",
        action="store_true",
        help="Seed the database and exit without running deep analysis.",
    )
    return parser


def main() -> int:
    # Import after path setup so this script works without package installation.
    from va_agent.config import Settings
    from va_agent.data.seed_generator import seed_database
    from va_agent.graph.deep_engine import run_deep_benchmark, run_deep_spike

    load_dotenv(REPO_ROOT / ".env")
    parser = _build_parser()
    args = parser.parse_args()

    if args.repeats < 1:
        parser.error("--repeats must be >= 1")

    settings = Settings()
    if args.model:
        settings.model_name = args.model
    if args.db_path:
        settings.db_path = args.db_path
    if args.deterministic:
        settings.temperature = 0.0
    settings.verbose = args.verbose
    settings.ensure_dirs()

    should_seed = args.seed or args.seed_only

    if should_seed:
        console.print("[bold]Seeding warehouse database...[/bold]")
        path, table_counts, checksum = seed_database(settings, force=args.force_seed)
        console.print(f"[green]Database created:[/green] {path}")
        console.print(f"[dim]Checksum: {checksum}[/dim]")
        console.print(f"[dim]Tables seeded: {len(table_counts)}[/dim]")
        if args.seed_only:
            return 0

    if not settings.db_path.exists():
        console.print(f"[red]Database not found:[/red] {settings.db_path}")
        console.print("Run with --seed or create the database first.")
        return 1

    if args.repeats == 1:
        report = run_deep_spike(settings=settings, run_label=args.run_label)
        console.print(f"[green]Deep analysis complete.[/green] {len(report.findings)} findings.")
        console.print(f"Run dir: {report.metadata.run_dir}")
        return 0

    result = run_deep_benchmark(
        settings=settings,
        repeats=args.repeats,
        run_label=args.run_label,
    )
    console.print(f"[green]Deep benchmark complete.[/green] repeats={args.repeats}")
    console.print(f"Summary: {result['summary_path']}")
    console.print(f"Comparison: {result['comparison_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
