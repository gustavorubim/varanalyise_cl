# Variance Analysis Agent — Specification

## Overview

A local-first autonomous variance analysis agent that:

1. Starts from report-level anomalies in a P&L mart table
2. Traverses upstream multi-hop SQL lineage through an 8-table data warehouse
3. Forms and tests hypotheses via controlled read-only SQL queries
4. Produces confidence-scored root-cause findings with full audit trail

## Design Decisions

### Agent Framework
- **deepagents v0.4.1** (LangChain AI's agent SDK built on LangGraph)
- Returns a `CompiledStateGraph` that manages tool invocation, state, and structured output
- `response_format=VarianceReport` for Pydantic-validated structured output

### LLM Provider
- **Google Gemini** via `langchain-google-genai` (`ChatGoogleGenerativeAI`)
- Default model: `google_genai:gemini-3-flash-preview`
- Requires `GOOGLE_API_KEY` environment variable
- Resolved through LangChain's `init_chat_model()` with `google_genai:` provider prefix

### SQL Safety (4-layer defense)
1. **URI mode=ro**: SQLite connection opened in read-only mode
2. **PRAGMA query_only=ON**: Runtime enforcement at session level
3. **set_authorizer callback**: Per-operation enforcement (only READ/SELECT/FUNCTION allowed)
4. **sqlparse guard**: Statement-level whitelist (only SELECT/WITH), blacklist of dangerous keywords

### Data Model
- 8-table lineage DAG: raw → staging → facts → intermediate → mart
- Synthesized data using numpy RNG (seed=42 for determinism)
- 5 injected anomalies with known root causes for regression testing

### Adjustments from Original Spec
1. **sqlglot replaced by sqlparse**: Lighter, pure Python, sufficient for whitelist validation
2. **Synthetic data instead of external downloads**: Avoids broken URL dependencies
3. **fct_fx_rates synthesized**: No real FX data source; generated from currency mix
4. **Windows compatibility**: `Path.as_posix()` for SQLite URIs, text pointer file instead of symlinks
5. **Fixed timestamp in seed_manifest**: Ensures deterministic database checksums

## Module Structure

```
src/va_agent/
├── __init__.py              # Package version
├── config.py                # Settings(BaseSettings) with VA_ prefix
├── models.py                # Pydantic: Finding, VarianceReport, ConfidenceScore, etc.
├── cli.py                   # Typer CLI: seed, analyze, report, audit
├── data/
│   ├── seed_generator.py    # Data pipeline: generate → inject anomalies → write SQLite
│   └── lineage_registry.py  # Static LINEAGE dict, upstream/downstream traversal
├── sql/
│   ├── connection.py        # 3-layer read-only SQLite connection
│   ├── guard.py             # sqlparse-based query whitelist (Layer 4)
│   ├── executor.py          # SQLExecutor: validate → execute → audit
│   └── templates.py         # Parameterized SQL templates
├── tools/
│   ├── sql_tools.py         # run_sql_query, run_sql_template, get_table_schema
│   ├── lineage_tools.py     # get_table_lineage, get_all_tables
│   └── report_tools.py      # write_finding, write_report_section
├── graph/
│   ├── build.py             # build_agent() → CompiledStateGraph
│   └── state.py             # AnalysisState TypedDict
├── analysis/
│   ├── variance.py          # compute_variance, materiality_threshold
│   ├── decomposition.py     # decompose_variance, Pareto drivers
│   └── confidence.py        # Weighted confidence scoring
├── output/
│   ├── writer.py            # ReportWriter: JSON, MD, SQL, log artifacts
│   └── schemas.py           # JSON Schema export
└── prompts/
    ├── system.md            # 4-phase methodology
    ├── hypothesis.md        # Hypothesis testing framework
    └── synthesis.md         # Report synthesis instructions
```

## Confidence Scoring Formula

```
score = evidence_breadth × 0.25
      + lineage_depth × 0.20
      + variance_explanation × 0.25
      + hypothesis_exclusion × 0.15
      + data_quality × 0.10
      + temporal_consistency × 0.05

HIGH:   score >= 0.70
MEDIUM: score >= 0.40
LOW:    score <  0.40
```

## Seeded Anomalies

| ID | Category | Injection Method | Detection Method |
|----|----------|-----------------|-----------------|
| A-001 | COGS Anomaly | ×1.25 amount for CC-300 in 2024-03/04 | Period-over-period comparison |
| A-002 | Revenue Anomaly | Zero amount for Sales in 2024-06 | Zero-value detection |
| A-003 | FX Anomaly | ×1.15 EUR rate in 2024-07 | Rate trend analysis |
| A-004 | Budget Misalignment | ×2.0 Finance budget in Q3 2024 | Budget vs rolling average |
| A-005 | Classification Error | Change account_type on 50 entries | account_type vs mapping JOIN |
