# Deep Engine Technical Background

This document describes the deep-only runtime implemented in `src/va_agent/graph/deep_engine.py`.

## 1. Scope

`deep_engine.py` is now the canonical runtime path for analysis.

It supports two modes:
1. Single analysis run (`run_deep_spike`)
2. Repeated benchmark runs (`run_deep_benchmark`)

CLI integration routes `va analyze` to this module.

## 2. Core Responsibilities

`deep_engine.py` is responsible for:
1. Creating and invoking a Deep Agents runtime
2. Exposing SQL, lineage, and report-writing tools
3. Enforcing a strict analysis task shape for deterministic behavior
4. Capturing trace/evaluation artifacts
5. Producing benchmark summaries across repeated runs

## 3. Runtime Construction

## 3.1 Prompt assembly

`_load_spike_prompt()` composes instructions from:
- `prompts/system.md`
- `prompts/hypothesis.md`
- `prompts/synthesis.md`
- `prompts/deep_spike_agents.md` (optional)

## 3.2 Tool surface

`_create_deep_agent()` exposes:
- `run_sql_query`
- `run_sql_template`
- `get_all_tables`
- `get_table_lineage`
- `write_finding`
- `write_report_section`

All SQL tools route through `SQLExecutor` with read-only enforcement.

## 3.3 Model binding

The deep runtime binds Gemini through `ChatGoogleGenerativeAI` with:
- model name from `Settings.model_name`
- temperature from `Settings.temperature`
- request timeout (`_DEFAULT_DEEP_REQUEST_TIMEOUT_S`)
- retries (`_DEFAULT_DEEP_RETRIES`)

## 4. Execution Flow

## 4.1 Single run (`run_deep_spike`)

High-level steps:
1. Validate DB path and initialize run directory under `runs/spikes/deep/`
2. Reset report tool state and wire `SQLExecutor`
3. Create Deep Agents runtime
4. Invoke with strict, bounded task message
5. Normalize message stream into trace steps
6. Apply fallback minimum finding/section if needed
7. Build `VarianceReport` from tool state
8. Write artifacts

### Fallback behavior

If deep invocation fails, the runtime still writes usable artifacts by:
- injecting a fallback finding from the largest mart variance
- injecting a fallback summary section

This prevents empty-run outputs.

## 4.2 Benchmark mode (`run_deep_benchmark`)

High-level steps:
1. Force deterministic temperature (`0.0`) during benchmark loop
2. Execute `run_deep_spike` repeatedly
3. Aggregate per-run evaluations
4. Compute consistency metrics (stddev)
5. Write benchmark summary artifacts

## 5. Artifacts

## 5.1 Per deep run

Written to `runs/spikes/deep/<timestamp>[_label]/`:
- `report.json`
- `report.md`
- `findings.json`
- `audit_log.json`
- `executed_queries.sql`
- `trace.json`
- `evaluation.json`
- `comparison.md`

Pointer:
- `runs/spikes/deep/latest_run`

## 5.2 Per benchmark

Written to `runs/spikes/deep/benchmark_<timestamp>/`:
- `benchmark_summary.json`
- `comparison.md`

## 6. Evaluation Model

`evaluate_findings()` uses heuristic matching against seeded anomaly rules (`_ANOMALY_RULES`) and computes:
- anomaly recall
- precision proxy
- evidence sufficiency
- root-cause depth
- matched/unmatched anomaly sets

`compute_consistency()` adds run-to-run stability metrics:
- recall stddev
- precision stddev

## 7. Important Tuning Points

Most impactful controls:
1. Prompt content (`prompts/*.md` and strict user task message)
2. Tool list in `_create_deep_agent()`
3. Runtime constants:
- `_DEFAULT_DEEP_REQUEST_TIMEOUT_S`
- `_DEFAULT_DEEP_RETRIES`
- `_DEFAULT_DEEP_RECURSION_LIMIT`
- `_MAX_SPIKE_TOOL_CALLS_HINT`
4. Heuristic evaluation rules (`_ANOMALY_RULES`)

## 8. Operational Commands

Single run:

```bash
uv run va analyze --verbose
# equivalent deep module entry
uv run python -m va_agent.graph.deep_engine --repeats 1 --deterministic --verbose
```

Benchmark:

```bash
uv run python -m va_agent.graph.deep_engine --repeats 3 --deterministic --verbose
```

## 9. Dependencies Used by Deep Runtime

- `deepagents`
- `langchain-google-genai`
- `typer` / `rich`
- SQL stack (`sqlparse`, SQLite runtime)

The legacy direct `google-genai` orchestration path is intentionally removed from the active runtime.
