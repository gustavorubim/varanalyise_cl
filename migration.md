# Migration Guide: Adapting This Project to a New Database

This guide is a practical, step-by-step walkthrough to migrate the Variance Analysis Agent to a new database.

It covers two scenarios:
- Scenario A: New dataset/schema, still using SQLite
- Scenario B: New SQL engine (Postgres, Snowflake, BigQuery, etc.)

The current codebase is SQLite-first and uses read-only SQL safety layers. If you switch engines, plan for adapter work in the SQL connection and guard layer.

## 0. What "done" looks like

Your migration is complete when all of these are true:
1. `va analyze` runs end-to-end against your new DB.
2. Agent orientation works (`get_all_tables`, `variance_summary`) and returns valid results.
3. Agent can trace anomalies upstream using your lineage graph.
4. Findings include amount, percentage, and dimensions.
5. Tests pass (unit + integration; regression if still relevant).
6. Output artifacts are generated (`report.json`, `report.md`, `audit_log.json`, `executed_queries.sql`).

## 1. Pre-migration decisions

Before touching code, decide:
1. Target SQL engine: SQLite or non-SQLite.
2. Data model strategy:
- Option 1: Keep existing canonical table names and reshape new source data into them.
- Option 2: Change canonical table names and update all references in code/prompts/tests.
3. Currency strategy:
- Keep `amount_local` + `fct_fx_rates` + USD conversion path.
- Or store all values in one reporting currency and simplify FX logic.
4. Time grain: monthly (`YYYY-MM`) vs daily/quarterly (requires query/template updates).
5. Materiality thresholds (currently prompt-driven as >5% and >$1,000).

Recommendation: For fastest migration, keep canonical names and the same grain.

## 2. Create a schema mapping sheet (mandatory)

Create a simple mapping table before coding.

Use this template:

| Existing Canonical Table | Existing Key Columns | Existing Measures | New Source Table/View | Transform Needed |
|---|---|---|---|---|
| raw_ledger_entries | entry_id, account_code, period, cost_center, department | amount_local | ... | ... |
| stg_account_mapping | account_code | - | ... | ... |
| stg_cost_center_mapping | cost_center | - | ... | ... |
| fct_actuals_monthly | account_code, cost_center, currency, period | amount_local, entry_count | ... | ... |
| fct_budget_monthly | department, account_type, period | budget_amount | ... | ... |
| fct_fx_rates | currency, period | rate_to_usd | ... | ... |
| int_actuals_usd | account_code, cost_center, period | amount_usd | ... | ... |
| mart_pnl_report | department, account_type, period | actual_usd, budget_usd, variance_usd, variance_pct | ... | ... |

Do not proceed until every row has a concrete mapping.

## 3. Quick repository inventory

Core files you will edit:
1. `src/va_agent/data/lineage_registry.py` (table metadata + DAG)
2. `src/va_agent/sql/templates.py` (all SQL templates)
3. `src/va_agent/tools/sql_tools.py` (tool docs/table list)
4. `src/va_agent/prompts/system.md` (workflow instructions)
5. `AGENTS.md` (agent memory and rules)
6. `src/va_agent/data/seed_generator.py` (if you still want synthetic seed data)
7. `tests/*` (schema assumptions and expected behavior)
8. `README.md` and `SPEC.md` (documentation consistency)

If using a non-SQLite engine, also edit:
1. `src/va_agent/sql/connection.py`
2. `src/va_agent/sql/executor.py`
3. `src/va_agent/sql/guard.py`

## 4. Build a migration branch

```bash
git checkout -b feat/db-migration-<target>
```

(Optional but recommended): snapshot current behavior.

```bash
uv run pytest tests/unit -v
uv run pytest tests/integration -v
```

## 5. Connect the project to your new DB file/instance

## 5.1 If staying on SQLite

Set database path via env var:

```bash
export VA_DB_PATH=/absolute/path/to/new_warehouse.db
# or in .env
# VA_DB_PATH=/absolute/path/to/new_warehouse.db
```

`Settings` already reads this via `src/va_agent/config.py`.

## 5.2 If moving to non-SQLite

Add new settings fields in `src/va_agent/config.py`:
- `db_dialect`
- `db_host`
- `db_port`
- `db_name`
- `db_user`
- `db_password`
- `db_schema` (if relevant)

Then refactor `open_readonly` in `src/va_agent/sql/connection.py` to build a read-only connection for your engine.

Important: preserve read-only behavior. If your engine supports roles, use a read-only user.

## 6. Update lineage registry first

Edit `src/va_agent/data/lineage_registry.py`:
1. Replace table names/descriptions/grain definitions.
2. Update `upstream` dependencies for each table.
3. Update `key_columns` and `measure_columns`.
4. Ensure no cycles in lineage.

Validation check:
- `get_all_tables()` should return the expected table count and metadata.
- `get_table_lineage("<your_mart>")` should include full upstream chain.

## 7. Update SQL templates

Edit `src/va_agent/sql/templates.py`.

For each template (`variance_summary`, `account_detail`, `fx_rate_history`, etc.):
1. Replace table/column names based on your mapping sheet.
2. Keep deterministic ordering and grouping.
3. Preserve query safety assumptions (single SELECT/WITH statement).
4. Preserve row and error behavior expected by tools/tests.

Add/remove templates only if necessary. If removed, update calls in prompts and tests.

Validation check: every template in `TEMPLATES` executes successfully.

## 8. Update SQL tool descriptions

Edit `src/va_agent/tools/sql_tools.py`:
1. In `get_table_schema` docstring, replace the "Available tables" list to match your schema.
2. Ensure helper queries (`sqlite_master`, sample rows, row count) still work for your dialect.
- For non-SQLite, replace schema introspection query logic.

Note: The agent relies heavily on tool docstrings for planning, so stale docs reduce quality.

## 9. Update prompts and agent memory

Edit these files to match your migrated model:
1. `src/va_agent/prompts/system.md`
2. `src/va_agent/prompts/hypothesis.md`
3. `src/va_agent/prompts/synthesis.md`
4. `AGENTS.md`

What to update:
1. Table names and lineage path examples.
2. Analysis patterns that assume old columns (e.g., `entry_count`, `rate_to_usd`).
3. Materiality and stop criteria if business rules changed.
4. Orientation instructions (still keep "start with `get_all_tables()` then `run_sql_template(\"variance_summary\")").

## 10. Decide what to do with seeding

Current project uses `va seed` to generate synthetic data.

Choose one:
1. Keep synthetic seeding: rewrite `src/va_agent/data/seed_generator.py` for your new schema.
2. Disable seeding for production migration: keep `va analyze --db-path ...` as primary path.

If seeding is kept, ensure seeded data still supports:
- material variances
- multi-hop lineage tracing
- confidence scoring

## 11. SQL safety adaptation (critical)

Current safety stack:
1. read-only connection mode
2. query-only runtime enforcement
3. authorizer callback
4. SQL statement guard

### SQLite path
No major redesign needed; verify your new DB file works with:
- `open_readonly`
- `validate_query`

### Non-SQLite path
You must redesign safety controls in:
- `src/va_agent/sql/connection.py`
- `src/va_agent/sql/executor.py`
- `src/va_agent/sql/guard.py`

Minimum requirements:
1. Read-only credentials/role.
2. Single-statement SELECT-only policy.
3. Timeout support.
4. Audit log capture unchanged.

## 12. Update tests in this order

1. Unit tests:
- `tests/unit/test_lineage.py`
- `tests/unit/test_guard.py`
- `tests/unit/test_executor.py`
- `tests/unit/test_models.py`

2. Integration tests:
- `tests/integration/test_tools.py`
- `tests/integration/test_sql_pipeline.py`
- `tests/integration/test_seed_generator.py` (if seeding retained)

3. Regression tests:
- `tests/regression/test_known_anomalies.py`
- `tests/regression/test_runtime_budget.py`

If old anomaly IDs or assumptions no longer apply, replace with new deterministic fixtures.

## 13. Run validation commands

Use this sequence after edits:

```bash
uv run pytest tests/unit -v
uv run pytest tests/integration -v
uv run pytest tests/regression -v -m regression
```

Then run the agent:

```bash
uv run va analyze --verbose --db-path /absolute/path/to/your/db
```

Check generated artifacts in `runs/<timestamp>/`:
1. `report.json`
2. `report.md`
3. `findings.json`
4. `audit_log.json`
5. `executed_queries.sql`

## 14. Functional acceptance checklist

Mark each item:
1. Orientation runs first (table summary + variance summary).
2. At least one anomaly traced from mart to upstream source.
3. Findings include amount, percent, dimensions, root cause, and recommendations.
4. Confidence scores are present and reasonable.
5. SQL audit log shows only read-only queries.
6. No hardcoded references to removed old tables/columns remain.

Helpful command:

```bash
rg -n "raw_ledger_entries|stg_account_mapping|stg_cost_center_mapping|fct_actuals_monthly|fct_budget_monthly|fct_fx_rates|int_actuals_usd|mart_pnl_report" -S src tests README.md SPEC.md AGENTS.md
```

Use this to find stale references if you renamed canonical tables.

## 15. Common migration pitfalls

1. Updating templates but forgetting lineage metadata.
2. Updating lineage but forgetting prompts and AGENTS memory.
3. Non-SQLite migration without equivalent read-only protections.
4. Time-grain mismatch (`YYYY-MM` assumptions in templates and windows).
5. Budget and actual measures in different currencies without normalization.
6. Tests silently still pointing at old seeded DB paths.

## 16. Suggested rollout strategy

1. Phase 1: schema-parity migration in SQLite (fast confidence build).
2. Phase 2: run side-by-side with old DB and compare top findings.
3. Phase 3: only then migrate SQL engine if needed.
4. Phase 4: tighten prompts and thresholds with real production feedback.

## 17. Minimal migration plan (quick version)

If you need the shortest viable path:
1. Keep canonical table names.
2. Load your new data into those 8 tables (or views).
3. Update only lineage metadata descriptions if needed.
4. Keep templates mostly intact.
5. Run tests and `va analyze`.

This approach avoids major code changes and is usually the highest-leverage first step.

## 18. Definition-of-ready checklist for your next DB

Before implementation, confirm:
1. You can provide all required fields for the mart and upstream trace tables.
2. You have a reliable `period` field for trend analysis.
3. You can separate budget vs actual and compute variance.
4. You can map account classification and cost-center/department dimensions.
5. You can run read-only SELECT queries from the runtime environment.

If any of these are missing, design compensating transformations before coding.
