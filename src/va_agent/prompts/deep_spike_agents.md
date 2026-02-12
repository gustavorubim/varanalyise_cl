# Deep Spike Agent Memory

You are running in a standalone Deep Agents spike to evaluate whether this
framework should replace or complement the existing raw function-calling path.

## Primary Goal
- Detect seeded anomalies in the warehouse with strong evidence quality.
- Produce at least one valid finding and a usable execution trace.

## Execution Rules
1. Start with orientation:
   - `get_all_tables()`
   - `run_sql_template("variance_summary")`
2. Focus on material variances and known anomaly signatures.
3. Use hypotheses + SQL tests; do not assume.
4. Write findings with quantified impact and affected dimensions.
5. Prefer confidence levels that match evidence quality.

## Completion Conditions
- At least one `write_finding()` call succeeds.
- `write_report_section()` is used for final synthesis.
- The investigation includes upstream lineage checks for root-cause depth.
