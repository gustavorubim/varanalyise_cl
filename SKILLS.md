# Variance Analysis Agent — Skills Reference

## Skill 1: Variance Triage

**Trigger:** Start of analysis or when needing to identify largest anomalies.

**Inputs:** None (operates on mart_pnl_report).

**SQL Template:** `variance_summary`

**Steps:**
1. Run `run_sql_template("variance_summary")` for all periods
2. Sort by absolute variance descending
3. Apply materiality filter: |variance_usd| > $1,000 AND |variance_pct| > 5%
4. Group by category (COGS, Revenue, OpEx, etc.)
5. Prioritize: largest absolute variances first

**Expected Output:** Ranked list of material variances with department, account_type, period, amount, and percentage.

**Failure Modes:**
- All variances below materiality → report "no material variances"
- Budget data missing → note in data quality section

---

## Skill 2: Lineage Backtrace

**Trigger:** When a variance is identified at mart level and needs root-cause tracing.

**Inputs:** table_name, dimension values (department, period, account_type).

**Steps:**
1. Call `get_table_lineage(table_name)` to get upstream chain
2. For each upstream table, query the relevant dimensions
3. Identify which upstream table shows the anomaly
4. Continue upstream until the anomaly disappears or reaches raw layer

**SQL Pattern:**
```sql
-- At each layer, check if the anomaly exists
SELECT period, SUM(amount_usd) FROM int_actuals_usd
WHERE department = ? AND period = ? GROUP BY period

SELECT period, SUM(amount_local) FROM fct_actuals_monthly
WHERE department = ? AND period = ? GROUP BY period

SELECT period, COUNT(*), SUM(amount_local) FROM raw_ledger_entries
WHERE department = ? AND period = ? GROUP BY period
```

**Expected Output:** The specific table/layer where the anomaly originates.

**Failure Modes:**
- Anomaly exists at all layers → originates in raw data (source issue)
- Anomaly only at mart → likely a budget or FX issue in the join

---

## Skill 3: FX Diagnosis

**Trigger:** When variance may be caused by FX rate movements.

**Inputs:** currency, period.

**SQL Template:** `fx_rate_history`

**Steps:**
1. Run `run_sql_template("fx_rate_history", currency=currency)`
2. Check period-over-period change for the target period
3. Compare rate to rolling 3-month average
4. Quantify FX impact: `amount_local × (actual_rate - expected_rate)`

**Expected Output:** FX impact amount, rate deviation percentage, affected entities.

**Failure Modes:**
- Currency not found → check stg_cost_center_mapping for the department's currency
- Rate deviation within normal range (±3%) → FX is not the cause

---

## Skill 4: Mapping Integrity Check

**Trigger:** When classification errors are suspected.

**SQL Template:** `classification_check`

**Steps:**
1. Run `run_sql_template("classification_check")`
2. If mismatches found, quantify impact per account_code
3. Trace affected entries to specific departments and periods
4. Calculate the net P&L impact of misclassification

**SQL Pattern:**
```sql
SELECT r.account_code, r.account_type AS ledger_type,
       m.account_type AS mapping_type,
       COUNT(*) AS entry_count,
       SUM(r.amount_local) AS total_amount
FROM raw_ledger_entries r
JOIN stg_account_mapping m ON r.account_code = m.account_code
WHERE r.account_type != m.account_type
GROUP BY r.account_code, r.account_type, m.account_type
```

**Expected Output:** List of misclassified entries with amounts and affected categories.

**Failure Modes:**
- No mismatches → classification is clean
- Mismatches exist but net impact is immaterial → note but don't flag as finding

---

## Skill 5: Aggregation Reconciliation

**Trigger:** When upstream and downstream totals don't match.

**Inputs:** Two table names, join key, measure column.

**Steps:**
1. Query upstream table aggregated by join key
2. Query downstream table by same key
3. Compare totals; identify dimension values with discrepancies
4. Calculate reconciliation difference

**SQL Pattern:**
```sql
-- Upstream total
SELECT SUM(amount_local) FROM fct_actuals_monthly
WHERE period = ? AND department = ?

-- Downstream total
SELECT SUM(amount_usd) FROM int_actuals_usd
WHERE period = ? AND department = ?
```

**Expected Output:** Reconciliation status with any discrepancies noted.

**Failure Modes:**
- FX conversion explains the difference → not an error
- Rounding differences < $1 → immaterial

---

## Skill 6: Report Synthesis

**Trigger:** After all investigations are complete.

**Inputs:** List of findings, accumulated sections.

**Steps:**
1. Review all findings by category
2. Calculate total variance explained
3. Write executive summary section
4. Write methodology section
5. Write findings-by-category sections (referencing finding IDs)
6. Write recommendations section
7. Write data quality notes section

**Expected Output:** Complete report with all sections populated.

**Failure Modes:**
- No findings → write "clean bill of health" report
- Low confidence findings only → note uncertainty prominently
