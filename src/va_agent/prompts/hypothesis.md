# Hypothesis Testing Framework

When investigating a variance, follow this structured approach:

## Step 1: STATE the hypothesis
"The [variance amount] [direction] variance in [dimension] is caused by [proposed root cause]."

## Step 2: DESIGN the test
- Write a SQL query that would confirm OR refute the hypothesis
- The query should isolate the proposed cause from other factors
- Use period-over-period comparisons to establish the baseline

## Step 3: EXECUTE and EVALUATE
- Run the SQL query
- Compare results against the hypothesis prediction
- If confirmed: gather additional supporting evidence
- If refuted: form a new hypothesis and repeat

## Step 4: ASSIGN CONFIDENCE
Rate each factor 0.0 to 1.0:
- evidence_breadth: How many independent queries support this? (1=3+ queries)
- lineage_depth: How deep upstream did you trace? (1=reached raw layer)
- variance_explanation: What % of the variance does this explain? (1=fully explains)
- hypothesis_exclusion: How many alternatives did you rule out? (1=2+ ruled out)
- data_quality: Are there data quality concerns? (1=no concerns)
- temporal_consistency: Is the pattern consistent across time? (1=fully consistent)

## Common Hypothesis Patterns
1. **Volume change**: Units/transactions changed, not price → check entry_count
2. **Price/rate change**: FX rate or price moved → check fct_fx_rates
3. **Mix shift**: Composition changed (more expensive items) → check dimensional breakdown
4. **Classification error**: Items in wrong category → check raw vs mapping
5. **Budget error**: Budget was wrong, actuals are fine → check fct_budget_monthly trends
6. **Timing**: Entries shifted between periods → check adjacent periods
