-- Query #1 (11ms, 480 rows)
SELECT
    department,
    account_type,
    period,
    ROUND(SUM(actual_usd), 2) AS total_actual,
    ROUND(SUM(budget_usd), 2) AS total_budget,
    ROUND(SUM(variance_usd), 2) AS total_variance,
    ROUND(AVG(variance_pct), 2) AS avg_variance_pct
FROM mart_pnl_report

GROUP BY department, account_type, period
ORDER BY ABS(SUM(variance_usd)) DESC

-- Query #2 (4ms, 2 rows)

SELECT 
    account_code, 
    cost_center, 
    SUM(amount_usd) as actual_usd
FROM int_actuals_usd
WHERE department = 'Marketing' 
  AND period = '2023-07'
  AND account_code IN (SELECT account_code FROM stg_account_mapping WHERE account_type = 'COGS')
GROUP BY 1, 2
ORDER BY actual_usd DESC


-- Query #3 (4ms, 0 rows)

SELECT 
    a.account_code, 
    a.account_name, 
    a.account_type,
    r.department,
    SUM(r.amount) as total_amount_local
FROM raw_ledger_entries r
JOIN stg_account_mapping a ON r.account_code = a.account_code
WHERE r.account_code IN ('5000', '5010')
GROUP BY 1, 2, 3, 4
ORDER BY 1, 4


-- Query #4 (2ms, 2 rows)

SELECT 
    account_code, 
    account_name, 
    account_type
FROM stg_account_mapping
WHERE account_code IN ('5000', '5010')


-- Query #5 (3ms, 6 rows)

SELECT 
    entry_id,
    account_code,
    cost_center,
    department,
    period,
    amount_local,
    currency
FROM raw_ledger_entries
WHERE department = 'Marketing'
  AND period = '2023-07'
  AND account_code IN ('5000', '5010')
LIMIT 10

