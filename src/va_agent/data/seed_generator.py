"""Download source datasets, transform into 8-table lineage model, inject anomalies.

Writes a SQLite warehouse.db with 8 analytical tables plus a seed_manifest
documenting injected anomalies.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from va_agent.config import Settings

PERIODS = pd.period_range("2023-01", "2024-12", freq="M")
DEPARTMENTS = ["Sales", "Marketing", "Engineering", "Finance", "Operations"]
COST_CENTERS = ["CC-100", "CC-200", "CC-300", "CC-400", "CC-500"]
CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD"]
ACCOUNT_TYPES = ["Revenue", "COGS", "OpEx", "Depreciation"]
SEGMENTS = ["Enterprise", "SMB", "Consumer"]
COUNTRIES = ["USA", "Germany", "UK", "Japan", "Canada"]
PRODUCTS = ["Product A", "Product B", "Product C", "Product D"]

# Account codes
ACCOUNTS = {
    "4000": ("Revenue", "Product Revenue"),
    "4010": ("Revenue", "Service Revenue"),
    "5000": ("COGS", "Direct Materials"),
    "5010": ("COGS", "Direct Labor"),
    "6000": ("OpEx", "Salaries & Wages"),
    "6010": ("OpEx", "Marketing Spend"),
    "6020": ("OpEx", "Travel & Entertainment"),
    "7000": ("Depreciation", "Asset Depreciation"),
}

# FX base rates (to USD)
BASE_FX_RATES = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.27,
    "JPY": 0.0067,
    "CAD": 0.74,
}


def _generate_raw_ledger(rng: np.random.Generator) -> pd.DataFrame:
    """Generate ~2000 raw journal entries."""
    rows = []
    entry_id = 1
    for period in PERIODS:
        period_str = str(period)
        for dept_idx, dept in enumerate(DEPARTMENTS):
            cc = COST_CENTERS[dept_idx]
            currency = CURRENCIES[dept_idx]
            for acct_code, (acct_type, _acct_name) in ACCOUNTS.items():
                # 1-3 entries per account per dept per month
                n_entries = rng.integers(1, 4)
                for _ in range(n_entries):
                    base = _base_amount(acct_type, rng)
                    rows.append(
                        {
                            "entry_id": entry_id,
                            "period": period_str,
                            "account_code": acct_code,
                            "account_type": acct_type,
                            "department": dept,
                            "cost_center": cc,
                            "currency": currency,
                            "segment": rng.choice(SEGMENTS),
                            "country": COUNTRIES[dept_idx],
                            "product": rng.choice(PRODUCTS),
                            "amount_local": round(float(base), 2),
                            "description": f"JE-{entry_id} {acct_type} entry",
                            "posted_date": f"{period_str}-{rng.integers(1, 29):02d}",
                        }
                    )
                    entry_id += 1
    return pd.DataFrame(rows)


def _base_amount(acct_type: str, rng: np.random.Generator) -> float:
    """Generate a realistic base amount by account type."""
    ranges = {
        "Revenue": (10_000, 80_000),
        "COGS": (5_000, 40_000),
        "OpEx": (3_000, 25_000),
        "Depreciation": (1_000, 5_000),
    }
    lo, hi = ranges.get(acct_type, (1_000, 10_000))
    return float(rng.uniform(lo, hi))


def _build_account_mapping() -> pd.DataFrame:
    """Build stg_account_mapping from static ACCOUNTS dict."""
    rows = [
        {"account_code": code, "account_type": atype, "account_name": aname}
        for code, (atype, aname) in ACCOUNTS.items()
    ]
    return pd.DataFrame(rows)


def _build_cost_center_mapping() -> pd.DataFrame:
    """Build stg_cost_center_mapping."""
    rows = [
        {
            "cost_center": COST_CENTERS[i],
            "department": DEPARTMENTS[i],
            "region": COUNTRIES[i],
        }
        for i in range(len(DEPARTMENTS))
    ]
    return pd.DataFrame(rows)


def _build_actuals_monthly(ledger: pd.DataFrame) -> pd.DataFrame:
    """Aggregate raw ledger to monthly actuals by account × cost_center × currency."""
    grouped = (
        ledger.groupby(["account_code", "cost_center", "currency", "period", "department"])
        .agg(amount_local=("amount_local", "sum"), entry_count=("entry_id", "count"))
        .reset_index()
    )
    return grouped


def _build_budget_monthly(rng: np.random.Generator) -> pd.DataFrame:
    """Generate monthly budget data (quarterly budgets spread to monthly)."""
    rows = []
    for period in PERIODS:
        period_str = str(period)
        for dept in DEPARTMENTS:
            for acct_type in ACCOUNT_TYPES:
                # Quarterly budget, spread evenly across 3 months
                quarterly_base = _base_amount(acct_type, rng) * 3
                monthly_budget = quarterly_base / 3
                # Add some variance
                monthly_budget *= rng.uniform(0.9, 1.1)
                rows.append(
                    {
                        "department": dept,
                        "account_type": acct_type,
                        "period": period_str,
                        "budget_amount": round(monthly_budget, 2),
                    }
                )
    return pd.DataFrame(rows)


def _build_fx_rates(rng: np.random.Generator) -> pd.DataFrame:
    """Generate monthly FX rates with mild random walk."""
    rows = []
    current_rates = dict(BASE_FX_RATES)
    for period in PERIODS:
        period_str = str(period)
        for currency, base_rate in BASE_FX_RATES.items():
            # Random walk: ±2% monthly drift
            drift = float(rng.normal(0, 0.02))
            current_rates[currency] = current_rates[currency] * (1 + drift)
            # Keep within 20% of base
            current_rates[currency] = max(
                base_rate * 0.8, min(base_rate * 1.2, current_rates[currency])
            )
            rows.append(
                {
                    "currency": currency,
                    "period": period_str,
                    "rate_to_usd": round(current_rates[currency], 6),
                }
            )
    return pd.DataFrame(rows)


def _build_actuals_usd(actuals: pd.DataFrame, fx_rates: pd.DataFrame) -> pd.DataFrame:
    """Convert actuals to USD using FX rates."""
    merged = actuals.merge(fx_rates, on=["currency", "period"], how="left")
    merged["amount_usd"] = (merged["amount_local"] * merged["rate_to_usd"]).round(2)
    result = (
        merged.groupby(["account_code", "cost_center", "department", "period"])
        .agg(amount_usd=("amount_usd", "sum"))
        .reset_index()
    )
    return result


def _build_pnl_report(
    actuals_usd: pd.DataFrame,
    budget: pd.DataFrame,
    account_mapping: pd.DataFrame,
) -> pd.DataFrame:
    """Build P&L report mart: actual vs budget by department × account_type × period."""
    # Add account_type to actuals
    actuals_with_type = actuals_usd.merge(
        account_mapping[["account_code", "account_type"]], on="account_code", how="left"
    )
    # Aggregate actuals by department × account_type × period
    actuals_agg = (
        actuals_with_type.groupby(["department", "account_type", "period"])
        .agg(actual_usd=("amount_usd", "sum"))
        .reset_index()
    )
    # Merge with budget
    pnl = actuals_agg.merge(
        budget[["department", "account_type", "period", "budget_amount"]],
        on=["department", "account_type", "period"],
        how="outer",
    )
    pnl["actual_usd"] = pnl["actual_usd"].fillna(0).round(2)
    pnl["budget_usd"] = pnl["budget_amount"].fillna(0).round(2)
    pnl["variance_usd"] = (pnl["actual_usd"] - pnl["budget_usd"]).round(2)
    pnl["variance_pct"] = (
        pnl["variance_usd"] / pnl["budget_usd"].replace(0, float("nan")) * 100
    ).round(2)
    pnl = pnl.drop(columns=["budget_amount"], errors="ignore")
    return pnl


# ── Anomaly Injection ─────────────────────────────────────────────────────────


def _inject_anomalies(
    ledger: pd.DataFrame,
    actuals: pd.DataFrame,
    budget: pd.DataFrame,
    fx_rates: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, list[dict]]:
    """Inject 5 seeded anomalies and return modified DataFrames + manifest."""
    manifest = []

    # Anomaly 1: COGS spike — +25% in 2 months for CC-300 (Engineering)
    mask = (
        actuals["cost_center"].eq("CC-300")
        & actuals["account_code"].isin(["5000", "5010"])
        & actuals["period"].isin(["2024-03", "2024-04"])
    )
    actuals.loc[mask, "amount_local"] = (actuals.loc[mask, "amount_local"] * 1.25).round(2)
    # Also inject into raw ledger
    ledger_mask = (
        ledger["cost_center"].eq("CC-300")
        & ledger["account_code"].isin(["5000", "5010"])
        & ledger["period"].isin(["2024-03", "2024-04"])
    )
    ledger.loc[ledger_mask, "amount_local"] = (
        ledger.loc[ledger_mask, "amount_local"] * 1.25
    ).round(2)
    manifest.append(
        {
            "anomaly_id": "A-001",
            "category": "COGS_ANOMALY",
            "description": "COGS spike: +25% in 2024-03 and 2024-04 for cost center CC-300",
            "affected_tables": "raw_ledger_entries,fct_actuals_monthly,int_actuals_usd,mart_pnl_report",
            "affected_dimensions": json.dumps(
                {"cost_center": "CC-300", "periods": ["2024-03", "2024-04"]}
            ),
        }
    )

    # Anomaly 2: Revenue drop — zeroed revenue for Sales dept in 2024-06
    mask2 = (
        actuals["department"].eq("Sales")
        & actuals["account_code"].isin(["4000", "4010"])
        & actuals["period"].eq("2024-06")
    )
    actuals.loc[mask2, "amount_local"] = 0.0
    ledger_mask2 = (
        ledger["department"].eq("Sales")
        & ledger["account_code"].isin(["4000", "4010"])
        & ledger["period"].eq("2024-06")
    )
    ledger.loc[ledger_mask2, "amount_local"] = 0.0
    manifest.append(
        {
            "anomaly_id": "A-002",
            "category": "REVENUE_ANOMALY",
            "description": "Revenue drop: zeroed revenue for Sales department in 2024-06",
            "affected_tables": "raw_ledger_entries,fct_actuals_monthly,int_actuals_usd,mart_pnl_report",
            "affected_dimensions": json.dumps({"department": "Sales", "periods": ["2024-06"]}),
        }
    )

    # Anomaly 3: FX anomaly — EUR/USD rate 15% off trend for 2024-07
    fx_mask = fx_rates["currency"].eq("EUR") & fx_rates["period"].eq("2024-07")
    fx_rates.loc[fx_mask, "rate_to_usd"] = (fx_rates.loc[fx_mask, "rate_to_usd"] * 1.15).round(6)
    manifest.append(
        {
            "anomaly_id": "A-003",
            "category": "FX_ANOMALY",
            "description": "FX anomaly: EUR/USD rate 15% above trend in 2024-07",
            "affected_tables": "fct_fx_rates,int_actuals_usd,mart_pnl_report",
            "affected_dimensions": json.dumps({"currency": "EUR", "periods": ["2024-07"]}),
        }
    )

    # Anomaly 4: Budget misalignment — Finance dept Q3 budget at 2x actual pattern
    budget_mask = budget["department"].eq("Finance") & budget["period"].isin(
        ["2024-07", "2024-08", "2024-09"]
    )
    budget.loc[budget_mask, "budget_amount"] = (budget.loc[budget_mask, "budget_amount"] * 2).round(
        2
    )
    manifest.append(
        {
            "anomaly_id": "A-004",
            "category": "BUDGET_MISALIGNMENT",
            "description": "Budget misalignment: Finance dept Q3 2024 budget set at 2x actual pattern",
            "affected_tables": "fct_budget_monthly,mart_pnl_report",
            "affected_dimensions": json.dumps(
                {"department": "Finance", "periods": ["2024-07", "2024-08", "2024-09"]}
            ),
        }
    )

    # Anomaly 5: Classification error — 50 ledger entries miscategorized (Revenue → Expense)
    # Only change account_type, keep original account_code so the mismatch is detectable
    # via JOIN on account_code → stg_account_mapping.account_type
    revenue_entries = ledger[ledger["account_type"] == "Revenue"].index
    if len(revenue_entries) >= 50:
        misclass_idx = revenue_entries[:50]
        ledger.loc[misclass_idx, "account_type"] = "OpEx"
    manifest.append(
        {
            "anomaly_id": "A-005",
            "category": "CLASSIFICATION_ERROR",
            "description": "Classification error: 50 ledger entries miscategorized from Revenue to OpEx",
            "affected_tables": "raw_ledger_entries,fct_actuals_monthly,int_actuals_usd,mart_pnl_report",
            "affected_dimensions": json.dumps(
                {"original_type": "Revenue", "misclassified_as": "OpEx", "entry_count": 50}
            ),
        }
    )

    return ledger, actuals, budget, fx_rates, manifest


# ── Seeding Pipeline ──────────────────────────────────────────────────────────


def seed_database(settings: Settings, force: bool = False) -> Path:
    """Generate the warehouse database with 8 tables + anomaly manifest.

    Args:
        settings: Application settings.
        force: If True, overwrite existing database.

    Returns:
        Path to the created database.
    """
    settings.ensure_dirs()
    db_path = settings.db_path

    if db_path.exists() and not force:
        raise FileExistsError(f"Database already exists at {db_path}. Use --force to overwrite.")

    rng = np.random.default_rng(seed=42)  # deterministic

    # Step 1: Generate base data
    ledger = _generate_raw_ledger(rng)
    account_mapping = _build_account_mapping()
    cost_center_mapping = _build_cost_center_mapping()
    actuals = _build_actuals_monthly(ledger)
    budget = _build_budget_monthly(rng)
    fx_rates = _build_fx_rates(rng)

    # Step 2: Inject anomalies (modifies in place)
    ledger, actuals, budget, fx_rates, manifest = _inject_anomalies(
        ledger, actuals, budget, fx_rates
    )

    # Step 3: Rebuild downstream tables after anomaly injection
    actuals_usd = _build_actuals_usd(actuals, fx_rates)
    pnl_report = _build_pnl_report(actuals_usd, budget, account_mapping)

    # Step 4: Write to SQLite
    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(str(db_path))
    try:
        ledger.to_sql("raw_ledger_entries", conn, index=False, if_exists="replace")
        account_mapping.to_sql("stg_account_mapping", conn, index=False, if_exists="replace")
        cost_center_mapping.to_sql(
            "stg_cost_center_mapping", conn, index=False, if_exists="replace"
        )
        actuals.to_sql("fct_actuals_monthly", conn, index=False, if_exists="replace")
        budget.to_sql("fct_budget_monthly", conn, index=False, if_exists="replace")
        fx_rates.to_sql("fct_fx_rates", conn, index=False, if_exists="replace")
        actuals_usd.to_sql("int_actuals_usd", conn, index=False, if_exists="replace")
        pnl_report.to_sql("mart_pnl_report", conn, index=False, if_exists="replace")

        # Seed manifest
        manifest_df = pd.DataFrame(manifest)
        manifest_df["seeded_at"] = "2024-01-01T00:00:00"  # fixed for determinism
        manifest_df.to_sql("seed_manifest", conn, index=False, if_exists="replace")

        # Add useful indexes
        cursor = conn.cursor()
        cursor.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_ledger_period ON raw_ledger_entries(period);
            CREATE INDEX IF NOT EXISTS idx_ledger_account ON raw_ledger_entries(account_code);
            CREATE INDEX IF NOT EXISTS idx_ledger_cc ON raw_ledger_entries(cost_center);
            CREATE INDEX IF NOT EXISTS idx_actuals_period ON fct_actuals_monthly(period);
            CREATE INDEX IF NOT EXISTS idx_actuals_cc ON fct_actuals_monthly(cost_center);
            CREATE INDEX IF NOT EXISTS idx_budget_dept ON fct_budget_monthly(department);
            CREATE INDEX IF NOT EXISTS idx_fx_currency ON fct_fx_rates(currency);
            CREATE INDEX IF NOT EXISTS idx_usd_period ON int_actuals_usd(period);
            CREATE INDEX IF NOT EXISTS idx_pnl_dept ON mart_pnl_report(department);
            CREATE INDEX IF NOT EXISTS idx_pnl_period ON mart_pnl_report(period);
            """
        )
        conn.commit()

        # Write latest pointer (text file instead of symlink for Windows)
        latest_file = settings.runs_dir / "latest"
        latest_file.write_text(str(db_path.resolve()))

        # Summary
        table_counts = {}
        for table in [
            "raw_ledger_entries",
            "stg_account_mapping",
            "stg_cost_center_mapping",
            "fct_actuals_monthly",
            "fct_budget_monthly",
            "fct_fx_rates",
            "int_actuals_usd",
            "mart_pnl_report",
            "seed_manifest",
        ]:
            count = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
            table_counts[table] = count

        # Write seed checksum
        db_bytes = db_path.read_bytes()
        checksum = hashlib.sha256(db_bytes).hexdigest()[:12]

    finally:
        conn.close()

    return db_path, table_counts, checksum
