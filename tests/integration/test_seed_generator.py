"""Integration tests for the seed generator pipeline."""

import sqlite3

import pytest

from va_agent.config import Settings
from va_agent.data.seed_generator import seed_database


@pytest.fixture(scope="module")
def seeded_db(tmp_path_factory):
    """Seed a fresh database for integration testing."""
    tmp = tmp_path_factory.mktemp("seed_test")
    settings = Settings(
        db_path=tmp / "test_warehouse.db",
        runs_dir=tmp / "runs",
        cache_dir=tmp / "cache",
    )
    settings.ensure_dirs()
    db_path, counts, checksum = seed_database(settings, force=True)
    return db_path, counts, checksum


class TestSeedPipeline:
    def test_database_created(self, seeded_db):
        db_path, _, _ = seeded_db
        assert db_path.exists()

    def test_all_tables_exist(self, seeded_db):
        db_path, _, _ = seeded_db
        conn = sqlite3.connect(str(db_path))
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = {row[0] for row in cursor.fetchall()}
        conn.close()

        expected = {
            "raw_ledger_entries",
            "stg_account_mapping",
            "stg_cost_center_mapping",
            "fct_actuals_monthly",
            "fct_budget_monthly",
            "fct_fx_rates",
            "int_actuals_usd",
            "mart_pnl_report",
            "seed_manifest",
        }
        assert expected.issubset(tables)

    def test_table_counts(self, seeded_db):
        _, counts, _ = seeded_db
        assert counts["raw_ledger_entries"] > 1000
        assert counts["stg_account_mapping"] == 8
        assert counts["stg_cost_center_mapping"] == 5
        assert counts["fct_actuals_monthly"] > 100
        assert counts["fct_budget_monthly"] > 100
        assert counts["fct_fx_rates"] > 50
        assert counts["int_actuals_usd"] > 100
        assert counts["mart_pnl_report"] > 100
        assert counts["seed_manifest"] == 5

    def test_five_anomalies_in_manifest(self, seeded_db):
        db_path, _, _ = seeded_db
        conn = sqlite3.connect(str(db_path))
        cursor = conn.execute("SELECT anomaly_id, category FROM seed_manifest")
        anomalies = cursor.fetchall()
        conn.close()

        assert len(anomalies) == 5
        ids = {a[0] for a in anomalies}
        assert ids == {"A-001", "A-002", "A-003", "A-004", "A-005"}

    def test_anomaly_categories(self, seeded_db):
        db_path, _, _ = seeded_db
        conn = sqlite3.connect(str(db_path))
        cursor = conn.execute("SELECT category FROM seed_manifest ORDER BY anomaly_id")
        categories = [row[0] for row in cursor.fetchall()]
        conn.close()

        assert "COGS_ANOMALY" in categories
        assert "REVENUE_ANOMALY" in categories
        assert "FX_ANOMALY" in categories
        assert "BUDGET_MISALIGNMENT" in categories
        assert "CLASSIFICATION_ERROR" in categories

    def test_checksum_is_deterministic(self, tmp_path):
        """Same seed should produce same checksum."""
        s1 = Settings(
            db_path=tmp_path / "db1.db",
            runs_dir=tmp_path / "r1",
            cache_dir=tmp_path / "c1",
        )
        s1.ensure_dirs()
        _, _, c1 = seed_database(s1, force=True)

        s2 = Settings(
            db_path=tmp_path / "db2.db",
            runs_dir=tmp_path / "r2",
            cache_dir=tmp_path / "c2",
        )
        s2.ensure_dirs()
        _, _, c2 = seed_database(s2, force=True)

        assert c1 == c2

    def test_force_flag_required(self, seeded_db, tmp_path_factory):
        """Without force, re-seeding should raise."""
        db_path, _, _ = seeded_db
        settings = Settings(db_path=db_path)
        with pytest.raises(FileExistsError):
            seed_database(settings, force=False)

    def test_cogs_spike_present(self, seeded_db):
        """Anomaly 1: COGS spike should be documented in manifest.

        Since the spike is applied to aggregated actuals but base amounts
        are random, we verify via the seed_manifest that the anomaly was
        injected, and check data integrity (non-zero values exist).
        """
        db_path, _, _ = seeded_db
        conn = sqlite3.connect(str(db_path))

        # Verify manifest documents the anomaly
        cursor = conn.execute("SELECT description FROM seed_manifest WHERE anomaly_id = 'A-001'")
        row = cursor.fetchone()
        assert row is not None
        assert "COGS" in row[0] and "CC-300" in row[0]

        # Verify data exists for the affected dimensions
        cursor = conn.execute(
            """
            SELECT COUNT(*) as cnt, SUM(amount_local) as total
            FROM fct_actuals_monthly
            WHERE cost_center = 'CC-300' AND account_code IN ('5000', '5010')
              AND period IN ('2024-03', '2024-04')
            """
        )
        row = cursor.fetchone()
        conn.close()
        assert row[0] > 0, "No COGS data for spike months"
        assert row[1] > 0, "COGS total should be positive"
