"""Regression test: ensure analysis completes within runtime budget.

This test verifies that the database seeding and SQL operations
complete within acceptable time bounds (no API call involved).
"""

import time

import pytest

from va_agent.config import Settings
from va_agent.data.seed_generator import seed_database
from va_agent.sql.executor import SQLExecutor
from va_agent.sql.templates import TEMPLATES


pytestmark = [pytest.mark.regression]


class TestRuntimeBudget:
    def test_seed_under_30_seconds(self, tmp_path):
        """Database seeding should complete in under 30 seconds."""
        settings = Settings(
            db_path=tmp_path / "bench.db",
            runs_dir=tmp_path / "runs",
            cache_dir=tmp_path / "cache",
        )
        settings.ensure_dirs()

        start = time.perf_counter()
        seed_database(settings, force=True)
        elapsed = time.perf_counter() - start

        assert elapsed < 30, f"Seeding took {elapsed:.1f}s (budget: 30s)"

    def test_all_templates_under_1_second(self, test_db):
        """Each SQL template should execute in under 1 second."""
        exe = SQLExecutor(test_db)
        try:
            for name, template_fn in TEMPLATES.items():
                # Call with no args (use defaults)
                try:
                    sql = template_fn()
                except TypeError:
                    # Some templates require args
                    if name == "account_detail":
                        sql = template_fn("4000")
                    elif name == "cost_center_drill":
                        sql = template_fn("CC-100")
                    else:
                        continue

                start = time.perf_counter()
                result = exe.execute(sql)
                elapsed = time.perf_counter() - start

                assert result.error is None, f"Template {name} failed: {result.error}"
                assert elapsed < 1.0, f"Template {name} took {elapsed:.2f}s"
        finally:
            exe.close()

    def test_complex_query_under_5_seconds(self, test_db):
        """A complex analytical query should complete in under 5 seconds."""
        exe = SQLExecutor(test_db)
        try:
            sql = """
            WITH dept_variance AS (
                SELECT department, account_type, period,
                       SUM(variance_usd) as total_var,
                       SUM(budget_usd) as total_budget
                FROM mart_pnl_report
                GROUP BY department, account_type, period
            ),
            ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY department
                           ORDER BY ABS(total_var) DESC
                       ) as rn
                FROM dept_variance
            )
            SELECT * FROM ranked WHERE rn <= 3
            """
            start = time.perf_counter()
            result = exe.execute(sql)
            elapsed = time.perf_counter() - start

            assert result.error is None
            assert elapsed < 5.0, f"Complex query took {elapsed:.2f}s"
        finally:
            exe.close()
