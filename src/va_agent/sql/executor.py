"""SQL executor with validation, execution, and audit logging."""

from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from va_agent.models import QueryResult
from va_agent.sql.connection import open_readonly
from va_agent.sql.guard import SQLGuardError, validate_query


@dataclass
class AuditEntry:
    """A single audit log entry for a query execution."""

    sql: str
    execution_time_ms: float
    row_count: int
    truncated: bool
    error: str | None = None
    timestamp: float = 0.0


class SQLExecutor:
    """Validates, executes, and audits read-only SQL queries.

    Uses the 4-layer defense stack:
    1. URI mode=ro (connection.py)
    2. PRAGMA query_only (connection.py)
    3. set_authorizer (connection.py)
    4. sqlparse guard (guard.py)
    """

    def __init__(self, db_path: Path, max_rows: int = 500, query_timeout: int = 30):
        self.db_path = db_path
        self.max_rows = max_rows
        self.query_timeout = query_timeout
        self.audit_log: list[AuditEntry] = []
        self._conn: sqlite3.Connection | None = None
        self._execute_lock = threading.Lock()

    def _get_connection(self) -> sqlite3.Connection:
        """Get or create the read-only connection."""
        if self._conn is None:
            self._conn = open_readonly(self.db_path, busy_timeout=self.query_timeout)
        return self._conn

    def _execute_with_timeout(
        self,
        conn: sqlite3.Connection,
        sql: str,
        max_rows: int | None,
    ) -> tuple[list[str], list[dict], bool]:
        """Execute SQL with strict runtime timeout enforcement."""
        timed_out = False
        progress_handler_installed = False

        if self.query_timeout > 0:
            deadline = time.monotonic() + self.query_timeout

            def _progress_handler() -> int:
                nonlocal timed_out
                if time.monotonic() > deadline:
                    timed_out = True
                    return 1  # Abort query
                return 0

            conn.set_progress_handler(_progress_handler, 1000)
            progress_handler_installed = True

        try:
            cursor = conn.cursor()
            cursor.execute(sql)

            columns = [desc[0] for desc in cursor.description] if cursor.description else []

            if max_rows is None:
                rows_raw = cursor.fetchall()
                truncated = False
            else:
                rows_raw = cursor.fetchmany(max_rows + 1)
                truncated = len(rows_raw) > max_rows
                if truncated:
                    rows_raw = rows_raw[:max_rows]

            rows = [dict(zip(columns, row)) for row in rows_raw]
            return columns, rows, truncated

        except sqlite3.Error as e:
            if timed_out:
                raise sqlite3.OperationalError(
                    f"Query exceeded timeout of {self.query_timeout}s"
                ) from e
            raise
        finally:
            if progress_handler_installed:
                conn.set_progress_handler(None, 0)

    def execute(self, sql: str, *, record_audit: bool = True) -> QueryResult:
        """Validate and execute a SQL query.

        Args:
            sql: The SQL query to execute.
            record_audit: Whether to append this query to the audit log.

        Returns:
            QueryResult with columns, rows, and metadata.
        """
        start = time.perf_counter()
        audit = AuditEntry(sql=sql, execution_time_ms=0, row_count=0, truncated=False)
        audit.timestamp = time.time()

        try:
            # Layer 4: sqlparse guard
            validated_sql = validate_query(sql)
            with self._execute_lock:
                conn = self._get_connection()
                columns, rows, truncated = self._execute_with_timeout(
                    conn=conn,
                    sql=validated_sql,
                    max_rows=self.max_rows,
                )

            elapsed = (time.perf_counter() - start) * 1000
            audit.execution_time_ms = elapsed
            audit.row_count = len(rows)
            audit.truncated = truncated
            if record_audit:
                self.audit_log.append(audit)

            return QueryResult(
                sql=validated_sql,
                columns=columns,
                rows=rows,
                row_count=len(rows),
                truncated=truncated,
                execution_time_ms=elapsed,
            )

        except SQLGuardError as e:
            elapsed = (time.perf_counter() - start) * 1000
            audit.execution_time_ms = elapsed
            audit.error = str(e)
            if record_audit:
                self.audit_log.append(audit)
            return QueryResult(sql=sql, execution_time_ms=elapsed, error=str(e))

        except sqlite3.Error as e:
            elapsed = (time.perf_counter() - start) * 1000
            audit.execution_time_ms = elapsed
            audit.error = str(e)
            if record_audit:
                self.audit_log.append(audit)
            return QueryResult(sql=sql, execution_time_ms=elapsed, error=str(e))

    def get_total_row_count(self, sql: str) -> tuple[int | None, str | None]:
        """Return the exact total row count for a SELECT query.

        Args:
            sql: Query to count (must be SELECT/WITH).

        Returns:
            Tuple of (row_count, error). row_count is None if count failed.
        """
        try:
            validated_sql = validate_query(sql)
            count_sql = (
                "SELECT COUNT(*) AS total_rows FROM "
                f"({validated_sql.rstrip(';').strip()}) AS __va_count_subquery"
            )
            with self._execute_lock:
                conn = self._get_connection()
                _, rows, _ = self._execute_with_timeout(
                    conn=conn,
                    sql=count_sql,
                    max_rows=1,
                )

            if not rows:
                return 0, None
            return int(rows[0]["total_rows"]), None
        except (SQLGuardError, sqlite3.Error) as e:
            return None, str(e)

    def get_table_names(self) -> list[str]:
        """Get all table names from the database."""
        result = self.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        return [row["name"] for row in result.rows]

    def get_table_schema(self, table_name: str) -> list[dict]:
        """Get column info for a table using PRAGMA-style query.

        Note: We use a SELECT on sqlite_master + parse, since PRAGMA is blocked.
        """
        result = self.execute(
            f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'"  # noqa: S608
        )
        if result.rows:
            return [{"create_sql": result.rows[0].get("sql", "")}]
        return []

    def get_audit_entries(self) -> list[dict]:
        """Return the audit log as a list of dicts."""
        return [
            {
                "sql": e.sql,
                "execution_time_ms": e.execution_time_ms,
                "row_count": e.row_count,
                "truncated": e.truncated,
                "error": e.error,
                "timestamp": e.timestamp,
            }
            for e in self.audit_log
        ]

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
