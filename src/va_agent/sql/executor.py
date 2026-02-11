"""SQL executor with validation, execution, and audit logging."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass, field
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

    def _get_connection(self) -> sqlite3.Connection:
        """Get or create the read-only connection."""
        if self._conn is None:
            self._conn = open_readonly(self.db_path)
        return self._conn

    def execute(self, sql: str) -> QueryResult:
        """Validate and execute a SQL query.

        Args:
            sql: The SQL query to execute.

        Returns:
            QueryResult with columns, rows, and metadata.
        """
        start = time.perf_counter()
        audit = AuditEntry(sql=sql, execution_time_ms=0, row_count=0, truncated=False)
        audit.timestamp = time.time()

        try:
            # Layer 4: sqlparse guard
            validated_sql = validate_query(sql)

            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(validated_sql)

            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows_raw = cursor.fetchmany(self.max_rows + 1)

            truncated = len(rows_raw) > self.max_rows
            if truncated:
                rows_raw = rows_raw[: self.max_rows]

            rows = [dict(zip(columns, row)) for row in rows_raw]

            elapsed = (time.perf_counter() - start) * 1000
            audit.execution_time_ms = elapsed
            audit.row_count = len(rows)
            audit.truncated = truncated
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
            self.audit_log.append(audit)
            return QueryResult(sql=sql, execution_time_ms=elapsed, error=str(e))

        except sqlite3.Error as e:
            elapsed = (time.perf_counter() - start) * 1000
            audit.execution_time_ms = elapsed
            audit.error = str(e)
            self.audit_log.append(audit)
            return QueryResult(sql=sql, execution_time_ms=elapsed, error=str(e))

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
