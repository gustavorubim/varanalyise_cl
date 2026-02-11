"""SQL safety guard using sqlparse — Layer 4 defense.

Validates that queries are:
- Single SELECT statements only
- No DDL, DML, or dangerous operations
- No ATTACH, PRAGMA, LOAD_EXTENSION
- No multi-statement injection
"""

from __future__ import annotations

import re

import sqlparse

# Blacklisted patterns (case-insensitive)
_BLACKLIST_PATTERNS = [
    re.compile(r"\bINTO\b", re.IGNORECASE),
    re.compile(r"\bATTACH\b", re.IGNORECASE),
    re.compile(r"\bDETACH\b", re.IGNORECASE),
    re.compile(r"\bPRAGMA\b", re.IGNORECASE),
    re.compile(r"\bLOAD_EXTENSION\b", re.IGNORECASE),
    re.compile(r"\bCREATE\b", re.IGNORECASE),
    re.compile(r"\bDROP\b", re.IGNORECASE),
    re.compile(r"\bALTER\b", re.IGNORECASE),
    re.compile(r"\bINSERT\b", re.IGNORECASE),
    re.compile(r"\bUPDATE\b", re.IGNORECASE),
    re.compile(r"\bDELETE\b", re.IGNORECASE),
    re.compile(r"\bREPLACE\b", re.IGNORECASE),
    re.compile(r"\bGRANT\b", re.IGNORECASE),
    re.compile(r"\bREVOKE\b", re.IGNORECASE),
    re.compile(r"\bBEGIN\b", re.IGNORECASE),
    re.compile(r"\bCOMMIT\b", re.IGNORECASE),
    re.compile(r"\bROLLBACK\b", re.IGNORECASE),
    re.compile(r"\bSAVEPOINT\b", re.IGNORECASE),
    re.compile(r"\bRELEASE\b", re.IGNORECASE),
    re.compile(r"\bVACUUM\b", re.IGNORECASE),
    re.compile(r"\bREINDEX\b", re.IGNORECASE),
    re.compile(r"\bANALYZE\b", re.IGNORECASE),
]


class SQLGuardError(Exception):
    """Raised when SQL fails safety validation."""


def validate_query(sql: str) -> str:
    """Validate and normalize a SQL query for safe read-only execution.

    Args:
        sql: The SQL query string to validate.

    Returns:
        The normalized SQL string.

    Raises:
        SQLGuardError: If the query fails any safety check.
    """
    if not sql or not sql.strip():
        raise SQLGuardError("Empty query")

    stripped = sql.strip()

    # Parse with sqlparse
    statements = sqlparse.parse(stripped)

    # Check: single statement only
    # Filter out empty/whitespace-only statements
    non_empty = [s for s in statements if s.tokens and str(s).strip()]
    if len(non_empty) == 0:
        raise SQLGuardError("No valid SQL statement found")
    if len(non_empty) > 1:
        raise SQLGuardError(
            "Multiple statements detected — only single SELECT queries are allowed"
        )

    stmt = non_empty[0]
    stmt_type = stmt.get_type()

    # Check: must be SELECT (or None for complex CTEs — we check prefix below)
    if stmt_type and stmt_type.upper() != "SELECT":
        raise SQLGuardError(
            f"Statement type '{stmt_type}' not allowed — only SELECT is permitted"
        )

    # Normalize and check prefix
    normalized = str(stmt).strip()
    upper = normalized.upper().lstrip()

    # Must start with SELECT or WITH (for CTEs)
    if not (upper.startswith("SELECT") or upper.startswith("WITH")):
        raise SQLGuardError(
            "Query must start with SELECT or WITH (CTE) — "
            f"found: {upper[:30]}..."
        )

    # Check: no semicolons in the middle (multi-statement injection)
    # Allow trailing semicolon only
    body = normalized.rstrip(";").strip()
    if ";" in body:
        raise SQLGuardError("Semicolons within query body are not allowed (multi-statement risk)")

    # Check: blacklist patterns
    for pattern in _BLACKLIST_PATTERNS:
        match = pattern.search(body)
        if match:
            # Allow certain keywords in specific contexts
            keyword = match.group(0).upper()

            # REPLACE is OK inside function calls like REPLACE(col, 'a', 'b')
            if keyword == "REPLACE" and re.search(
                r"\bREPLACE\s*\(", body, re.IGNORECASE
            ):
                continue

            # ANALYZE is OK if it's part of a column/table name
            # but not as a standalone statement keyword
            if keyword == "ANALYZE" and not upper.startswith("ANALYZE"):
                continue

            raise SQLGuardError(
                f"Blocked keyword '{keyword}' detected — "
                "only read-only SELECT queries are allowed"
            )

    return normalized
