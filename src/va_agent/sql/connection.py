"""3-layer read-only SQLite connection.

Layer 1: URI mode=ro (open-time enforcement)
Layer 2: PRAGMA query_only=ON (runtime enforcement)
Layer 3: set_authorizer callback (per-operation enforcement)
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

# SQLite authorizer action codes
_SQLITE_OK = sqlite3.SQLITE_OK
_SQLITE_DENY = sqlite3.SQLITE_DENY

# Actions that are read-only
_READ_ACTIONS = frozenset(
    {
        sqlite3.SQLITE_READ,
        sqlite3.SQLITE_SELECT,
        sqlite3.SQLITE_FUNCTION,
    }
)


def _readonly_authorizer(action: int, arg1, arg2, db_name, trigger) -> int:  # noqa: ANN001
    """SQLite authorizer callback that denies all non-read operations."""
    if action in _READ_ACTIONS:
        return _SQLITE_OK
    return _SQLITE_DENY


def open_readonly(db_path: Path, busy_timeout: int = 30) -> sqlite3.Connection:
    """Open a read-only SQLite connection with 3 layers of protection.

    Args:
        db_path: Path to the SQLite database file.

    Returns:
        A read-only sqlite3.Connection.

    Raises:
        FileNotFoundError: If the database file doesn't exist.
        sqlite3.OperationalError: If the connection fails.
    """
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    # Layer 1: URI mode=ro
    # check_same_thread=False is safe here: the connection is read-only
    # (enforced by 3 layers), and LangGraph may invoke tools from worker threads.
    uri = f"file:{db_path.resolve().as_posix()}?mode=ro"
    conn = sqlite3.connect(
        uri,
        uri=True,
        timeout=max(1, busy_timeout),
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row

    # Layer 2: PRAGMA query_only (belt-and-suspenders)
    conn.execute("PRAGMA query_only = ON")

    # Layer 3: Authorizer callback
    conn.set_authorizer(_readonly_authorizer)

    return conn
