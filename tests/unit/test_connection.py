"""Unit tests for read-only connection enforcement."""

import sqlite3

import pytest

from va_agent.sql.connection import open_readonly


class TestReadOnlyConnection:
    def test_select_succeeds(self, test_db):
        conn = open_readonly(test_db)
        try:
            cursor = conn.execute("SELECT COUNT(*) FROM mart_pnl_report")
            row = cursor.fetchone()
            assert row[0] > 0
        finally:
            conn.close()

    def test_insert_blocked_by_authorizer(self, test_db):
        conn = open_readonly(test_db)
        try:
            with pytest.raises(sqlite3.DatabaseError):
                conn.execute("INSERT INTO mart_pnl_report VALUES (1,2,3,4,5,6,7)")
        finally:
            conn.close()

    def test_create_table_blocked(self, test_db):
        conn = open_readonly(test_db)
        try:
            with pytest.raises(sqlite3.DatabaseError):
                conn.execute("CREATE TABLE evil (id INTEGER)")
        finally:
            conn.close()

    def test_drop_table_blocked(self, test_db):
        conn = open_readonly(test_db)
        try:
            with pytest.raises(sqlite3.DatabaseError):
                conn.execute("DROP TABLE mart_pnl_report")
        finally:
            conn.close()

    def test_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            open_readonly(tmp_path / "nonexistent.db")

    def test_row_factory_is_row(self, test_db):
        conn = open_readonly(test_db)
        try:
            assert conn.row_factory == sqlite3.Row
        finally:
            conn.close()
