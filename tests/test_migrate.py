from __future__ import annotations

import sqlite3
from pathlib import Path

from app.infra.migrate import apply_migrations


def test_apply_migrations_initializes_schema(tmp_path: Path) -> None:
    con = sqlite3.connect(tmp_path / "db.sqlite3")
    try:
        applied = apply_migrations(con, Path("app/db/migrations"))
        assert applied == 7

        tables = {
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "cards_raw" in tables
        assert "schema_migrations" in tables
        assert "invalid_ids" in tables
    finally:
        con.close()


def test_apply_migrations_is_idempotent(tmp_path: Path) -> None:
    con = sqlite3.connect(tmp_path / "db.sqlite3")
    try:
        assert apply_migrations(con, Path("app/db/migrations")) == 7
        assert apply_migrations(con, Path("app/db/migrations")) == 0

        count = con.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]
        assert count == 7
    finally:
        con.close()


def test_apply_migrations_rolls_back_on_failure(tmp_path: Path) -> None:
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    (migration_dir / "0001_init.sql").write_text("CREATE TABLE t1(id INTEGER PRIMARY KEY);", encoding="utf-8")
    (migration_dir / "0002_bad.sql").write_text(
        "CREATE TABLE t2(id INTEGER PRIMARY KEY);\nTHIS IS INVALID SQL;",
        encoding="utf-8",
    )

    con = sqlite3.connect(tmp_path / "db.sqlite3")
    try:
        try:
            apply_migrations(con, migration_dir)
            assert False, "expected migration to fail"
        except sqlite3.Error:
            pass

        rows = con.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()
        assert rows == []

        tables = {
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "t1" not in tables
        assert "t2" not in tables
    finally:
        con.close()
