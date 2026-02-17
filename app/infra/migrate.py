from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class MigrationFile:
    version: int
    name: str
    path: Path


def _parse_migration_file(path: Path) -> MigrationFile:
    prefix, _, _ = path.name.partition("_")
    if not prefix.isdigit():
        raise ValueError(f"Invalid migration filename: {path.name}")
    return MigrationFile(version=int(prefix), name=path.name, path=path)


def list_migrations(migrations_dir: Path) -> list[MigrationFile]:
    files = [_parse_migration_file(p) for p in migrations_dir.glob("*.sql")]
    return sorted(files, key=lambda item: item.version)


def ensure_migrations_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations(
          version INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          applied_at TEXT NOT NULL
        )
        """
    )
    con.commit()


def applied_versions(con: sqlite3.Connection) -> set[int]:
    rows = con.execute("SELECT version FROM schema_migrations").fetchall()
    return {int(row[0]) for row in rows}




def _execute_sql_script(con: sqlite3.Connection, sql: str) -> None:
    statement = ""
    for line in sql.splitlines(keepends=True):
        statement += line
        if sqlite3.complete_statement(statement):
            stmt = statement.strip()
            if stmt:
                con.execute(stmt)
            statement = ""

    rest = statement.strip()
    if rest:
        con.execute(rest)

def apply_migrations(con: sqlite3.Connection, migrations_dir: Path) -> int:
    ensure_migrations_table(con)

    migrations = list_migrations(migrations_dir)
    already_applied = applied_versions(con)
    pending = [m for m in migrations if m.version not in already_applied]

    if not pending:
        return 0

    con.execute("BEGIN IMMEDIATE")
    try:
        for migration in pending:
            sql = migration.path.read_text(encoding="utf-8")
            _execute_sql_script(con, sql)
            con.execute(
                "INSERT INTO schema_migrations(version, name, applied_at) VALUES(?, ?, datetime('now'))",
                (migration.version, migration.name),
            )
        con.commit()
        return len(pending)
    except Exception:
        con.rollback()
        raise
