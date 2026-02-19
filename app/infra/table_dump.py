from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path
from typing import Any


DEFAULT_DICT_DUMP_TABLES: tuple[str, ...] = (
    "dsl_dictionary_patterns",
    "dsl_dictionary_terms",
    "kv_store",
)

EXCLUDED_TABLES: set[str] = {
    "cards_raw",
    "cards_index",
}


class TableDumpError(ValueError):
    """管理テーブルダンプ時の入力エラー。"""


def parse_tables_arg(tables_text: str | None, *, default_tables: tuple[str, ...] = DEFAULT_DICT_DUMP_TABLES) -> list[str]:
    if tables_text is None or not tables_text.strip():
        return list(default_tables)
    return [name.strip() for name in tables_text.split(",") if name.strip()]


def validate_tables(con: sqlite3.Connection, tables: list[str]) -> list[str]:
    if not tables:
        raise TableDumpError("tables must not be empty")

    schema_rows = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    existing = {str(row["name"]) for row in schema_rows}

    normalized: list[str] = []
    for table in tables:
        if table in EXCLUDED_TABLES:
            raise TableDumpError(f"table is excluded from dump: {table}")
        if table not in existing:
            raise TableDumpError(f"table not found: {table}")
        normalized.append(table)

    return normalized


def _iter_rows(con: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    rows = con.execute(f"SELECT * FROM {table}").fetchall()
    return [dict(row) for row in rows]


def dump_tables(con: sqlite3.Connection, *, tables: list[str], out_path: Path, fmt: str) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    exported = 0
    if fmt == "jsonl":
        with out_path.open("w", encoding="utf-8") as f:
            for table in tables:
                for row in _iter_rows(con, table):
                    f.write(json.dumps({"table": table, "row": row}, ensure_ascii=False) + "\n")
                    exported += 1
        return exported

    if fmt == "csv":
        with out_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["table", "row_json"])
            writer.writeheader()
            for table in tables:
                for row in _iter_rows(con, table):
                    writer.writerow(
                        {
                            "table": table,
                            "row_json": json.dumps(row, ensure_ascii=False, sort_keys=True),
                        }
                    )
                    exported += 1
        return exported

    raise TableDumpError(f"unsupported format: {fmt}")
