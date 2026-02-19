from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import main


@pytest.fixture
def temp_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> sqlite3.Connection:
    monkeypatch.setattr(main, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(main, "LOCK_DIR", main.DATA_DIR / "lock")
    monkeypatch.setattr(main, "DICT_LOCK_DIR", main.DATA_DIR / "locks")
    monkeypatch.setattr(main, "STAGING_DIR", main.DATA_DIR / "staging")
    monkeypatch.setattr(main, "LOG_DIR", main.DATA_DIR / "logs")
    monkeypatch.setattr(main, "DB_DIR", main.DATA_DIR / "db")
    monkeypatch.setattr(main, "DB_PATH", main.DB_DIR / "ygo.sqlite3")
    monkeypatch.setattr(main, "TEMP_IMAGE_DIR", main.DATA_DIR / "image" / "temp")
    monkeypatch.setattr(main, "FAILED_INGEST_DIR", main.DATA_DIR / "failed")
    monkeypatch.setattr(main, "LOCK_PATH", main.LOCK_DIR / "daemon.lock")
    monkeypatch.setattr(main, "DICT_LOCK_PATH", main.DICT_LOCK_DIR / "dict_builder.lock")
    monkeypatch.setattr(main, "DICT_LOG_PATH", main.LOG_DIR / "dict_builder.log")

    con = main.db_connect()
    main.ensure_schema(con)
    yield con
    con.close()
