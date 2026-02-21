from __future__ import annotations

import sqlite3
import json
from pathlib import Path
from typing import Any

import pytest
import requests

import main


class DummyResponse:
    def __init__(self, status_code: int, payload: dict[str, Any] | None = None) -> None:
        self.status_code = status_code
        self._payload = payload or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}")

    def json(self) -> dict[str, Any]:
        return self._payload


class DummySession:
    def __init__(self, steps: list[Any]) -> None:
        self.steps = steps
        self.calls = 0

    def get(self, url: str, params: dict[str, Any], timeout: int) -> DummyResponse:
        step = self.steps[self.calls]
        self.calls += 1
        if isinstance(step, Exception):
            raise step
        return step



def test_extract_konami_id_from_misc_info() -> None:
    card = {"misc_info": [{"konami_id": "1234"}]}
    assert main.extract_konami_id(card) == 1234


def test_get_json_retries_and_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    client = main.ApiClient()
    client.session = DummySession(
        [
            requests.Timeout("timeout"),
            DummyResponse(status_code=200, payload={"ok": True}),
        ]
    )
    monkeypatch.setattr(main, "sleep_rate", lambda: None)
    monkeypatch.setattr(main.time, "sleep", lambda _: None)

    result = client._get_json("https://example.test", {"misc": "yes"})

    assert result == {"ok": True}
    assert client.api_calls == 2


def test_get_json_fails_after_max_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    client = main.ApiClient()
    client.session = DummySession([requests.Timeout("timeout")] * main.RETRY_MAX_ATTEMPTS)
    monkeypatch.setattr(main, "sleep_rate", lambda: None)
    monkeypatch.setattr(main.time, "sleep", lambda _: None)

    with pytest.raises(RuntimeError, match="retries exhausted"):
        client._get_json("https://example.test", {"misc": "yes"})


def test_queue_mark_retry_sets_error_for_next_run(temp_db: sqlite3.Connection) -> None:
    main.queue_add(temp_db, konami_id=1234, keyword=None)
    row = main.queue_pick_next(temp_db)
    assert row is not None

    main.queue_mark_retry(temp_db, int(row["id"]), "network failed")

    pending = temp_db.execute("SELECT COUNT(*) AS c FROM request_queue WHERE state='PENDING'").fetchone()
    errors = temp_db.execute("SELECT COUNT(*) AS c FROM request_queue WHERE state='ERROR'").fetchone()
    assert pending["c"] == 0
    assert errors["c"] == 1




def test_queue_add_accepts_keyword(temp_db: sqlite3.Connection) -> None:
    main.queue_add(temp_db, konami_id=None, keyword="Blue-Eyes")

    row = temp_db.execute("SELECT konami_id, keyword, state FROM request_queue ORDER BY id DESC LIMIT 1").fetchone()
    assert row["konami_id"] is None
    assert row["keyword"] == "Blue-Eyes"
    assert row["state"] == "PENDING"


def test_cli_queue_add_requires_exclusive_arg() -> None:
    with pytest.raises(SystemExit):
        main.main(["queue-add", "--konami-id", "1", "--keyword", "Blue-Eyes"])

    with pytest.raises(SystemExit):
        main.main(["queue-add"])

def test_step_download_images_skips_existing_file(temp_db: sqlite3.Connection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    image_dir = tmp_path / "data" / "image" / "card"
    temp_dir = tmp_path / "data" / "image" / "temp"
    image_dir.mkdir(parents=True, exist_ok=True)
    temp_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(main, "IMAGE_DIR", image_dir)
    monkeypatch.setattr(main, "TEMP_IMAGE_DIR", temp_dir)

    existing = image_dir / "100.jpg"
    existing_cropped = image_dir / "100_cropped.jpg"
    existing.write_bytes(b"already")
    existing_cropped.write_bytes(b"already-cropped")

    temp_db.execute(
        """
        INSERT INTO cards_raw(card_id, konami_id, json, content_hash, fetched_at, dbver_hash, source, fetch_status)
        VALUES(100, NULL, '{}', 'h100', datetime('now'), NULL, 'queue', 'OK')
        """
    )
    temp_db.execute(
        """
        INSERT INTO card_images(
          card_id, image_url, image_url_cropped, image_path, image_path_cropped, fetch_status, last_error, updated_at
        )
        VALUES(100, 'https://img/100.jpg', 'https://img/100c.jpg', NULL, NULL, 'NEED_FETCH', NULL, datetime('now'))
        """
    )
    temp_db.commit()

    class NoCallSession:
        def get(self, *args: Any, **kwargs: Any) -> Any:
            raise AssertionError("download should be skipped for existing image")

    api = main.ApiClient()
    api.session = NoCallSession()  # type: ignore[assignment]

    downloaded = main.step_download_images(temp_db, api, limit=10)
    assert downloaded == 0

    row = temp_db.execute("SELECT image_path, image_path_cropped, fetch_status FROM card_images WHERE card_id=100").fetchone()
    assert row is not None
    assert row["fetch_status"] == "OK"
    assert row["image_path"] == str(existing)
    assert row["image_path_cropped"] == str(existing_cropped)


def test_is_valid_next_offset() -> None:
    assert main.is_valid_next_offset(100, 0)
    assert not main.is_valid_next_offset(None, 0)
    assert not main.is_valid_next_offset(-1, 0)
    assert not main.is_valid_next_offset(0, 0)


def test_step_fullsync_once_updates_offset(temp_db: sqlite3.Connection) -> None:
    main.kv_set(temp_db, "fullsync_offset", "0")
    main.kv_set(temp_db, "fullsync_num", "2")
    main.kv_set(temp_db, "fullsync_done", "0")

    class ApiStub:
        def cardinfo_fullsync_page(self, offset: int, num: int) -> main.ApiResult:
            assert offset == 0
            assert num == 2
            return main.ApiResult(
                data=[{"id": 1, "name": "A"}, {"id": 2, "name": "B"}],
                meta={"next_page_offset": 2},
                raw={},
            )

    ran, cards, upserts, next_offset = main.step_fullsync_once(temp_db, ApiStub())

    assert ran
    assert cards == 2
    assert upserts == 2
    assert next_offset == 2
    assert main.kv_get(temp_db, "fullsync_offset") == "2"
    assert main.kv_get(temp_db, "fullsync_done") == "0"


def test_step_fullsync_once_marks_done_on_invalid_next_offset(temp_db: sqlite3.Connection) -> None:
    main.kv_set(temp_db, "fullsync_offset", "5")
    main.kv_set(temp_db, "fullsync_num", "2")
    main.kv_set(temp_db, "fullsync_done", "0")

    class ApiStub:
        def cardinfo_fullsync_page(self, offset: int, num: int) -> main.ApiResult:
            assert offset == 5
            return main.ApiResult(
                data=[{"id": 10, "name": "C"}],
                meta={"next_page_offset": 5},
                raw={},
            )

    ran, cards, upserts, next_offset = main.step_fullsync_once(temp_db, ApiStub())

    assert ran
    assert cards == 1
    assert upserts == 1
    assert next_offset is None
    assert main.kv_get(temp_db, "fullsync_done") == "1"


def test_cli_dict_build_forwards_options(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_cmd(max_runtime_sec: int | None, batch_size: int | None, dry_run: bool, log_level: str | None) -> int:
        captured["max_runtime_sec"] = max_runtime_sec
        captured["batch_size"] = batch_size
        captured["dry_run"] = dry_run
        captured["log_level"] = log_level
        return 0

    monkeypatch.setattr(main, "cmd_dict_build", fake_cmd)

    code = main.main(["dict-build", "--max-runtime-sec", "10", "--batch-size", "5", "--dry-run", "--log-level", "DEBUG"])

    assert code == 0
    assert captured == {
        "max_runtime_sec": 10,
        "batch_size": 5,
        "dry_run": True,
        "log_level": "DEBUG",
    }


def test_cli_dict_dump_forwards_options(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_cmd(tables: str | None, out: str, fmt: str) -> int:
        captured["tables"] = tables
        captured["out"] = out
        captured["fmt"] = fmt
        return 0

    monkeypatch.setattr(main, "cmd_dict_dump", fake_cmd)

    code = main.main(["dict-dump", "--tables", "kv_store", "--out", "data/exports/dump.jsonl", "--format", "jsonl"])

    assert code == 0
    assert captured == {
        "tables": "kv_store",
        "out": "data/exports/dump.jsonl",
        "fmt": "jsonl",
    }


def test_cmd_dict_dump_writes_jsonl(temp_db: sqlite3.Connection, tmp_path: Path) -> None:
    main.kv_set(temp_db, "sample_key", "sample_value")
    temp_db.commit()

    out = tmp_path / "exports" / "dict_dump.jsonl"
    code = main.cmd_dict_dump("kv_store", str(out), "jsonl")

    assert code == 0
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) >= 1
    first = json.loads(lines[0])
    assert first["table"] == "kv_store"
    assert "row" in first


def test_cmd_db_dump_rejects_cards_raw(temp_db: sqlite3.Connection, tmp_path: Path) -> None:
    out = tmp_path / "exports" / "db_dump.jsonl"

    code = main.cmd_db_dump("cards_raw", str(out), "jsonl")

    assert code == 2


def test_cli_dict_set_latest_ruleset_forwards_option(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_cmd(ruleset_id: int) -> int:
        captured["ruleset_id"] = ruleset_id
        return 0

    monkeypatch.setattr(main, "cmd_dict_set_latest_ruleset", fake_cmd)

    code = main.main(["dict-set-latest-ruleset", "--id", "3"])

    assert code == 0
    assert captured == {"ruleset_id": 3}


def test_cmd_dict_set_latest_ruleset_updates_kv(temp_db: sqlite3.Connection) -> None:
    code = main.cmd_dict_set_latest_ruleset(4)

    assert code == 0
    row = temp_db.execute("SELECT value FROM kv_store WHERE key='dict_build:latest_ruleset_id'").fetchone()
    assert row is not None
    assert row["value"] == "4"


def test_extract_cmd_header() -> None:
    line = "2026-01-01 [INFO] CMD=dict-build RUN_ID=20260101_000000 DB=data/db/ygo.sqlite"
    assert main.extract_cmd_header(line) == "CMD=dict-build"


def test_get_latest_log_file(tmp_path: Path) -> None:
    a = tmp_path / "logs" / "run.log"
    b = tmp_path / "logs" / "dict.log"
    a.parent.mkdir(parents=True, exist_ok=True)
    a.write_text("a", encoding="utf-8")
    b.write_text("b", encoding="utf-8")
    import os
    os.utime(b, (a.stat().st_atime + 10, a.stat().st_mtime + 10))
    assert main.get_latest_log_file(tmp_path / "logs") == b


def test_cli_status_calls_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"ok": False}

    def fake_status() -> int:
        called["ok"] = True
        return 0

    monkeypatch.setattr(main, "cmd_status", fake_status)
    assert main.main(["status"]) == 0
    assert called["ok"]
