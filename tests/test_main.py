from __future__ import annotations

import sqlite3
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


@pytest.fixture
def temp_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> sqlite3.Connection:
    monkeypatch.setattr(main, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(main, "LOCK_DIR", main.DATA_DIR / "lock")
    monkeypatch.setattr(main, "STAGING_DIR", main.DATA_DIR / "staging")
    monkeypatch.setattr(main, "LOG_DIR", main.DATA_DIR / "logs")
    monkeypatch.setattr(main, "DB_DIR", main.DATA_DIR / "db")
    monkeypatch.setattr(main, "DB_PATH", main.DB_DIR / "ygo.sqlite3")
    monkeypatch.setattr(main, "TEMP_IMAGE_DIR", main.DATA_DIR / "image" / "temp")
    monkeypatch.setattr(main, "FAILED_INGEST_DIR", main.DATA_DIR / "failed")
    monkeypatch.setattr(main, "LOCK_PATH", main.LOCK_DIR / "daemon.lock")

    con = main.db_connect()
    main.ensure_schema(con)
    yield con
    con.close()


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

def test_enqueue_need_fetch_cards_queues_only_candidates(temp_db: sqlite3.Connection) -> None:
    temp_db.execute(
        """
        INSERT INTO cards_raw(card_id, konami_id, json, content_hash, fetched_at, dbver_hash, source, fetch_status)
        VALUES(1, 1001, '{}', 'h1', '2026-01-01T00:00:00+00:00', NULL, 'queue', 'NEED_FETCH')
        """
    )
    temp_db.execute(
        """
        INSERT INTO cards_raw(card_id, konami_id, json, content_hash, fetched_at, dbver_hash, source, fetch_status)
        VALUES(2, 1002, '{}', 'h2', '2026-01-01T00:00:00+00:00', NULL, 'queue', 'OK')
        """
    )
    temp_db.commit()

    inserted = main.enqueue_need_fetch_cards(temp_db, limit=10)

    assert inserted == 1
    row = temp_db.execute("SELECT konami_id, state FROM request_queue ORDER BY id LIMIT 1").fetchone()
    assert row["konami_id"] == 1001
    assert row["state"] == "PENDING"


def test_step_download_images_skips_existing_file(temp_db: sqlite3.Connection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    image_dir = tmp_path / "data" / "image" / "card"
    temp_dir = tmp_path / "data" / "image" / "temp"
    image_dir.mkdir(parents=True, exist_ok=True)
    temp_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(main, "IMAGE_DIR", image_dir)
    monkeypatch.setattr(main, "TEMP_IMAGE_DIR", temp_dir)

    existing = image_dir / "100.jpg"
    existing.write_bytes(b"already")

    temp_db.execute(
        """
        INSERT INTO cards_raw(card_id, konami_id, json, content_hash, fetched_at, dbver_hash, source, fetch_status)
        VALUES(100, NULL, '{}', 'h100', datetime('now'), NULL, 'queue', 'OK')
        """
    )
    temp_db.execute(
        """
        INSERT INTO card_images(card_id, image_url, image_path, fetch_status, last_error, updated_at)
        VALUES(100, 'https://img/100.jpg', NULL, 'NEED_FETCH', NULL, datetime('now'))
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

    row = temp_db.execute("SELECT image_path, fetch_status FROM card_images WHERE card_id=100").fetchone()
    assert row is not None
    assert row["fetch_status"] == "OK"
    assert row["image_path"] == str(existing)
