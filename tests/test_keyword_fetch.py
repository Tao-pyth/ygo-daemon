from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import requests

from app.infra.migrate import apply_migrations
from app.keyword_fetch import fetch_and_store_by_keyword, parse_cards


class DummyResponse:
    def __init__(self, status_code: int, payload: dict[str, Any] | None = None, content: bytes = b"") -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.content = content

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}")

    def json(self) -> dict[str, Any]:
        return self._payload


class DummySession:
    def __init__(self, responses: list[DummyResponse]) -> None:
        self._responses = responses
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, url: str, params: dict[str, Any] | None = None, timeout: int = 15) -> DummyResponse:
        self.calls.append((url, params or {}))
        return self._responses.pop(0)


def test_parse_cards_splits_per_card() -> None:
    payload = {
        "data": [
            {"id": 1, "name": "Blue-Eyes", "card_images": [{"image_url": "https://img/1.jpg"}]},
            {"id": 2, "name": "Dark Magician", "misc_info": [{"konami_id": "123"}]},
        ]
    }

    cards = parse_cards(payload)

    assert len(cards) == 2
    assert cards[0].card_id == 1
    assert cards[0].image_url == "https://img/1.jpg"
    assert cards[1].konami_id == 123


def test_fetch_and_store_by_keyword_upserts_and_downloads(tmp_path: Path) -> None:
    con = sqlite3.connect(tmp_path / "db.sqlite3")
    con.row_factory = sqlite3.Row
    apply_migrations(con, Path("app/db/migrations"))

    payload = {
        "data": [
            {
                "id": 100,
                "name": "Blue-Eyes White Dragon",
                "type": "Normal Monster",
                "race": "Dragon",
                "attribute": "LIGHT",
                "level": 8,
                "desc": "Legendary dragon.",
                "card_images": [{"image_url": "https://img/100.jpg"}],
            }
        ]
    }
    session = DummySession(
        [
            DummyResponse(200, payload=payload),
            DummyResponse(200, content=b"fake-image-binary"),
        ]
    )

    summary = fetch_and_store_by_keyword(
        con=con,
        keyword_text="Blue-Eyes",
        image_base_dir=tmp_path / "data" / "image" / "card",
        session=session,
    )

    assert summary.cards_total == 1
    assert summary.cards_upserted == 1
    assert summary.images_downloaded == 1

    raw = con.execute("SELECT card_id, source, fetch_status FROM cards_raw WHERE card_id=100").fetchone()
    assert raw is not None
    assert raw["source"] == "keyword"

    image = con.execute("SELECT image_path, fetch_status FROM card_images WHERE card_id=100").fetchone()
    assert image is not None
    assert image["fetch_status"] == "OK"
    assert Path(image["image_path"]).exists()

    search_call = session.calls[0]
    assert search_call[1]["misc"] == "yes"
    assert search_call[1]["fname"] == "Blue-Eyes"
