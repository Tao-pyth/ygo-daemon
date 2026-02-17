from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import requests
from requests import Response
from requests.exceptions import RequestException

API_CARDINFO = "https://db.ygoprodeck.com/api/v7/cardinfo.php"
DEFAULT_TIMEOUT_SEC = 15
DEFAULT_MAX_RETRIES = 2
DEFAULT_DETAIL_PARAMS: dict[str, str] = {"misc": "yes"}
DEFAULT_SEARCH_PARAM = "fname"


@dataclass(frozen=True)
class CardDTO:
    card_id: int
    konami_id: int | None
    name: str | None
    type: str | None
    race: str | None
    attribute: str | None
    level: int | None
    desc: str | None
    image_url: str | None
    raw_json: dict[str, Any]


@dataclass
class FetchSummary:
    cards_total: int = 0
    cards_upserted: int = 0
    images_downloaded: int = 0
    errors: list[dict[str, str]] = field(default_factory=list)


class KeywordFetchError(RuntimeError):
    pass


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return int(value)
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _extract_konami_id(card: dict[str, Any]) -> int | None:
    top_level = _to_int(card.get("konami_id"))
    if top_level is not None:
        return top_level

    misc_info = card.get("misc_info")
    if isinstance(misc_info, list) and misc_info:
        first = misc_info[0]
        if isinstance(first, dict):
            return _to_int(first.get("konami_id"))
    return None


def _extract_image_url(card: dict[str, Any]) -> str | None:
    images = card.get("card_images")
    if isinstance(images, list) and images:
        first = images[0]
        if isinstance(first, dict):
            value = first.get("image_url")
            if isinstance(value, str) and value:
                return value
    return None


def parse_cards(response_json: dict[str, Any]) -> list[CardDTO]:
    data = response_json.get("data")
    if not isinstance(data, list):
        return []

    cards: list[CardDTO] = []
    for raw_card in data:
        if not isinstance(raw_card, dict):
            continue
        card_id = _to_int(raw_card.get("id"))
        if card_id is None:
            continue

        cards.append(
            CardDTO(
                card_id=card_id,
                konami_id=_extract_konami_id(raw_card),
                name=raw_card.get("name") if isinstance(raw_card.get("name"), str) else None,
                type=raw_card.get("type") if isinstance(raw_card.get("type"), str) else None,
                race=raw_card.get("race") if isinstance(raw_card.get("race"), str) else None,
                attribute=raw_card.get("attribute") if isinstance(raw_card.get("attribute"), str) else None,
                level=_to_int(raw_card.get("level")) or _to_int(raw_card.get("linkval")),
                desc=raw_card.get("desc") if isinstance(raw_card.get("desc"), str) else None,
                image_url=_extract_image_url(raw_card),
                raw_json=raw_card,
            )
        )
    return cards


def _should_retry(response: Response | None, error: Exception | None) -> bool:
    if error is not None:
        return True
    if response is None:
        return False
    return response.status_code in (429, 500, 502, 503, 504)


def fetch_keyword_cards(
    keyword_text: str,
    session: requests.Session | None = None,
    detail_params: dict[str, str] | None = None,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    max_retries: int = DEFAULT_MAX_RETRIES,
    search_param: str = DEFAULT_SEARCH_PARAM,
) -> list[CardDTO]:
    if not keyword_text.strip():
        return []

    params = {search_param: keyword_text}
    params.update(detail_params or DEFAULT_DETAIL_PARAMS)

    client = session or requests.Session()

    last_error: Exception | None = None
    last_response: Response | None = None
    for attempt in range(max_retries + 1):
        try:
            response = client.get(API_CARDINFO, params=params, timeout=timeout_sec)
            last_response = response
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise KeywordFetchError("API response was not a JSON object")
            return parse_cards(payload)
        except RequestException as error:
            last_error = error
        except ValueError as error:
            raise KeywordFetchError(f"Failed to decode JSON response: {error}") from error

        if attempt >= max_retries or not _should_retry(last_response, last_error):
            break
        time.sleep(min(1.5 * (2**attempt), 4.0))

    raise KeywordFetchError(f"API request failed for keyword={keyword_text!r}: {last_error}")


def upsert_card(con: sqlite3.Connection, card: CardDTO) -> None:
    raw_text = json.dumps(card.raw_json, ensure_ascii=False)
    canonical = json.dumps(card.raw_json, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    content_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    con.execute(
        """
        INSERT INTO cards_raw(card_id, konami_id, json, content_hash, fetched_at, dbver_hash, source, fetch_status)
        VALUES(?, ?, ?, ?, datetime('now'), NULL, 'keyword', 'OK')
        ON CONFLICT(card_id) DO UPDATE SET
          konami_id=excluded.konami_id,
          json=excluded.json,
          content_hash=excluded.content_hash,
          fetched_at=excluded.fetched_at,
          source=excluded.source,
          fetch_status='OK'
        """,
        (card.card_id, card.konami_id, raw_text, content_hash),
    )

    con.execute(
        """
        INSERT INTO cards_index(
          card_id, konami_id, name, type, race, attribute, level, atk, def, archetype, ban_tcg, ban_ocg, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, datetime('now'))
        ON CONFLICT(card_id) DO UPDATE SET
          konami_id=excluded.konami_id,
          name=excluded.name,
          type=excluded.type,
          race=excluded.race,
          attribute=excluded.attribute,
          level=excluded.level,
          updated_at=excluded.updated_at
        """,
        (card.card_id, card.konami_id, card.name, card.type, card.race, card.attribute, card.level),
    )

    con.execute(
        """
        INSERT INTO card_images(card_id, image_url, image_path, fetch_status, last_error, updated_at)
        VALUES(?, ?, NULL, 'NEED_FETCH', NULL, datetime('now'))
        ON CONFLICT(card_id) DO UPDATE SET
          image_url=excluded.image_url,
          updated_at=excluded.updated_at,
          fetch_status=CASE
            WHEN card_images.image_path IS NULL OR card_images.image_path='' THEN 'NEED_FETCH'
            ELSE card_images.fetch_status
          END
        """,
        (card.card_id, card.image_url),
    )


def download_card_image(
    con: sqlite3.Connection,
    card: CardDTO,
    session: requests.Session,
    base_dir: Path,
) -> bool:
    if not card.image_url:
        con.execute(
            "UPDATE card_images SET fetch_status='ERROR', last_error='image_url missing', updated_at=datetime('now') WHERE card_id=?",
            (card.card_id,),
        )
        return False

    temp_dir = base_dir.parent / "temp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    base_dir.mkdir(parents=True, exist_ok=True)

    final_path = base_dir / f"{card.card_id}.jpg"
    temp_path = temp_dir / f"{card.card_id}.tmp"

    try:
        response = session.get(card.image_url, timeout=DEFAULT_TIMEOUT_SEC)
        response.raise_for_status()
        temp_path.write_bytes(response.content)
        temp_path.replace(final_path)
        con.execute(
            "UPDATE card_images SET image_path=?, fetch_status='OK', last_error=NULL, updated_at=datetime('now') WHERE card_id=?",
            (str(final_path), card.card_id),
        )
        return True
    except Exception as error:
        temp_path.unlink(missing_ok=True)
        con.execute(
            "UPDATE card_images SET fetch_status='ERROR', last_error=?, updated_at=datetime('now') WHERE card_id=?",
            (str(error)[:255], card.card_id),
        )
        return False


def fetch_and_store_by_keyword(
    con: sqlite3.Connection,
    keyword_text: str,
    image_base_dir: Path,
    session: requests.Session | None = None,
    detail_params: dict[str, str] | None = None,
) -> FetchSummary:
    summary = FetchSummary()
    client = session or requests.Session()

    cards = fetch_keyword_cards(keyword_text=keyword_text, session=client, detail_params=detail_params)
    summary.cards_total = len(cards)

    for card in cards:
        try:
            upsert_card(con, card)
            summary.cards_upserted += 1
        except Exception as error:
            summary.errors.append({"card_id": str(card.card_id), "reason": f"upsert: {error}"})

    con.commit()

    for card in cards:
        try:
            if download_card_image(con, card, client, image_base_dir):
                summary.images_downloaded += 1
            else:
                summary.errors.append({"card_id": str(card.card_id), "reason": "image download failed"})
        except Exception as error:
            summary.errors.append({"card_id": str(card.card_id), "reason": f"image: {error}"})

    con.commit()
    return summary
