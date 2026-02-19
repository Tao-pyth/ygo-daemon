from __future__ import annotations

import sqlite3
from pathlib import Path

import main
from app.dict_builder import DictBuilderConfig, normalize_template, run_incremental_build


def _insert_card(con: sqlite3.Connection, card_id: int, fetched_at: str, desc: str) -> None:
    payload = '{"id": %d, "desc": "%s"}' % (card_id, desc.replace('"', '\\"'))
    con.execute(
        """
        INSERT INTO cards_raw(card_id, konami_id, json, content_hash, fetched_at, dbver_hash, source, fetch_status)
        VALUES(?, NULL, ?, ?, ?, NULL, 'queue', 'OK')
        """,
        (card_id, payload, f"h-{card_id}", fetched_at),
    )
    con.commit()


def _config(tmp_path: Path) -> DictBuilderConfig:
    return DictBuilderConfig(
        lock_path=tmp_path / "data" / "locks" / "dict_builder.lock",
        log_path=tmp_path / "data" / "logs" / "dict_builder.log",
        log_level="INFO",
        max_runtime_sec=60,
        batch_size=100,
        ruleset_version="v3.0",
    )


def test_normalize_template_placeholders() -> None:
    tpl = normalize_template("Add 2 monster from Deck to hand.")
    assert tpl == "add {N} {TARGET_MONSTER} from {ZONE_DECK} to {ZONE_HAND}."


def test_dict_builder_incremental_only(temp_db: sqlite3.Connection, tmp_path: Path) -> None:
    _insert_card(temp_db, 100, "2026-01-01T00:00:00+00:00", "Draw 2 cards.")
    first = run_incremental_build(temp_db, _config(tmp_path))
    assert first.processed_cards == 1
    assert first.new_phrases == 1

    second = run_incremental_build(temp_db, _config(tmp_path))
    assert second.processed_cards == 0

    _insert_card(temp_db, 200, "2026-01-01T00:00:01+00:00", "Once per turn: Pay 1000 LP; Draw 1 card.")
    third = run_incremental_build(temp_db, _config(tmp_path))
    assert third.processed_cards == 1
    assert third.new_phrases >= 1


def test_dict_builder_lock_skip(temp_db: sqlite3.Connection, tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    cfg.lock_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.lock_path.write_text("locked", encoding="utf-8")

    result = run_incremental_build(temp_db, cfg)

    assert result.stop_reason == "lock_exists"
