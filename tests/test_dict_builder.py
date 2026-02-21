from __future__ import annotations

import sqlite3
from pathlib import Path

from app.dict_builder import DictBuilderConfig, detect_category, normalize_template, run_incremental_build, split_sentences


def _insert_card(con: sqlite3.Connection, card_id: int, fetched_at: str, desc: str, *, race: str | None = None, attribute: str | None = None) -> None:
    payload = {
        "id": card_id,
        "desc": desc,
    }
    if race is not None:
        payload["race"] = race
    if attribute is not None:
        payload["attribute"] = attribute

    import json

    con.execute(
        """
        INSERT INTO cards_raw(card_id, konami_id, json, content_hash, fetched_at, dbver_hash, source, fetch_status)
        VALUES(?, NULL, ?, ?, ?, NULL, 'queue', 'OK')
        """,
        (card_id, json.dumps(payload), f"h-{card_id}", fetched_at),
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
        accept_thresholds={"action_patterns": 2},
    )


def test_normalize_template_placeholders() -> None:
    tpl = normalize_template('Add 2 monster from Deck to hand. Then reveal "Dark Magician".')
    assert tpl == "add {NUM} {TARGET_MONSTER} from deck to hand. then reveal {CARDNAME}."


def test_normalize_template_with_json_vocab_terms() -> None:
    tpl = normalize_template("Target 1 LIGHT Spellcaster monster", race_terms={"Spellcaster"}, attribute_terms={"LIGHT"})
    assert tpl == "target {NUM} {ATTRIBUTE} {RACE} {TARGET_MONSTER}"


def test_split_sentences_keeps_connector_clause() -> None:
    sentences = split_sentences("Draw 1 card. Then, and if you do, destroy it.")
    assert len(sentences) == 1


def test_detect_category_avoids_short_trigger_words() -> None:
    decision = detect_category("when")
    assert decision.category == "unclassified_patterns"


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


def test_dict_builder_reprocesses_when_latest_ruleset_changes(temp_db: sqlite3.Connection, tmp_path: Path) -> None:
    _insert_card(temp_db, 500, "2026-01-01T00:00:05+00:00", "Draw 1 card.")

    first = run_incremental_build(temp_db, _config(tmp_path))
    assert first.processed_cards == 1

    temp_db.execute(
        "INSERT INTO kv_store(key, value) VALUES('dict_build:latest_ruleset_id', '3') ON CONFLICT(key) DO UPDATE SET value=excluded.value"
    )
    temp_db.commit()

    second = run_incremental_build(temp_db, _config(tmp_path))
    assert second.processed_cards == 1

    processed = temp_db.execute(
        "SELECT COUNT(*) AS c FROM dict_build_processed_cards WHERE card_id=500"
    ).fetchone()
    assert processed["c"] == 2


def test_dict_builder_lock_skip(temp_db: sqlite3.Connection, tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    cfg.lock_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.lock_path.write_text("locked", encoding="utf-8")

    result = run_incremental_build(temp_db, cfg)

    assert result.stop_reason == "lock_exists"


def test_unclassified_saved(temp_db: sqlite3.Connection, tmp_path: Path) -> None:
    _insert_card(temp_db, 300, "2026-01-01T00:00:02+00:00", "This sentence has no known pattern tokens")
    run_incremental_build(temp_db, _config(tmp_path))

    row = temp_db.execute(
        "SELECT count FROM dsl_dictionary_patterns WHERE ruleset_id=2 AND category='unclassified_patterns'"
    ).fetchone()
    assert row is not None


def test_candidate_promoted_to_accepted(temp_db: sqlite3.Connection, tmp_path: Path) -> None:
    _insert_card(temp_db, 400, "2026-01-01T00:00:03+00:00", "Draw 1 card.")
    _insert_card(temp_db, 401, "2026-01-01T00:00:04+00:00", "Draw 1 card.")
    run_incremental_build(temp_db, _config(tmp_path))

    row = temp_db.execute(
        "SELECT status FROM dsl_dictionary_patterns WHERE ruleset_id=2 AND category='action_patterns' AND template='draw {NUM} {TARGET_CARD}.'"
    ).fetchone()
    assert row is not None
    assert row["status"] == "accepted"
