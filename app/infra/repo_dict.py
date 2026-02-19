from __future__ import annotations

import sqlite3

from app.service.dict_promote import apply_status_rules


def iter_target_cards(con: sqlite3.Connection, *, fetched_at: str, card_id: int, batch_size: int) -> list[sqlite3.Row]:
    return list(
        con.execute(
            """
            SELECT card_id, fetched_at, json
            FROM cards_raw
            WHERE fetched_at > ? OR (fetched_at = ? AND card_id > ?)
            ORDER BY fetched_at ASC, card_id ASC
            LIMIT ?
            """,
            (fetched_at, fetched_at, card_id, batch_size),
        ).fetchall()
    )


def upsert_phrase(
    con: sqlite3.Connection,
    *,
    category: str,
    template: str,
    ruleset_version: str,
    captured_at: str,
) -> bool:
    exists = con.execute(
        "SELECT 1 FROM dsl_dictionary_patterns WHERE category=? AND template=? AND dict_ruleset_version=?",
        (category, template, ruleset_version),
    ).fetchone()
    con.execute(
        """
        INSERT INTO dsl_dictionary_patterns(
          category, template, count, status, dict_ruleset_version, first_seen_at, last_seen_at, updated_at
        )
        VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(category, template, dict_ruleset_version)
        DO UPDATE SET
          count=dsl_dictionary_patterns.count+1,
          last_seen_at=excluded.last_seen_at,
          updated_at=excluded.updated_at
        """,
        (category, template, 1, "candidate", ruleset_version, captured_at, captured_at, captured_at),
    )
    return exists is None


def upsert_term(
    con: sqlite3.Connection,
    *,
    term_type: str,
    normalized_term: str,
    placeholder: str,
    ruleset_version: str,
    captured_at: str,
) -> None:
    con.execute(
        """
        INSERT INTO dsl_dictionary_terms(
          term_type, normalized_term, placeholder, count, status, dict_ruleset_version, first_seen_at, last_seen_at, updated_at
        )
        VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(term_type, normalized_term, dict_ruleset_version)
        DO UPDATE SET
          count=dsl_dictionary_terms.count+1,
          last_seen_at=excluded.last_seen_at,
          updated_at=excluded.updated_at
        """,
        (term_type, normalized_term, placeholder, 1, "candidate", ruleset_version, captured_at, captured_at, captured_at),
    )


def apply_phrase_status_rules(
    con: sqlite3.Connection,
    *,
    category: str,
    template: str,
    ruleset_version: str,
    threshold: int,
    captured_at: str,
) -> tuple[bool, bool]:
    row = con.execute(
        "SELECT count, status FROM dsl_dictionary_patterns WHERE category=? AND template=? AND dict_ruleset_version=?",
        (category, template, ruleset_version),
    ).fetchone()
    if row is None:
        return (False, False)

    count = int(row["count"])
    prev_status = str(row["status"])
    next_status = apply_status_rules(
        count=count,
        status=prev_status,
        category=category,
        template=template,
        threshold=threshold,
    )
    if next_status == prev_status:
        return (False, False)

    con.execute(
        "UPDATE dsl_dictionary_patterns SET status=?, updated_at=? WHERE category=? AND template=? AND dict_ruleset_version=?",
        (next_status, captured_at, category, template, ruleset_version),
    )
    return (next_status == "accepted", next_status == "rejected")


def load_dict_progress(con: sqlite3.Connection) -> tuple[str, int]:
    last_fetched_at = (
        con.execute("SELECT value FROM kv_store WHERE key='dict_builder_last_fetched_at'").fetchone() or {"value": ""}
    )["value"]
    last_card_id_text = (con.execute("SELECT value FROM kv_store WHERE key='dict_builder_last_card_id'").fetchone() or {"value": "0"})[
        "value"
    ]
    if not last_fetched_at:
        last_fetched_at = "1970-01-01T00:00:00+00:00"
    return str(last_fetched_at or ""), int(last_card_id_text or 0)


def save_dict_progress(con: sqlite3.Connection, *, last_fetched_at: str, last_card_id: int) -> None:
    con.execute(
        "INSERT INTO kv_store(key,value) VALUES('dict_builder_last_fetched_at', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (last_fetched_at,),
    )
    con.execute(
        "INSERT INTO kv_store(key,value) VALUES('dict_builder_last_card_id', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (str(last_card_id),),
    )
