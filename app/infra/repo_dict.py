from __future__ import annotations

import sqlite3

from app.service.dict_promote import apply_status_rules


LATEST_RULESET_KEY = "dict_build:latest_ruleset_id"


def get_latest_ruleset_id(con: sqlite3.Connection) -> int:
    row = con.execute("SELECT value FROM kv_store WHERE key=?", (LATEST_RULESET_KEY,)).fetchone()
    if row is None:
        return 2
    try:
        value = int(row["value"])
    except (TypeError, ValueError):
        return 2
    return max(value, 1)


def set_latest_ruleset_id(con: sqlite3.Connection, ruleset_id: int) -> None:
    con.execute(
        "INSERT INTO kv_store(key,value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (LATEST_RULESET_KEY, str(ruleset_id)),
    )


def iter_target_cards(con: sqlite3.Connection, *, ruleset_id: int, batch_size: int) -> list[sqlite3.Row]:
    return list(
        con.execute(
            """
            SELECT c.card_id, c.fetched_at, c.json
            FROM cards_raw AS c
            WHERE NOT EXISTS (
              SELECT 1
              FROM dict_build_processed_cards AS p
              WHERE p.card_id = c.card_id AND p.ruleset_id = ?
            )
            ORDER BY c.fetched_at ASC, c.card_id ASC
            LIMIT ?
            """,
            (ruleset_id, batch_size),
        ).fetchall()
    )


def mark_card_processed(con: sqlite3.Connection, *, card_id: int, ruleset_id: int, processed_at: str) -> None:
    con.execute(
        """
        INSERT INTO dict_build_processed_cards(card_id, ruleset_id, processed_at)
        VALUES(?,?,?)
        ON CONFLICT(card_id, ruleset_id)
        DO UPDATE SET processed_at=excluded.processed_at
        """,
        (card_id, ruleset_id, processed_at),
    )


def upsert_phrase(
    con: sqlite3.Connection,
    *,
    ruleset_id: int,
    category: str,
    template: str,
    ruleset_version: str,
    captured_at: str,
) -> bool:
    exists = con.execute(
        "SELECT 1 FROM dsl_dictionary_patterns WHERE ruleset_id=? AND template=?",
        (ruleset_id, template),
    ).fetchone()
    con.execute(
        """
        INSERT INTO dsl_dictionary_patterns(
          ruleset_id, category, template, count, status, dict_ruleset_version, first_seen_at, last_seen_at, updated_at
        )
        VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(ruleset_id, template)
        DO UPDATE SET
          count=dsl_dictionary_patterns.count+1,
          category=excluded.category,
          last_seen_at=excluded.last_seen_at,
          updated_at=excluded.updated_at
        """,
        (ruleset_id, category, template, 1, "candidate", ruleset_version, captured_at, captured_at, captured_at),
    )
    return exists is None


def upsert_term(
    con: sqlite3.Connection,
    *,
    ruleset_id: int,
    term_type: str,
    normalized_term: str,
    placeholder: str,
    ruleset_version: str,
    captured_at: str,
) -> None:
    con.execute(
        """
        INSERT INTO dsl_dictionary_terms(
          ruleset_id, term_type, normalized_term, placeholder, count, status, dict_ruleset_version, first_seen_at, last_seen_at, updated_at
        )
        VALUES(?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(ruleset_id, term_type, normalized_term)
        DO UPDATE SET
          count=dsl_dictionary_terms.count+1,
          last_seen_at=excluded.last_seen_at,
          updated_at=excluded.updated_at
        """,
        (ruleset_id, term_type, normalized_term, placeholder, 1, "candidate", ruleset_version, captured_at, captured_at, captured_at),
    )


def apply_phrase_status_rules(
    con: sqlite3.Connection,
    *,
    ruleset_id: int,
    category: str,
    template: str,
    ruleset_version: str,
    threshold: int,
    captured_at: str,
) -> tuple[bool, bool]:
    row = con.execute(
        "SELECT count, status FROM dsl_dictionary_patterns WHERE ruleset_id=? AND template=?",
        (ruleset_id, template),
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
        "UPDATE dsl_dictionary_patterns SET status=?, updated_at=? WHERE ruleset_id=? AND template=?",
        (next_status, captured_at, ruleset_id, template),
    )
    return (next_status == "accepted", next_status == "rejected")


def get_ruleset_metrics(con: sqlite3.Connection, *, short_token_threshold: int = 4) -> list[dict[str, object]]:
    rulesets = con.execute(
        "SELECT DISTINCT ruleset_id FROM dsl_dictionary_patterns ORDER BY ruleset_id"
    ).fetchall()
    metrics: list[dict[str, object]] = []

    for row in rulesets:
        ruleset_id = int(row["ruleset_id"])
        total = int(
            con.execute(
                "SELECT COUNT(*) AS c FROM dsl_dictionary_patterns WHERE ruleset_id=?",
                (ruleset_id,),
            ).fetchone()["c"]
        )
        count_one = int(
            con.execute(
                "SELECT COUNT(*) AS c FROM dsl_dictionary_patterns WHERE ruleset_id=? AND count=1",
                (ruleset_id,),
            ).fetchone()["c"]
        )
        short_count = int(
            con.execute(
                "SELECT COUNT(*) AS c FROM dsl_dictionary_patterns WHERE ruleset_id=? AND ((LENGTH(template)-LENGTH(REPLACE(template, ' ', '')))+1) <= ?",
                (ruleset_id, short_token_threshold),
            ).fetchone()["c"]
        )

        category_rows = [
            dict(r)
            for r in con.execute(
                "SELECT category, COUNT(*) AS rows FROM dsl_dictionary_patterns WHERE ruleset_id=? GROUP BY category ORDER BY rows DESC",
                (ruleset_id,),
            ).fetchall()
        ]
        top_all = [
            dict(r)
            for r in con.execute(
                "SELECT template, count FROM dsl_dictionary_patterns WHERE ruleset_id=? ORDER BY count DESC, template ASC LIMIT 20",
                (ruleset_id,),
            ).fetchall()
        ]

        def _top_by_category(category: str) -> list[dict[str, object]]:
            return [
                dict(r)
                for r in con.execute(
                    "SELECT template, count FROM dsl_dictionary_patterns WHERE ruleset_id=? AND category=? ORDER BY count DESC, template ASC LIMIT 20",
                    (ruleset_id, category),
                ).fetchall()
            ]

        metrics.append(
            {
                "ruleset_id": ruleset_id,
                "total_rows": total,
                "count_eq_1_rows": count_one,
                "count_eq_1_ratio": (count_one / total) if total else 0.0,
                "short_rows": short_count,
                "short_ratio": (short_count / total) if total else 0.0,
                "category_rows": category_rows,
                "top20_all": top_all,
                "top20_trigger": _top_by_category("trigger_patterns"),
                "top20_action": _top_by_category("action_patterns"),
            }
        )

    return metrics
