from __future__ import annotations

import json
import logging
import re
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path


ZONES: dict[str, str] = {
    "extra deck": "{ZONE_EXTRA_DECK}",
    "deck": "{ZONE_DECK}",
    "gy": "{ZONE_GRAVE}",
    "graveyard": "{ZONE_GRAVE}",
    "field": "{ZONE_FIELD}",
    "hand": "{ZONE_HAND}",
    "banished": "{ZONE_BANISHED}",
    "banish": "{ZONE_BANISHED}",
}

TARGETS: dict[str, str] = {
    "face-up monster": "{TARGET_FACEUP_MONSTER}",
    "spell/trap": "{TARGET_SPELL_TRAP}",
    "monster": "{TARGET_MONSTER}",
    "card": "{TARGET_CARD}",
}

CATEGORY_PRIORITY: dict[str, int] = {
    "cost_patterns": 90,
    "action_patterns": 80,
    "restriction_patterns": 70,
    "trigger_patterns": 60,
    "condition_patterns": 40,
    "unclassified_patterns": 10,
}


@dataclass(frozen=True)
class PatternRule:
    category: str
    name: str
    regex: str


PATTERN_RULES: list[PatternRule] = [
    PatternRule("cost_patterns", "pay_lp", r"\bpay \{N\} lp\b"),
    PatternRule("cost_patterns", "discard_cost", r"\bdiscard \{N\} cards?\b"),
    PatternRule("cost_patterns", "tribute_cost", r"\btribute \{N\} monsters?\b"),
    PatternRule("action_patterns", "add_from_deck_to_hand", r"\badd \{N\} .* from \{ZONE_DECK\} to \{ZONE_HAND\}\b"),
    PatternRule("action_patterns", "special_summon_from_zone", r"\bspecial summon \{N\} .* from \{ZONE_[A-Z_]+\}\b"),
    PatternRule("action_patterns", "draw_cards", r"\bdraw \{N\} (cards?|\{TARGET_CARD\})(\.|\b)"),
    PatternRule("action_patterns", "destroy_target", r"\bdestroy \{N\} .*\b"),
    PatternRule("restriction_patterns", "once_per_turn", r"\bonce per turn\b"),
    PatternRule("restriction_patterns", "only_use_effect", r"\byou can only use this effect\b"),
    PatternRule("trigger_patterns", "normal_summoned", r"\bwhen this card is normal summoned\b"),
    PatternRule("trigger_patterns", "special_summoned", r"\bwhen this card is special summoned\b"),
    PatternRule("trigger_patterns", "sent_to_gy", r"\bif this card is sent to the \{ZONE_GRAVE\}\b"),
    PatternRule("trigger_patterns", "destroyed", r"\bif this card is destroyed\b"),
]

CONDITION_RULES: list[PatternRule] = [
    PatternRule("condition_patterns", "if_clause", r"\bif\b"),
    PatternRule("condition_patterns", "when_clause", r"\bwhen\b"),
]


@dataclass(frozen=True)
class CategoryDecision:
    category: str
    reason: str


@dataclass(frozen=True)
class DictBuildStats:
    processed_cards: int = 0
    new_phrases: int = 0
    updated_phrases: int = 0
    promoted_phrases: int = 0
    rejected_phrases: int = 0
    stop_reason: str = "completed"


@dataclass(frozen=True)
class DictBuilderConfig:
    lock_path: Path
    log_path: Path
    log_level: str
    max_runtime_sec: int
    batch_size: int
    ruleset_version: str
    dry_run: bool = False
    accept_thresholds: dict[str, int] = field(default_factory=dict)


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def configure_logger(log_path: Path, level: str) -> logging.Logger:
    logger = logging.getLogger("ygo-daemon.dict-builder")
    if logger.handlers:
        return logger

    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    handler = RotatingFileHandler(log_path, maxBytes=1_000_000, backupCount=5, encoding="utf-8")
    handler.setLevel(getattr(logging, level.upper(), logging.INFO))
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    return logger


def acquire_lock(lock_path: Path) -> bool:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with lock_path.open("x", encoding="utf-8") as lock_file:
            lock_file.write(now_iso())
        return True
    except FileExistsError:
        return False


def release_lock(lock_path: Path) -> None:
    try:
        lock_path.unlink(missing_ok=True)
    except Exception:
        pass


def split_sentences(text: str) -> list[str]:
    chunks = re.split(r"[\n\r]+|(?<=\.)\s+|(?<=;)\s+", text)
    return [c.strip() for c in chunks if c.strip()]


def normalize_template(sentence: str) -> str:
    normalized = sentence.lower()
    normalized = re.sub(r"\b\d+\b", "{N}", normalized)

    for zone, placeholder in sorted(ZONES.items(), key=lambda item: len(item[0]), reverse=True):
        normalized = re.sub(rf"\b{re.escape(zone)}\b", placeholder, normalized)

    for target, placeholder in sorted(TARGETS.items(), key=lambda item: len(item[0]), reverse=True):
        normalized = re.sub(rf"\b{re.escape(target)}\b", placeholder, normalized)

    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _decision_score(template: str, match_text: str, category: str) -> tuple[int, int, int]:
    return (
        len(match_text),
        template.count("{"),
        CATEGORY_PRIORITY.get(category, 0),
    )


def detect_category(template: str) -> CategoryDecision:
    best: tuple[tuple[int, int, int], CategoryDecision] | None = None
    for rule in PATTERN_RULES:
        match = re.search(rule.regex, template)
        if not match:
            continue
        score = _decision_score(template, match.group(0), rule.category)
        decision = CategoryDecision(rule.category, f"rule={rule.name} match_len={len(match.group(0))} placeholders={template.count('{')}")
        if best is None or score > best[0]:
            best = (score, decision)

    if best is not None:
        return best[1]

    for rule in CONDITION_RULES:
        if re.search(rule.regex, template):
            return CategoryDecision(rule.category, f"rule={rule.name}")

    return CategoryDecision("unclassified_patterns", "rule=fallback_unclassified")


def _upsert_phrase(
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


def _upsert_term(
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


def _iter_target_cards(con: sqlite3.Connection, *, fetched_at: str, card_id: int, batch_size: int) -> list[sqlite3.Row]:
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


def _extract_card_sentences(raw_json: str) -> list[str]:
    try:
        card = json.loads(raw_json)
    except json.JSONDecodeError:
        return []

    desc = card.get("desc")
    if not isinstance(desc, str) or not desc.strip():
        return []

    return split_sentences(desc)


def _resolve_threshold(config: DictBuilderConfig, category: str) -> int:
    defaults = {
        "cost_patterns": 2,
        "action_patterns": 3,
        "trigger_patterns": 4,
        "restriction_patterns": 2,
        "condition_patterns": 4,
        "unclassified_patterns": 6,
    }
    return int(config.accept_thresholds.get(category, defaults.get(category, 3)))


def _should_auto_reject(category: str, template: str) -> bool:
    if category in {"condition_patterns", "trigger_patterns"} and template in {"if", "when"}:
        return True
    if len(template.split()) <= 2 and "{" not in template:
        return True
    return False


def _apply_status_rules(
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
    status = str(row["status"])

    if status == "candidate" and _should_auto_reject(category, template):
        con.execute(
            "UPDATE dsl_dictionary_patterns SET status='rejected', updated_at=? WHERE category=? AND template=? AND dict_ruleset_version=?",
            (captured_at, category, template, ruleset_version),
        )
        return (False, True)

    if status == "candidate" and count >= threshold:
        con.execute(
            "UPDATE dsl_dictionary_patterns SET status='accepted', updated_at=? WHERE category=? AND template=? AND dict_ruleset_version=?",
            (captured_at, category, template, ruleset_version),
        )
        return (True, False)

    return (False, False)


def run_incremental_build(con: sqlite3.Connection, config: DictBuilderConfig) -> DictBuildStats:
    logger = configure_logger(config.log_path, config.log_level)
    started = time.monotonic()
    logger.info(
        "dict_build_start max_runtime_sec=%s batch_size=%s dry_run=%s",
        config.max_runtime_sec,
        config.batch_size,
        config.dry_run,
    )

    if not acquire_lock(config.lock_path):
        logger.info("dict_build_skip reason=lock_exists lock_path=%s", config.lock_path)
        return DictBuildStats(stop_reason="lock_exists")

    processed_cards = 0
    new_phrases = 0
    updated_phrases = 0
    promoted_phrases = 0
    rejected_phrases = 0
    stop_reason = "completed"
    category_stats: dict[str, dict[str, int]] = {}

    try:
        last_fetched_at = (
            con.execute("SELECT value FROM kv_store WHERE key='dict_builder_last_fetched_at'").fetchone() or {"value": ""}
        )["value"]
        last_card_id_text = (con.execute("SELECT value FROM kv_store WHERE key='dict_builder_last_card_id'").fetchone() or {"value": "0"})[
            "value"
        ]
        last_card_id = int(last_card_id_text or 0)
        last_fetched_at = str(last_fetched_at or "")

        if not last_fetched_at:
            last_fetched_at = "1970-01-01T00:00:00+00:00"

        while True:
            elapsed = time.monotonic() - started
            if elapsed >= config.max_runtime_sec:
                stop_reason = "max_runtime_reached"
                break

            rows = _iter_target_cards(con, fetched_at=last_fetched_at, card_id=last_card_id, batch_size=config.batch_size)
            if not rows:
                break

            for row in rows:
                captured_at = now_iso()
                templates = [_t for sentence in _extract_card_sentences(row["json"]) if (_t := normalize_template(sentence))]
                for template in templates:
                    decision = detect_category(template)
                    category = decision.category
                    category_stats.setdefault(category, {"new": 0, "updated": 0, "accepted": 0, "rejected": 0})

                    is_new = _upsert_phrase(
                        con,
                        category=category,
                        template=template,
                        ruleset_version=config.ruleset_version,
                        captured_at=captured_at,
                    )
                    if is_new:
                        new_phrases += 1
                        category_stats[category]["new"] += 1
                    else:
                        updated_phrases += 1
                        category_stats[category]["updated"] += 1

                    promoted, rejected = _apply_status_rules(
                        con,
                        category=category,
                        template=template,
                        ruleset_version=config.ruleset_version,
                        threshold=_resolve_threshold(config, category),
                        captured_at=captured_at,
                    )
                    if promoted:
                        promoted_phrases += 1
                        category_stats[category]["accepted"] += 1
                    if rejected:
                        rejected_phrases += 1
                        category_stats[category]["rejected"] += 1

                    logger.debug(
                        "dict_pattern_detected category=%s reason=%s template=%s",
                        category,
                        decision.reason,
                        template,
                    )

                    for zone, placeholder in ZONES.items():
                        if placeholder in template:
                            _upsert_term(
                                con,
                                term_type="zone_dictionary",
                                normalized_term=zone,
                                placeholder=placeholder,
                                ruleset_version=config.ruleset_version,
                                captured_at=captured_at,
                            )
                    for target, placeholder in TARGETS.items():
                        if placeholder in template:
                            _upsert_term(
                                con,
                                term_type="target_dictionary",
                                normalized_term=target,
                                placeholder=placeholder,
                                ruleset_version=config.ruleset_version,
                                captured_at=captured_at,
                            )

                processed_cards += 1
                last_fetched_at = str(row["fetched_at"])
                last_card_id = int(row["card_id"])

            con.execute(
                "INSERT INTO kv_store(key,value) VALUES('dict_builder_last_fetched_at', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (last_fetched_at,),
            )
            con.execute(
                "INSERT INTO kv_store(key,value) VALUES('dict_builder_last_card_id', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(last_card_id),),
            )
            if config.dry_run:
                con.rollback()
            else:
                con.commit()

    except Exception:
        logger.exception("dict_build_exception")
        stop_reason = "exception"
    finally:
        release_lock(config.lock_path)

    elapsed_sec = time.monotonic() - started
    logger.info(
        "dict_build_finish stop_reason=%s processed_cards=%s new_phrases=%s updated_phrases=%s promoted_phrases=%s rejected_phrases=%s elapsed_sec=%.3f",
        stop_reason,
        processed_cards,
        new_phrases,
        updated_phrases,
        promoted_phrases,
        rejected_phrases,
        elapsed_sec,
    )
    for category, stats in sorted(category_stats.items()):
        logger.info(
            "dict_build_category_summary category=%s new=%s updated=%s accepted=%s rejected=%s",
            category,
            stats["new"],
            stats["updated"],
            stats["accepted"],
            stats["rejected"],
        )

    return DictBuildStats(
        processed_cards=processed_cards,
        new_phrases=new_phrases,
        updated_phrases=updated_phrases,
        promoted_phrases=promoted_phrases,
        rejected_phrases=rejected_phrases,
        stop_reason=stop_reason,
    )
