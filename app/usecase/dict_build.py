from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path

from app.infra.lockfile import acquire_lock, now_iso, release_lock
from app.infra.loggers import configure_logger
from app.infra.repo_dict import (
    apply_phrase_status_rules,
    get_latest_ruleset_id,
    iter_target_cards,
    mark_card_processed,
    upsert_phrase,
    upsert_term,
)
from app.service.dict_classify import detect_category
from app.service.dict_promote import resolve_threshold
from app.service.dict_text import TARGETS, ZONES, normalize_template, split_sentences


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


def _extract_card_sentences(raw_json: str) -> list[str]:
    try:
        card = json.loads(raw_json)
    except json.JSONDecodeError:
        return []

    desc = card.get("desc")
    if not isinstance(desc, str) or not desc.strip():
        return []

    return split_sentences(desc)


def execute_dict_build(con: sqlite3.Connection, config: DictBuilderConfig) -> DictBuildStats:
    logger = configure_logger(config.log_path, config.log_level)
    started = time.monotonic()
    latest_ruleset_id = get_latest_ruleset_id(con)
    logger.info(
        "dict_build_start max_runtime_sec=%s batch_size=%s dry_run=%s latest_ruleset_id=%s",
        config.max_runtime_sec,
        config.batch_size,
        config.dry_run,
        latest_ruleset_id,
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
        while True:
            elapsed = time.monotonic() - started
            if elapsed >= config.max_runtime_sec:
                stop_reason = "max_runtime_reached"
                break

            rows = iter_target_cards(con, ruleset_id=latest_ruleset_id, batch_size=config.batch_size)
            if not rows:
                break

            for row in rows:
                captured_at = now_iso()
                templates = [_t for sentence in _extract_card_sentences(row["json"]) if (_t := normalize_template(sentence))]
                for template in templates:
                    decision = detect_category(template)
                    category = decision.category
                    category_stats.setdefault(category, {"new": 0, "updated": 0, "accepted": 0, "rejected": 0})

                    is_new = upsert_phrase(
                        con,
                        ruleset_id=latest_ruleset_id,
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

                    promoted, rejected = apply_phrase_status_rules(
                        con,
                        ruleset_id=latest_ruleset_id,
                        category=category,
                        template=template,
                        ruleset_version=config.ruleset_version,
                        threshold=resolve_threshold(config.accept_thresholds, category),
                        captured_at=captured_at,
                    )
                    if promoted:
                        promoted_phrases += 1
                        category_stats[category]["accepted"] += 1
                    if rejected:
                        rejected_phrases += 1
                        category_stats[category]["rejected"] += 1

                    logger.debug("dict_pattern_detected category=%s reason=%s template=%s", category, decision.reason, template)

                    for zone, placeholder in ZONES.items():
                        if placeholder in template:
                            upsert_term(
                                con,
                                ruleset_id=latest_ruleset_id,
                                term_type="zone_dictionary",
                                normalized_term=zone,
                                placeholder=placeholder,
                                ruleset_version=config.ruleset_version,
                                captured_at=captured_at,
                            )
                    for target, placeholder in TARGETS.items():
                        if placeholder in template:
                            upsert_term(
                                con,
                                ruleset_id=latest_ruleset_id,
                                term_type="target_dictionary",
                                normalized_term=target,
                                placeholder=placeholder,
                                ruleset_version=config.ruleset_version,
                                captured_at=captured_at,
                            )

                mark_card_processed(
                    con,
                    card_id=int(row["card_id"]),
                    ruleset_id=latest_ruleset_id,
                    processed_at=captured_at,
                )
                processed_cards += 1

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


def run_incremental_build(con: sqlite3.Connection, config: DictBuilderConfig) -> DictBuildStats:
    return execute_dict_build(con, config)
