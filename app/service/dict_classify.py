from __future__ import annotations

import re
from dataclasses import dataclass


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
