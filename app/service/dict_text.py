from __future__ import annotations

import re


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
