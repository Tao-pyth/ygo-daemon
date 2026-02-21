from __future__ import annotations

import re


CARDNAME_PATTERN = re.compile(r'"[^"]+"')
NUMBER_PATTERN = re.compile(r"\b\d+\b")
TOKEN_PATTERN = re.compile(r"[a-z0-9{}\-']+")

CONNECTOR_HINTS: tuple[str, ...] = (
    "and if you do",
    "then",
    "also",
    ", and",
)

ZONE_VARIANTS: dict[str, str] = {
    "gy": "graveyard",
    "graveyard": "graveyard",
    "grave": "graveyard",
    "hand": "hand",
    "deck": "deck",
    "extra deck": "extra deck",
    "banished": "banished",
    "banish": "banished",
    "field": "field",
}

TARGETS: dict[str, str] = {
    "face-up monster": "{TARGET_FACEUP_MONSTER}",
    "spell/trap": "{TARGET_SPELL_TRAP}",
    "monster": "{TARGET_MONSTER}",
    "card": "{TARGET_CARD}",
}


def split_sentences(text: str) -> list[str]:
    lines = [line.strip() for line in re.split(r"[\n\r]+", text) if line.strip()]
    sentences: list[str] = []

    for line in lines:
        parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+", line) if part.strip()]
        if not parts:
            continue
        sentence = parts[0]
        for part in parts[1:]:
            token_count = len(TOKEN_PATTERN.findall(part.lower()))
            tail = sentence.lower()
            should_attach = token_count < 4 or any(connector in part.lower() for connector in CONNECTOR_HINTS) or any(
                tail.endswith(marker) for marker in (";", ":", ",")
            )
            if should_attach:
                sentence = f"{sentence} {part}".strip()
            else:
                sentences.append(sentence)
                sentence = part
        sentences.append(sentence)

    merged: list[str] = []
    for sentence in sentences:
        token_count = len(TOKEN_PATTERN.findall(sentence.lower()))
        if token_count < 3 and merged:
            merged[-1] = f"{merged[-1]} {sentence}".strip()
        else:
            merged.append(sentence)
    return merged


def normalize_template(sentence: str, *, race_terms: set[str] | None = None, attribute_terms: set[str] | None = None) -> str:
    normalized = sentence.lower()
    normalized = CARDNAME_PATTERN.sub("{CARDNAME}", normalized)

    for race in sorted(race_terms or set(), key=len, reverse=True):
        normalized = re.sub(rf"\b{re.escape(race.lower())}\b", "{RACE}", normalized)
    for attribute in sorted(attribute_terms or set(), key=len, reverse=True):
        normalized = re.sub(rf"\b{re.escape(attribute.lower())}\b", "{ATTRIBUTE}", normalized)

    normalized = NUMBER_PATTERN.sub("{NUM}", normalized)

    for variant, canonical in sorted(ZONE_VARIANTS.items(), key=lambda item: len(item[0]), reverse=True):
        normalized = re.sub(rf"\b{re.escape(variant)}\b", canonical, normalized)

    for target, placeholder in sorted(TARGETS.items(), key=lambda item: len(item[0]), reverse=True):
        normalized = re.sub(rf"\b{re.escape(target)}\b", placeholder, normalized)

    normalized = re.sub(r"\s*([,;:.!?])\s*", r"\1 ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def token_count(text: str) -> int:
    return len(TOKEN_PATTERN.findall(text.lower()))
