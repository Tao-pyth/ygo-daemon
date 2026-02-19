from __future__ import annotations


def resolve_threshold(accept_thresholds: dict[str, int], category: str) -> int:
    defaults = {
        "cost_patterns": 2,
        "action_patterns": 3,
        "trigger_patterns": 4,
        "restriction_patterns": 2,
        "condition_patterns": 4,
        "unclassified_patterns": 6,
    }
    return int(accept_thresholds.get(category, defaults.get(category, 3)))


def should_auto_reject(category: str, template: str) -> bool:
    if category in {"condition_patterns", "trigger_patterns"} and template in {"if", "when"}:
        return True
    if len(template.split()) <= 2 and "{" not in template:
        return True
    return False


def apply_status_rules(*, count: int, status: str, category: str, template: str, threshold: int) -> str:
    if status != "candidate":
        return status
    if should_auto_reject(category, template):
        return "rejected"
    if count >= threshold:
        return "accepted"
    return status
