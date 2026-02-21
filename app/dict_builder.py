from __future__ import annotations

"""Backward-compatible dict-builder imports.

Prefer the new layer modules:
- app.usecase.dict_build
- app.service.dict_text
- app.service.dict_classify
"""

from app.service.dict_classify import (  # noqa: F401
    CategoryDecision,
    CONDITION_RULES,
    PATTERN_RULES,
    PatternRule,
    detect_category,
)
from app.service.dict_text import TARGETS, normalize_template, split_sentences  # noqa: F401
from app.usecase.dict_build import DictBuildStats, DictBuilderConfig, execute_dict_build, run_incremental_build  # noqa: F401
