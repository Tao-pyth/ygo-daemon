from __future__ import annotations

import logging
from logging import FileHandler
from pathlib import Path


def configure_logger(log_path: Path, level: str) -> logging.Logger:
    logger = logging.getLogger("ygo-daemon.dict-builder")
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    handler = FileHandler(log_path, encoding="utf-8")
    handler.setLevel(getattr(logging, level.upper(), logging.INFO))
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    return logger
