from __future__ import annotations

import configparser
from dataclasses import dataclass
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT_DIR / "config"
HELP_DIR = CONFIG_DIR / "Help"
DEFAULT_CONFIG_PATH = CONFIG_DIR / "app.conf"


@dataclass(frozen=True)
class AppConfig:
    api_cardinfo: str
    api_dbver: str
    api_misc_value: str
    run_interval_sleep_sec: float
    jitter_sec: float
    http_timeout_sec: int
    retry_max_attempts: int
    retry_base_sec: float
    retry_max_sec: float
    max_queue_items_per_run: int
    max_need_fetch_enqueue_per_run: int
    max_api_calls_per_run: int
    image_download_limit_per_run: int
    keyword_timeout_sec: int
    keyword_max_retries: int
    keyword_search_param: str
    log_level: str


def _read_config(path: Path) -> configparser.ConfigParser:
    parser = configparser.ConfigParser()
    parser.read(path, encoding="utf-8")
    return parser


def load_app_config(path: Path = DEFAULT_CONFIG_PATH) -> AppConfig:
    parser = _read_config(path)
    return AppConfig(
        api_cardinfo=parser.get("api", "cardinfo_url", fallback="https://db.ygoprodeck.com/api/v7/cardinfo.php"),
        api_dbver=parser.get("api", "dbver_url", fallback="https://db.ygoprodeck.com/api/v7/checkDBVer.php"),
        api_misc_value=parser.get("api", "misc_value", fallback="yes"),
        run_interval_sleep_sec=parser.getfloat("runtime", "run_interval_sleep_sec", fallback=0.6),
        jitter_sec=parser.getfloat("runtime", "jitter_sec", fallback=0.2),
        http_timeout_sec=parser.getint("network", "http_timeout_sec", fallback=30),
        retry_max_attempts=parser.getint("network", "retry_max_attempts", fallback=5),
        retry_base_sec=parser.getfloat("network", "retry_base_sec", fallback=0.5),
        retry_max_sec=parser.getfloat("network", "retry_max_sec", fallback=8.0),
        max_queue_items_per_run=parser.getint("limits", "max_queue_items_per_run", fallback=100),
        max_need_fetch_enqueue_per_run=parser.getint("limits", "max_need_fetch_enqueue_per_run", fallback=100),
        max_api_calls_per_run=parser.getint("limits", "max_api_calls_per_run", fallback=120),
        image_download_limit_per_run=parser.getint("limits", "image_download_limit_per_run", fallback=30),
        keyword_timeout_sec=parser.getint("keyword_fetch", "default_timeout_sec", fallback=15),
        keyword_max_retries=parser.getint("keyword_fetch", "default_max_retries", fallback=2),
        keyword_search_param=parser.get("keyword_fetch", "default_search_param", fallback="fname"),
        log_level=parser.get("logging", "log_level", fallback="INFO").upper(),
    )


def load_help_text(filename: str, fallback: str = "") -> str:
    help_path = HELP_DIR / filename
    if not help_path.exists():
        return fallback
    return help_path.read_text(encoding="utf-8").strip()
