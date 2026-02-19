#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
YGOPRODeck API v7 定期取得デーモン
- cards_raw: APIカードJSONをロスレス保存
- cards_index: 検索向け索引
- request_queue: KONAMI_IDキューを最優先で最大100件処理
- dbver変更時は cards_raw.fetch_status を NEED_FETCH へ更新し段階的に再取得
- JSONL取り込み成否に関わらず中間ファイルは削除
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests import Response
from requests.exceptions import RequestException

from app.cli import dispatch
from app.config import DB_PATH, load_app_config
from app.usecase.dict_build import DictBuilderConfig, run_incremental_build
from app.infra.migrate import apply_migrations
from app.infra.table_dump import TableDumpError, dump_tables, parse_tables_arg, validate_tables
from app.orchestrator import execute_run_cycle


# =========================
# 設定（必要ならここだけ調整）
# =========================
APP_CONFIG = load_app_config()

API_CARDINFO = APP_CONFIG.api_cardinfo
API_DBVER = APP_CONFIG.api_dbver
API_MISC_VALUE = APP_CONFIG.api_misc_value

RUN_INTERVAL_SLEEP_SEC = APP_CONFIG.run_interval_sleep_sec  # API呼び出し間隔（秒）: 0.6秒=約1.6req/s（保守的）
JITTER_SEC = APP_CONFIG.jitter_sec                          # ランダム揺らぎ（秒）
HTTP_TIMEOUT_SEC = APP_CONFIG.http_timeout_sec
RETRY_MAX_ATTEMPTS = APP_CONFIG.retry_max_attempts
RETRY_BASE_SEC = APP_CONFIG.retry_base_sec
RETRY_MAX_SEC = APP_CONFIG.retry_max_sec

MAX_QUEUE_ITEMS_PER_RUN = APP_CONFIG.max_queue_items_per_run
MAX_API_CALLS_PER_RUN = APP_CONFIG.max_api_calls_per_run   # 100件処理 + dbver確認を想定した上限
FULLSYNC_NUM = APP_CONFIG.fullsync_num

# ディレクトリ
ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
LOCK_DIR = DATA_DIR / "lock"
DICT_LOCK_DIR = DATA_DIR / "locks"
STAGING_DIR = DATA_DIR / "staging"
LOG_DIR = DATA_DIR / "logs"
IMAGE_DIR = DATA_DIR / "image" / "card"
TEMP_IMAGE_DIR = DATA_DIR / "image" / "temp"
FAILED_INGEST_DIR = DATA_DIR / "failed"
DB_DIR = DATA_DIR / "db"

LOCK_PATH = LOCK_DIR / "daemon.lock"
DICT_LOCK_PATH = DICT_LOCK_DIR / "dict_builder.lock"
DICT_LOG_PATH = LOG_DIR / "dict_builder.log"
LOG_LEVEL = os.getenv("YGO_LOG_LEVEL", APP_CONFIG.log_level).upper()
DICT_LOG_LEVEL = os.getenv("YGO_DICT_LOG_LEVEL", APP_CONFIG.log_level).upper()
IMAGE_DOWNLOAD_LIMIT_PER_RUN = APP_CONFIG.image_download_limit_per_run
DICT_BUILDER_MAX_RUNTIME_SEC = APP_CONFIG.dict_builder_max_runtime_sec
DICT_BUILDER_BATCH_SIZE = APP_CONFIG.dict_builder_batch_size
DICT_RULESET_VERSION = APP_CONFIG.dict_ruleset_version
DICT_ACCEPT_THRESHOLDS = {
    "cost_patterns": APP_CONFIG.dict_accept_threshold_cost,
    "action_patterns": APP_CONFIG.dict_accept_threshold_action,
    "trigger_patterns": APP_CONFIG.dict_accept_threshold_trigger,
    "restriction_patterns": APP_CONFIG.dict_accept_threshold_restriction,
    "condition_patterns": APP_CONFIG.dict_accept_threshold_condition,
    "unclassified_patterns": APP_CONFIG.dict_accept_threshold_unclassified,
}

LOGGER = logging.getLogger("ygo-daemon")


# =========================
# ユーティリティ
# =========================
def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def ensure_dirs() -> None:
    for p in [DATA_DIR, DB_DIR, LOCK_DIR, DICT_LOCK_DIR, STAGING_DIR, LOG_DIR, IMAGE_DIR, TEMP_IMAGE_DIR, FAILED_INGEST_DIR]:
        p.mkdir(parents=True, exist_ok=True)


def configure_logging() -> None:
    ensure_dirs()
    if LOGGER.handlers:
        return

    LOGGER.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    handler = RotatingFileHandler(
        LOG_DIR / "daemon.log",
        maxBytes=1_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    handler.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    LOGGER.addHandler(handler)


def acquire_lock() -> bool:
    """単純ロックファイルを作成し、多重起動を防ぐ。"""
    ensure_dirs()
    try:
        with LOCK_PATH.open("x", encoding="utf-8") as lock_file:
            lock_file.write(now_iso())
        return True
    except FileExistsError:
        return False


def release_lock() -> None:
    """ロックファイルを削除する。異常系でも終了処理を優先して例外は握りつぶす。"""
    try:
        if LOCK_PATH.exists():
            LOCK_PATH.unlink()
    except Exception:
        pass


def stable_json_dumps(obj: Any) -> str:
    # 原本は「ロスなく保存」だが、ハッシュ用途では安定化（ソート）して比較しやすくする
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def sleep_rate() -> None:
    base = RUN_INTERVAL_SLEEP_SEC
    jitter = random.uniform(-JITTER_SEC, JITTER_SEC)
    sec = max(0.0, base + jitter)
    time.sleep(sec)


@dataclass
class ApiResult:
    data: List[Dict[str, Any]]
    meta: Dict[str, Any]
    raw: Dict[str, Any]


def parse_cards_from_response(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = raw.get("data")
    if not isinstance(data, list):
        return []
    return [card for card in data if isinstance(card, dict)]


class ApiClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.api_calls = 0

    def _calc_backoff(self, attempt: int) -> float:
        capped_attempt = max(1, attempt)
        return min(RETRY_MAX_SEC, RETRY_BASE_SEC * (2 ** (capped_attempt - 1)))

    def _should_retry(self, response: Optional[Response], error: Optional[Exception]) -> bool:
        if error is not None:
            return True
        if response is None:
            return False
        return response.status_code in (429, 500, 502, 503, 504)

    def _get_json(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        if self.api_calls >= MAX_API_CALLS_PER_RUN:
            raise RuntimeError("API呼び出し上限に到達しました（暴走防止）")

        last_error: Optional[Exception] = None
        last_response: Optional[Response] = None

        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            sleep_rate()
            self.api_calls += 1
            started_at = time.monotonic()
            try:
                response = self.session.get(url, params=params, timeout=HTTP_TIMEOUT_SEC)
                last_response = response
                response.raise_for_status()
                elapsed_ms = int((time.monotonic() - started_at) * 1000)
                LOGGER.info("api_call url=%s status=%s elapsed_ms=%s count=%s", url, response.status_code, elapsed_ms, self.api_calls)
                return response.json()
            except RequestException as err:
                elapsed_ms = int((time.monotonic() - started_at) * 1000)
                status_code = getattr(last_response, "status_code", "N/A")
                LOGGER.warning(
                    "api_retry url=%s status=%s elapsed_ms=%s attempt=%s/%s error=%s",
                    url,
                    status_code,
                    elapsed_ms,
                    attempt,
                    RETRY_MAX_ATTEMPTS,
                    err,
                )
                last_error = err
                if not self._should_retry(last_response, err):
                    raise

            if attempt == RETRY_MAX_ATTEMPTS:
                break

            backoff_sec = self._calc_backoff(attempt)
            time.sleep(backoff_sec)

            if self.api_calls >= MAX_API_CALLS_PER_RUN:
                raise RuntimeError("API呼び出し上限に到達しました（暴走防止）")

        if last_error is not None:
            raise RuntimeError(f"API呼び出し失敗（retries exhausted）: {last_error}")

        raise RuntimeError("API呼び出しに失敗しました（レスポンス不正）")

    def check_dbver(self) -> Dict[str, Any]:
        return self._get_json(API_DBVER, {})

    def cardinfo_by_konami_id(self, konami_id: int) -> ApiResult:
        params = {"konami_id": str(konami_id), "misc": API_MISC_VALUE}
        raw = self._get_json(API_CARDINFO, params)
        return ApiResult(
            data=parse_cards_from_response(raw),
            meta=dict(raw.get("meta") or {}),
            raw=raw,
        )

    def cardinfo_by_keyword(self, keyword: str) -> ApiResult:
        params = {"fname": keyword, "misc": API_MISC_VALUE}
        raw = self._get_json(API_CARDINFO, params)
        return ApiResult(
            data=parse_cards_from_response(raw),
            meta=dict(raw.get("meta") or {}),
            raw=raw,
        )

    def cardinfo_fullsync_page(self, offset: int, num: int) -> ApiResult:
        params = {"misc": API_MISC_VALUE, "num": str(num), "offset": str(offset)}
        raw = self._get_json(API_CARDINFO, params)
        return ApiResult(
            data=parse_cards_from_response(raw),
            meta=dict(raw.get("meta") or {}),
            raw=raw,
        )


# =========================
# SQLite（状態管理 + ロスレス保存）
# =========================
MIGRATIONS_DIR = ROOT / "app" / "db" / "migrations"


def db_connect() -> sqlite3.Connection:
    ensure_dirs()
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")
    return con


def ensure_schema(con: sqlite3.Connection) -> None:
    apply_migrations(con, MIGRATIONS_DIR)


def kv_get(con: sqlite3.Connection, key: str, default: Optional[str] = None) -> Optional[str]:
    row = con.execute("SELECT value FROM kv_store WHERE key=?", (key,)).fetchone()
    return str(row["value"]) if row else default


def kv_set(con: sqlite3.Connection, key: str, value: str) -> None:
    con.execute(
        "INSERT INTO kv_store(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def kv_get_int(con: sqlite3.Connection, key: str, default: int) -> int:
    value = kv_get(con, key)
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def kv_get_bool(con: sqlite3.Connection, key: str, default: bool) -> bool:
    value = kv_get(con, key)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def kv_set_int(con: sqlite3.Connection, key: str, value: int) -> None:
    kv_set(con, key, str(value))


def kv_set_bool(con: sqlite3.Connection, key: str, value: bool) -> None:
    kv_set(con, key, "1" if value else "0")


# =========================
# JSON抽出（索引用）
# =========================
def try_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        # "?" などが混ざる可能性を考慮
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if s == "":
            return None
        return int(float(s))
    except Exception:
        return None


def extract_konami_id(card: Dict[str, Any]) -> Optional[int]:
    # APIによって格納場所が変わる可能性があるため、安全に拾う
    # 1) トップレベルにあればそれを使う
    if "konami_id" in card:
        ki = try_int(card.get("konami_id"))
        if ki is not None:
            return ki

    # 2) misc_info 配列の中
    misc = card.get("misc_info")
    if isinstance(misc, list) and misc:
        ki = try_int(misc[0].get("konami_id"))
        if ki is not None:
            return ki

    return None


def extract_banlist(card: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    # banlist_info: {"ban_tcg":"Limited","ban_ocg":"Unlimited", ...} のような形式が多い
    info = card.get("banlist_info")
    if not isinstance(info, dict):
        return None, None
    ban_tcg = info.get("ban_tcg")
    ban_ocg = info.get("ban_ocg")
    return (str(ban_tcg) if ban_tcg is not None else None, str(ban_ocg) if ban_ocg is not None else None)


def extract_level(card: Dict[str, Any]) -> Optional[int]:
    # 通常は level。リンクは linkval など別。
    lvl = try_int(card.get("level"))
    if lvl is not None:
        return lvl
    # linkvalをlevel欄に入れたい場合の暫定（好みで）
    link = try_int(card.get("linkval"))
    return link


def extract_index_fields(card: Dict[str, Any]) -> Dict[str, Any]:
    konami_id = extract_konami_id(card)
    ban_tcg, ban_ocg = extract_banlist(card)
    return {
        "card_id": try_int(card.get("id")),
        "konami_id": konami_id,
        "name": card.get("name"),
        "type": card.get("type"),
        "race": card.get("race"),
        "attribute": card.get("attribute"),
        "level": extract_level(card),
        "atk": try_int(card.get("atk")),
        "def": try_int(card.get("def")),
        "archetype": card.get("archetype"),
        "ban_tcg": ban_tcg,
        "ban_ocg": ban_ocg,
    }


# =========================
# ステップA：checkDBVer
# =========================
def step_check_dbver(con: sqlite3.Connection, api: ApiClient) -> str:
    j = api.check_dbver()
    raw = stable_json_dumps(j)
    h = sha256_text(raw)

    old = kv_get(con, "dbver_hash")
    if old != h:
        # NOTE(引き継ぎ): dbver変化時は「既存カードを段階再取得」するためのトリガだけ立てる。
        # ここで即時に全件API再取得しないのは、1回実行での処理量を制御するため。
        kv_set(con, "dbver_hash", h)
        kv_set(con, "dbver_changed", "1")
        kv_set_int(con, "fullsync_offset", 0)
        kv_set_bool(con, "fullsync_done", False)
        kv_set(con, "fullsync_last_dbver", h)
        con.commit()

    return h


# =========================
# ステップB：キュー（KONAMI_ID）優先消化 → JSONL蓄積
# =========================
def queue_add(con: sqlite3.Connection, *, konami_id: Optional[int], keyword: Optional[str]) -> None:
    if (konami_id is None) == (keyword is None):
        raise ValueError("Either konami_id or keyword must be set, but not both")

    con.execute(
        "INSERT INTO request_queue(konami_id, keyword, state, attempts, added_at) VALUES(?,?,?,?,?)",
        (konami_id, keyword, "PENDING", 0, now_iso()),
    )
    con.commit()


def queue_pick_next(con: sqlite3.Connection) -> Optional[sqlite3.Row]:
    return con.execute(
        "SELECT * FROM request_queue WHERE state='PENDING' ORDER BY id ASC LIMIT 1"
    ).fetchone()


def queue_has_pending(con: sqlite3.Connection) -> bool:
    row = con.execute("SELECT 1 FROM request_queue WHERE state='PENDING' LIMIT 1").fetchone()
    return row is not None


def queue_requeue_errors(con: sqlite3.Connection) -> None:
    # NOTE(引き継ぎ): ERRORを次回以降に再挑戦させる。
    # ここで握りつぶさず再投入しておくことで、APIの一時障害に強くする。
    con.execute("UPDATE request_queue SET state='PENDING' WHERE state='ERROR'")
    con.commit()


def queue_mark_done(con: sqlite3.Connection, qid: int) -> None:
    con.execute("UPDATE request_queue SET state='DONE' WHERE id=?", (qid,))
    con.commit()


def queue_mark_retry(con: sqlite3.Connection, qid: int, err: str) -> None:
    con.execute(
        "UPDATE request_queue SET state='ERROR', attempts=attempts+1, last_error=? WHERE id=?",
        (err[:2000], qid),
    )
    con.commit()


def mark_need_fetch_by_konami_id(con: sqlite3.Connection, konami_id: int) -> None:
    con.execute(
        "UPDATE cards_raw SET fetch_status='NEED_FETCH' WHERE konami_id=?",
        (konami_id,),
    )
    con.commit()


def staging_write_cards(cards: List[Dict[str, Any]], source: str) -> Optional[Path]:
    if not cards:
        return None
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = STAGING_DIR / f"cards_{source}_{ts}.jsonl"
    with path.open("w", encoding="utf-8") as f:
        for c in cards:
            # ロスレス保存の原則：カードオブジェクトは加工せず丸ごと1行に
            f.write(json.dumps(c, ensure_ascii=False) + "\n")
    return path


def step_consume_queue(con: sqlite3.Connection, api: ApiClient, dbver_hash: str) -> int:
    """PENDING キューを上限件数まで処理し、取得結果を staging JSONL へ保存する。"""
    done = 0
    for _ in range(MAX_QUEUE_ITEMS_PER_RUN):
        row = queue_pick_next(con)
        if not row:
            break

        qid = int(row["id"])
        konami_id = try_int(row["konami_id"])
        keyword = row["keyword"]

        try:
            # 1キュー項目につき API 呼び出しは1回のみ。成功時だけ DONE へ進める。
            if konami_id is not None:
                res = api.cardinfo_by_konami_id(konami_id)
                source = "queue"
            elif isinstance(keyword, str) and keyword.strip():
                res = api.cardinfo_by_keyword(keyword.strip())
                source = "keyword"
            else:
                raise RuntimeError("queue item missing konami_id and keyword")

            # 取得できない（data空）場合も運用上は「DONE」扱いにするか悩むところ。
            # 初期は「DONE」にして、必要なら別途再投入する運用が安定。
            LOGGER.info("parse_result qid=%s source=%s cards=%s", qid, source, len(res.data))
            # data=[] の場合でも「問い合わせ済み」として DONE 扱いにする。
            # 再調査が必要なIDは運用側で再投入する運用を前提とする。
            staging_write_cards(res.data, source=source)
            queue_mark_done(con, qid)
            done += 1
        except Exception as e:
            LOGGER.error("queue item failed (qid=%s, konami_id=%s, keyword=%s): %s", qid, konami_id, keyword, e)
            # ERROR に落として次周回で再投入する。run 全体は止めない。
            queue_mark_retry(con, qid, str(e))
            if konami_id is not None:
                mark_need_fetch_by_konami_id(con, konami_id)

    return done


# =========================
# ステップC：全件同期（offset/num）→ JSONL蓄積
# =========================
def get_fullsync_state(con: sqlite3.Connection) -> tuple[int, int, bool]:
    """fullsync 進捗を kv_store から取得し、安全な初期値へ正規化する。"""
    offset = max(0, kv_get_int(con, "fullsync_offset", 0))
    num = kv_get_int(con, "fullsync_num", FULLSYNC_NUM)
    if num <= 0:
        num = FULLSYNC_NUM
    done = kv_get_bool(con, "fullsync_done", False)
    return offset, num, done


def set_fullsync_state(con: sqlite3.Connection, *, offset: int | None = None, num: int | None = None, done: bool | None = None) -> None:
    """fullsync 進捗を必要項目だけ更新する。"""
    if offset is not None:
        kv_set_int(con, "fullsync_offset", max(0, offset))
    if num is not None and num > 0:
        kv_set_int(con, "fullsync_num", num)
    if done is not None:
        kv_set_bool(con, "fullsync_done", done)


def is_valid_next_offset(next_offset: Any, current_offset: int) -> bool:
    parsed = try_int(next_offset)
    return parsed is not None and parsed >= 0 and parsed > current_offset


def step_fullsync_once(con: sqlite3.Connection, api: ApiClient) -> tuple[bool, int, int, int | None]:
    """queue 消化後の余力で fullsync を1ページだけ進める。"""
    current_offset, num, done = get_fullsync_state(con)
    if done:
        LOGGER.info("fullsync_skip reason=done")
        return False, 0, 0, None

    result = api.cardinfo_fullsync_page(current_offset, num)
    card_count = len(result.data)
    staging_path = staging_write_cards(result.data, source="fullsync")

    next_offset_raw = result.meta.get("next_page_offset")
    next_offset = try_int(next_offset_raw)
    if is_valid_next_offset(next_offset_raw, current_offset):
        # API が次ページを示した場合のみ offset を前進させる。
        set_fullsync_state(con, offset=int(next_offset), done=False, num=num)
    else:
        # next_page_offset が欠落/不正/後退値なら完了扱いで停止。
        next_offset = None
        set_fullsync_state(con, done=True, num=num)

    con.commit()
    LOGGER.info(
        "fullsync_page offset=%s num=%s cards=%s upserted=%s next_page_offset=%s staging=%s",
        current_offset,
        num,
        card_count,
        card_count,
        next_offset_raw,
        staging_path,
    )
    return True, card_count, card_count, next_offset


# =========================
# ステップD：JSONL → SQLite 一括取り込み
# =========================
def ingest_register_pending(con: sqlite3.Connection, path: Path) -> None:
    con.execute(
        "INSERT INTO ingest_files(path,status,added_at) VALUES(?,?,?) "
        "ON CONFLICT(path) DO NOTHING",
        (str(path), "PENDING", now_iso()),
    )


def ingest_scan_and_register(con: sqlite3.Connection) -> None:
    # staging 上に存在する jsonl を ingest 管理テーブルへ取り込み登録する。
    # 既存 path は ON CONFLICT DO NOTHING により重複登録しない。
    for p in sorted(STAGING_DIR.glob("*.jsonl")):
        ingest_register_pending(con, p)
    con.commit()


def ingest_get_pending_files(con: sqlite3.Connection) -> List[sqlite3.Row]:
    return list(con.execute("SELECT * FROM ingest_files WHERE status='PENDING' ORDER BY added_at ASC").fetchall())


def upsert_card_rows(con: sqlite3.Connection, card: Dict[str, Any], dbver_hash: str, source: str) -> None:
    card_id = try_int(card.get("id"))
    if card_id is None:
        # 取り込み不能（データ破損）
        return

    konami_id = extract_konami_id(card)

    # 原本JSON（ロスレス）: ここで加工はしない。json.dumps(ensure_ascii=False)で保存
    raw_json_text = json.dumps(card, ensure_ascii=False)
    # 差分検知: 安定化したJSONをハッシュに使う
    canonical = stable_json_dumps(card)
    h = sha256_text(canonical)

    # cards_raw: 原本保存
    con.execute(
        """
        INSERT INTO cards_raw(card_id, konami_id, json, content_hash, fetched_at, dbver_hash, source, fetch_status)
        VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(card_id) DO UPDATE SET
          konami_id=excluded.konami_id,
          json=excluded.json,
          content_hash=excluded.content_hash,
          fetched_at=excluded.fetched_at,
          dbver_hash=excluded.dbver_hash,
          source=excluded.source,
          fetch_status='OK'
        """,
        (card_id, konami_id, raw_json_text, h, now_iso(), dbver_hash, source, "OK"),
    )

    # cards_index: 検索用抽出
    idx = extract_index_fields(card)
    con.execute(
        """
        INSERT INTO cards_index(
          card_id, konami_id, name, type, race, attribute, level, atk, def, archetype, ban_tcg, ban_ocg, updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(card_id) DO UPDATE SET
          konami_id=excluded.konami_id,
          name=excluded.name,
          type=excluded.type,
          race=excluded.race,
          attribute=excluded.attribute,
          level=excluded.level,
          atk=excluded.atk,
          def=excluded.def,
          archetype=excluded.archetype,
          ban_tcg=excluded.ban_tcg,
          ban_ocg=excluded.ban_ocg,
          updated_at=excluded.updated_at
        """,
        (
            idx["card_id"],
            idx["konami_id"],
            idx["name"],
            idx["type"],
            idx["race"],
            idx["attribute"],
            idx["level"],
            idx["atk"],
            idx["def"],
            idx["archetype"],
            idx["ban_tcg"],
            idx["ban_ocg"],
            now_iso(),
        ),
    )

    image_url: Optional[str] = None
    image_url_cropped: Optional[str] = None
    card_images = card.get("card_images")
    if isinstance(card_images, list) and card_images and isinstance(card_images[0], dict):
        v = card_images[0].get("image_url")
        if isinstance(v, str) and v:
            image_url = v
        v_cropped = card_images[0].get("image_url_cropped")
        if isinstance(v_cropped, str) and v_cropped:
            image_url_cropped = v_cropped

    con.execute(
        """
        INSERT INTO card_images(
          card_id,
          image_url,
          image_url_cropped,
          image_path,
          image_path_cropped,
          fetch_status,
          last_error,
          updated_at
        )
        VALUES(?, ?, ?, NULL, NULL, CASE WHEN ? IS NULL OR ? IS NULL THEN 'ERROR' ELSE 'NEED_FETCH' END, NULL, ?)
        ON CONFLICT(card_id) DO UPDATE SET
          image_url=excluded.image_url,
          image_url_cropped=excluded.image_url_cropped,
          updated_at=excluded.updated_at,
          fetch_status=CASE
            WHEN excluded.image_url IS NULL OR excluded.image_url_cropped IS NULL THEN 'ERROR'
            WHEN card_images.image_path IS NULL OR card_images.image_path='' THEN 'NEED_FETCH'
            WHEN card_images.image_path_cropped IS NULL OR card_images.image_path_cropped='' THEN 'NEED_FETCH'
            ELSE card_images.fetch_status
          END
        """,
        (card_id, image_url, image_url_cropped, image_url, image_url_cropped, now_iso()),
    )


def step_download_images(con: sqlite3.Connection, api: ApiClient, limit: int = IMAGE_DOWNLOAD_LIMIT_PER_RUN) -> int:
    rows = con.execute(
        """
        SELECT card_id, image_url, image_url_cropped, image_path, image_path_cropped
        FROM card_images
        WHERE fetch_status IN ('NEED_FETCH', 'ERROR')
          AND image_url IS NOT NULL
          AND image_url <> ''
          AND image_url_cropped IS NOT NULL
          AND image_url_cropped <> ''
        ORDER BY card_id ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    done = 0
    TEMP_IMAGE_DIR.mkdir(parents=True, exist_ok=True)

    for row in rows:
        card_id = int(row["card_id"])
        image_url = str(row["image_url"])
        image_url_cropped = str(row["image_url_cropped"])
        final_path = IMAGE_DIR / f"{card_id}.jpg"
        final_path_cropped = IMAGE_DIR / f"{card_id}_cropped.jpg"
        temp_path = TEMP_IMAGE_DIR / f"{card_id}.tmp"
        temp_path_cropped = TEMP_IMAGE_DIR / f"{card_id}_cropped.tmp"

        normal_ready = final_path.exists() and final_path.stat().st_size > 0
        cropped_ready = final_path_cropped.exists() and final_path_cropped.stat().st_size > 0

        if normal_ready and cropped_ready:
            con.execute(
                "UPDATE card_images SET image_path=?, image_path_cropped=?, fetch_status='OK', last_error=NULL, updated_at=? WHERE card_id=?",
                (str(final_path), str(final_path_cropped), now_iso(), card_id),
            )
            con.commit()
            LOGGER.info("image_skip card_id=%s path=%s cropped_path=%s", card_id, final_path, final_path_cropped)
            continue
        try:
            LOGGER.info("image_download_start card_id=%s url=%s cropped_url=%s", card_id, image_url, image_url_cropped)
            if not normal_ready:
                response = api.session.get(image_url, timeout=HTTP_TIMEOUT_SEC)
                response.raise_for_status()
                temp_path.write_bytes(response.content)
                temp_path.replace(final_path)

            if not cropped_ready:
                response_cropped = api.session.get(image_url_cropped, timeout=HTTP_TIMEOUT_SEC)
                response_cropped.raise_for_status()
                temp_path_cropped.write_bytes(response_cropped.content)
                temp_path_cropped.replace(final_path_cropped)

            con.execute(
                "UPDATE card_images SET image_path=?, image_path_cropped=?, fetch_status='OK', last_error=NULL, updated_at=? WHERE card_id=?",
                (str(final_path), str(final_path_cropped), now_iso(), card_id),
            )
            con.commit()
            done += 1
            LOGGER.info("image_download_ok card_id=%s path=%s cropped_path=%s", card_id, final_path, final_path_cropped)
        except Exception as e:
            temp_path.unlink(missing_ok=True)
            temp_path_cropped.unlink(missing_ok=True)
            con.execute(
                "UPDATE card_images SET fetch_status='ERROR', last_error=?, updated_at=? WHERE card_id=?",
                (str(e)[:255], now_iso(), card_id),
            )
            con.commit()
            LOGGER.error("image_download_failed card_id=%s error=%s", card_id, e)
    return done


def ingest_one_file(con: sqlite3.Connection, path: Path, dbver_hash: str) -> Tuple[int, Optional[str]]:
    """
    戻り値: (取り込んだカード数, エラー文字列 or None)
    """
    count = 0
    try:
        # ファイル名から source 推定（運用しやすさのため）
        source = "unknown"
        name = path.name.lower()
        if "queue" in name:
            source = "queue"
        elif "fullsync" in name:
            source = "full_sync"

        with path.open("r", encoding="utf-8") as f:
            # 1ファイル単位でトランザクションを閉じることで、
            # 途中失敗時は当該ファイルのみロールバックできる。
            con.execute("BEGIN")
            for line in f:
                line = line.strip()
                if not line:
                    continue
                card = json.loads(line)
                upsert_card_rows(con, card, dbver_hash=dbver_hash, source=source)
                count += 1
            con.commit()
        return count, None
    except Exception as e:
        try:
            con.rollback()
        except Exception:
            pass
        return count, str(e)


def ingest_finalize(con: sqlite3.Connection, path: Path, status: str, err: Optional[str]) -> None:
    """ingest 管理テーブルの状態を更新する。"""
    con.execute(
        """
        UPDATE ingest_files
        SET status=?, processed_at=?, last_error=?
        WHERE path=?
        """,
        (status, now_iso(), (err[:2000] if err else None), str(path)),
    )
    con.commit()


def step_ingest_sqlite(con: sqlite3.Connection, dbver_hash: str) -> int:
    # staging をスキャンして ingest_files へ登録。
    # 成功時はファイル削除、失敗時は failed/ へ退避して痕跡を残す。
    # 「取得済みデータを失わない」ため、失敗時もその場で破棄しないことが重要。
    ingest_scan_and_register(con)
    pendings = ingest_get_pending_files(con)
    total = 0

    for row in pendings:
        path = Path(row["path"])
        if not path.exists():
            # ファイルが無いならDONE扱い（運用ポリシー）
            ingest_finalize(con, path, status="DONE", err=None)
            continue

        cnt, err = ingest_one_file(con, path, dbver_hash=dbver_hash)
        total += cnt

        if err is None:
            ingest_finalize(con, path, status="DONE", err=None)
            path.unlink(missing_ok=True)
        else:
            LOGGER.error("ingest failed path=%s err=%s", path, err)
            ingest_finalize(con, path, status="FAILED", err=err)
            FAILED_INGEST_DIR.mkdir(parents=True, exist_ok=True)
            failed_path = FAILED_INGEST_DIR / path.name
            path.replace(failed_path)

    LOGGER.info("db_upsert_summary upserted_cards=%s pending_files=%s", total, len(pendings))
    return total


# =========================
# runner（1回実行）
# =========================
def run_once() -> int:
    """デーモン1サイクル実行。失敗時もロック解放だけは必ず行う。"""
    configure_logging()
    started = time.monotonic()
    LOGGER.info(
        "run_start command=run db_path=%s max_queue_items=%s image_limit=%s",
        DB_PATH,
        MAX_QUEUE_ITEMS_PER_RUN,
        IMAGE_DOWNLOAD_LIMIT_PER_RUN,
    )
    if not acquire_lock():
        LOGGER.info("run_skip reason=lock_exists lock_path=%s", LOCK_PATH)
        print("[SKIP] 既に実行中の可能性があるため終了します。")
        return 0

    con = None
    try:
        con = db_connect()
        ensure_schema(con)

        api = ApiClient()

        result = execute_run_cycle(
            con,
            max_queue_items_per_run=MAX_QUEUE_ITEMS_PER_RUN,
            api=api,
            kv_get=kv_get,
            kv_set=kv_set,
            step_check_dbver=step_check_dbver,
            queue_requeue_errors=queue_requeue_errors,
            queue_has_pending=queue_has_pending,
            step_consume_queue=step_consume_queue,
            step_fullsync_once=step_fullsync_once,
            step_ingest_sqlite=step_ingest_sqlite,
            step_download_images=step_download_images,
            now_iso=now_iso,
        )

        elapsed = time.monotonic() - started
        LOGGER.info(
            "run_finish elapsed_sec=%.3f queue_done=%s fullsync_ran=%s fullsync_cards=%s fullsync_upserted=%s fullsync_next_offset=%s ingested_cards=%s images_done=%s api_calls=%s",
            elapsed,
            result.queue_done,
            result.fullsync_ran,
            result.fullsync_cards,
            result.fullsync_upserted,
            result.fullsync_next_offset,
            result.ingested_cards,
            result.images_done,
            result.api_calls,
        )
        print(
            "[OK] run: "
            f"queue_done={result.queue_done}, "
            f"fullsync_ran={result.fullsync_ran}, "
            f"fullsync_cards={result.fullsync_cards}, "
            f"fullsync_upserted={result.fullsync_upserted}, "
            f"fullsync_next_offset={result.fullsync_next_offset}, "
            f"ingested_cards={result.ingested_cards}, "
            f"images_done={result.images_done}, "
            f"api_calls={result.api_calls}"
        )
        return 0

    except Exception as e:
        LOGGER.error("run failed: %s", e, exc_info=LOG_LEVEL == "DEBUG")
        print(f"[ERROR] {e}")
        return 1

    finally:
        try:
            if con is not None:
                con.close()
        finally:
            release_lock()


# =========================
# CLI
# =========================
def cmd_initdb() -> int:
    ensure_dirs()
    con = db_connect()
    try:
        ensure_schema(con)
        print(f"[OK] DB initialized: {DB_PATH}")
        return 0
    finally:
        con.close()


def cmd_queue_add(konami_id: Optional[int], keyword: Optional[str]) -> int:
    con = db_connect()
    try:
        ensure_schema(con)
        queue_add(con, konami_id=konami_id, keyword=keyword)
        if konami_id is not None:
            print(f"[OK] queued konami_id={konami_id}")
        else:
            print(f"[OK] queued keyword={keyword}")
        return 0
    finally:
        con.close()


def cmd_dict_build(max_runtime_sec: Optional[int], batch_size: Optional[int], dry_run: bool, log_level: Optional[str]) -> int:
    """辞書増分構築コマンド。実処理は run_incremental_build に委譲する。"""
    configure_logging()
    con = db_connect()
    try:
        ensure_schema(con)
        stats = run_incremental_build(
            con,
            DictBuilderConfig(
                lock_path=DICT_LOCK_PATH,
                log_path=DICT_LOG_PATH,
                log_level=(log_level or DICT_LOG_LEVEL).upper(),
                max_runtime_sec=max_runtime_sec if max_runtime_sec is not None else DICT_BUILDER_MAX_RUNTIME_SEC,
                batch_size=batch_size if batch_size is not None else DICT_BUILDER_BATCH_SIZE,
                ruleset_version=DICT_RULESET_VERSION,
                dry_run=dry_run,
                accept_thresholds=DICT_ACCEPT_THRESHOLDS,
            ),
        )
        LOGGER.info(
            "dict_build_summary processed_cards=%s new_phrases=%s updated_phrases=%s promoted_phrases=%s rejected_phrases=%s stop_reason=%s",
            stats.processed_cards,
            stats.new_phrases,
            stats.updated_phrases,
            stats.promoted_phrases,
            stats.rejected_phrases,
            stats.stop_reason,
        )
        if stats.stop_reason == "exception":
            return 1
        print(
            "[OK] dict-build: "
            f"processed_cards={stats.processed_cards}, "
            f"new_phrases={stats.new_phrases}, "
            f"updated_phrases={stats.updated_phrases}, "
            f"promoted_phrases={stats.promoted_phrases}, "
            f"rejected_phrases={stats.rejected_phrases}, "
            f"stop_reason={stats.stop_reason}"
        )
        return 0
    except Exception as e:
        LOGGER.error("dict-build failed: %s", e, exc_info=LOG_LEVEL == "DEBUG")
        print(f"[ERROR] {e}")
        return 1
    finally:
        con.close()


def _cmd_dump(tables_text: Optional[str], out: str, fmt: str, *, use_default_tables: bool) -> int:
    con = db_connect()
    try:
        ensure_schema(con)
        default_tables = None if not use_default_tables else (
            "dsl_dictionary_patterns",
            "dsl_dictionary_terms",
            "kv_store",
        )
        tables = parse_tables_arg(tables_text, default_tables=default_tables or ())
        tables = validate_tables(con, tables)
        exported = dump_tables(con, tables=tables, out_path=Path(out), fmt=fmt)
        print(f"[OK] dump: tables={','.join(tables)} rows={exported} format={fmt} out={out}")
        return 0
    except TableDumpError as e:
        print(f"[ERROR] {e}")
        return 2
    finally:
        con.close()


def cmd_dict_dump(tables_text: Optional[str], out: str, fmt: str) -> int:
    return _cmd_dump(tables_text, out, fmt, use_default_tables=True)


def cmd_db_dump(tables_text: Optional[str], out: str, fmt: str) -> int:
    return _cmd_dump(tables_text, out, fmt, use_default_tables=False)


def main(argv: List[str]) -> int:
    return dispatch(
        argv,
        cmd_initdb=cmd_initdb,
        cmd_queue_add=cmd_queue_add,
        cmd_run_once=run_once,
        cmd_dict_build=cmd_dict_build,
        cmd_dict_dump=cmd_dict_dump,
        cmd_db_dump=cmd_db_dump,
    )


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
