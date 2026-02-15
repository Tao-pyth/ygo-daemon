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

import argparse
import hashlib
import json
import logging
import random
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests import Response
from requests.exceptions import RequestException


# =========================
# 設定（必要ならここだけ調整）
# =========================
API_CARDINFO = "https://db.ygoprodeck.com/api/v7/cardinfo.php"
API_DBVER = "https://db.ygoprodeck.com/api/v7/checkDBVer.php"

RUN_INTERVAL_SLEEP_SEC = 0.6  # API呼び出し間隔（秒）: 0.6秒=約1.6req/s（保守的）
JITTER_SEC = 0.2              # ランダム揺らぎ（秒）
HTTP_TIMEOUT_SEC = 30
RETRY_MAX_ATTEMPTS = 5
RETRY_BASE_SEC = 0.5
RETRY_MAX_SEC = 8.0

MAX_QUEUE_ITEMS_PER_RUN = 100
MAX_NEED_FETCH_ENQUEUE_PER_RUN = 100
MAX_API_CALLS_PER_RUN = 120   # 100件処理 + dbver確認を想定した上限

# ディレクトリ
ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
STATE_DIR = DATA_DIR / "state"
STAGING_DIR = DATA_DIR / "staging"
LOG_DIR = DATA_DIR / "logs"

DB_PATH = STATE_DIR / "crawl.sqlite3"
LOCK_PATH = STATE_DIR / "run.lock"

LOGGER = logging.getLogger("ygo-daemon")


# =========================
# ユーティリティ
# =========================
def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def ensure_dirs() -> None:
    for p in [DATA_DIR, STATE_DIR, STAGING_DIR, LOG_DIR]:
        p.mkdir(parents=True, exist_ok=True)


def configure_logging() -> None:
    ensure_dirs()
    if LOGGER.handlers:
        return

    LOGGER.setLevel(logging.INFO)
    handler = RotatingFileHandler(
        LOG_DIR / "daemon.log",
        maxBytes=1_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    handler.setLevel(logging.ERROR)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    LOGGER.addHandler(handler)


def acquire_lock() -> bool:
    ensure_dirs()
    if LOCK_PATH.exists():
        return False
    LOCK_PATH.write_text(now_iso(), encoding="utf-8")
    return True


def release_lock() -> None:
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
            try:
                response = self.session.get(url, params=params, timeout=HTTP_TIMEOUT_SEC)
                last_response = response
                response.raise_for_status()
                return response.json()
            except RequestException as err:
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
        params = {"konami_id": str(konami_id), "misc": "yes"}
        raw = self._get_json(API_CARDINFO, params)
        return ApiResult(
            data=list(raw.get("data") or []),
            meta=dict(raw.get("meta") or {}),
            raw=raw,
        )

    def cardinfo_fullsync_page(self, offset: int, num: int) -> ApiResult:
        params = {"misc": "yes", "num": str(num), "offset": str(offset)}
        raw = self._get_json(API_CARDINFO, params)
        return ApiResult(
            data=list(raw.get("data") or []),
            meta=dict(raw.get("meta") or {}),
            raw=raw,
        )


# =========================
# SQLite（状態管理 + ロスレス保存）
# =========================
DDL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS kv_store(
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- 初期実装：KONAMI_IDキュー
CREATE TABLE IF NOT EXISTS request_queue(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  konami_id INTEGER NOT NULL,
  state TEXT NOT NULL DEFAULT 'PENDING',   -- PENDING/DONE/ERROR
  attempts INTEGER NOT NULL DEFAULT 0,
  added_at TEXT NOT NULL,
  last_error TEXT
);

-- 原本（ロスレス保存）
CREATE TABLE IF NOT EXISTS cards_raw(
  card_id INTEGER PRIMARY KEY,
  konami_id INTEGER,
  json TEXT NOT NULL,             -- 生JSON丸ごと（ロスなく）
  content_hash TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  dbver_hash TEXT,
  source TEXT NOT NULL,           -- queue/full_sync
  fetch_status TEXT NOT NULL DEFAULT 'OK'
);

-- 検索用索引（必要最小限）
CREATE TABLE IF NOT EXISTS cards_index(
  card_id INTEGER PRIMARY KEY,
  konami_id INTEGER,
  name TEXT,
  type TEXT,
  race TEXT,
  attribute TEXT,
  level INTEGER,
  atk INTEGER,
  def INTEGER,
  archetype TEXT,
  ban_tcg TEXT,
  ban_ocg TEXT,
  updated_at TEXT NOT NULL
);

-- 取り込みファイル管理（任意だが堅牢）
CREATE TABLE IF NOT EXISTS ingest_files(
  path TEXT PRIMARY KEY,
  status TEXT NOT NULL,           -- PENDING/DONE/FAILED
  added_at TEXT NOT NULL,
  processed_at TEXT,
  last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_queue_state ON request_queue(state, id);
CREATE INDEX IF NOT EXISTS idx_raw_konami ON cards_raw(konami_id);
CREATE INDEX IF NOT EXISTS idx_index_konami ON cards_index(konami_id);
"""


def db_connect() -> sqlite3.Connection:
    ensure_dirs()
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def ensure_schema(con: sqlite3.Connection) -> None:
    con.executescript(DDL)

    columns = {
        row["name"]
        for row in con.execute("PRAGMA table_info(cards_raw)").fetchall()
    }
    if "fetch_status" not in columns:
        con.execute("ALTER TABLE cards_raw ADD COLUMN fetch_status TEXT NOT NULL DEFAULT 'OK'")

    con.commit()


def kv_get(con: sqlite3.Connection, key: str, default: Optional[str] = None) -> Optional[str]:
    row = con.execute("SELECT value FROM kv_store WHERE key=?", (key,)).fetchone()
    return str(row["value"]) if row else default


def kv_set(con: sqlite3.Connection, key: str, value: str) -> None:
    con.execute(
        "INSERT INTO kv_store(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


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
        con.commit()

    return h


# =========================
# ステップB：キュー（KONAMI_ID）優先消化 → JSONL蓄積
# =========================
def queue_add(con: sqlite3.Connection, konami_id: int) -> None:
    con.execute(
        "INSERT INTO request_queue(konami_id, state, attempts, added_at) VALUES(?,?,?,?)",
        (konami_id, "PENDING", 0, now_iso()),
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


def enqueue_need_fetch_cards(con: sqlite3.Connection, limit: int) -> int:
    # NOTE(引き継ぎ): queue優先設計を守るため、PENDING重複投入を避けつつ小分けで再投入する。
    # limitで1runあたりの投入上限をかけ、Task Scheduler想定の漸進同期に寄せている。
    candidates = con.execute(
        """
        SELECT DISTINCT cr.konami_id
        FROM cards_raw cr
        WHERE cr.konami_id IS NOT NULL
          AND cr.fetch_status IN ('NEED_FETCH', 'ERROR')
          AND NOT EXISTS (
              SELECT 1
              FROM request_queue q
              WHERE q.konami_id=cr.konami_id AND q.state='PENDING'
          )
        ORDER BY cr.fetched_at ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    if not candidates:
        return 0

    added_at = now_iso()
    con.executemany(
        "INSERT INTO request_queue(konami_id, state, attempts, added_at) VALUES(?,?,?,?)",
        [(int(row["konami_id"]), "PENDING", 0, added_at) for row in candidates],
    )
    con.commit()
    return len(candidates)


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
    done = 0
    for _ in range(MAX_QUEUE_ITEMS_PER_RUN):
        row = queue_pick_next(con)
        if not row:
            break

        qid = int(row["id"])
        konami_id = int(row["konami_id"])

        try:
            res = api.cardinfo_by_konami_id(konami_id)
            # 取得できない（data空）場合も運用上は「DONE」扱いにするか悩むところ。
            # 初期は「DONE」にして、必要なら別途再投入する運用が安定。
            staging_write_cards(res.data, source="queue")
            queue_mark_done(con, qid)
            done += 1
        except Exception as e:
            LOGGER.error("queue item failed (qid=%s, konami_id=%s): %s", qid, konami_id, e)
            queue_mark_retry(con, qid, str(e))
            mark_need_fetch_by_konami_id(con, konami_id)

    return done


# =========================
# ステップC：全件同期（未知取得アルゴリズム）→ JSONL蓄積
# =========================
# =========================
# ステップC：JSONL → SQLite 一括取り込み
# =========================
def ingest_register_pending(con: sqlite3.Connection, path: Path) -> None:
    con.execute(
        "INSERT INTO ingest_files(path,status,added_at) VALUES(?,?,?) "
        "ON CONFLICT(path) DO NOTHING",
        (str(path), "PENDING", now_iso()),
    )


def ingest_scan_and_register(con: sqlite3.Connection) -> None:
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
        INSERT INTO cards_raw(card_id, konami_id, json, content_hash, fetched_at, dbver_hash, source)
        VALUES(?,?,?,?,?,?,?)
        ON CONFLICT(card_id) DO UPDATE SET
          konami_id=excluded.konami_id,
          json=excluded.json,
          content_hash=excluded.content_hash,
          fetched_at=excluded.fetched_at,
          dbver_hash=excluded.dbver_hash,
          source=excluded.source,
          fetch_status='OK'
        """,
        (card_id, konami_id, raw_json_text, h, now_iso(), dbver_hash, source),
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
    # stagingをスキャンして ingest_filesへ登録
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
        else:
            LOGGER.error("ingest failed path=%s err=%s", path, err)
            ingest_finalize(con, path, status="FAILED", err=err)
        path.unlink(missing_ok=True)

    return total


# =========================
# runner（1回実行）
# =========================
def run_once() -> int:
    configure_logging()
    if not acquire_lock():
        print("[SKIP] 既に実行中の可能性があるため終了します。")
        return 0

    con = None
    try:
        con = db_connect()
        ensure_schema(con)

        api = ApiClient()

        # A) DB更新検知
        dbver_hash = step_check_dbver(con, api)

        if kv_get(con, "dbver_changed", "0") == "1":
            # NOTE(引き継ぎ): dbver差分検知後はfetch_statusをNEED_FETCHに戻すだけ。
            # 実際の再取得はキュー経由で少しずつ進める（処理時間のスパイク回避）。
            con.execute("UPDATE cards_raw SET fetch_status='NEED_FETCH' WHERE konami_id IS NOT NULL")
            kv_set(con, "dbver_changed", "0")
            con.commit()

        queue_requeue_errors(con)

        if not queue_has_pending(con):
            # NOTE(引き継ぎ): 現在の実装は「キューが空ならNEED_FETCHを再投入」まで。
            # 仕様書にあるoffsetベースのfull sync(1ページ進行)は別途接続が必要。
            enqueue_need_fetch_cards(con, MAX_NEED_FETCH_ENQUEUE_PER_RUN)

        # B) キュー優先
        q_done = step_consume_queue(con, api, dbver_hash=dbver_hash)

        # C) SQLite一括取り込み
        ingested = step_ingest_sqlite(con, dbver_hash=dbver_hash)

        kv_set(con, "last_run_at", now_iso())
        con.commit()

        print(f"[OK] run: queue_done={q_done}, ingested_cards={ingested}, api_calls={api.api_calls}")
        return 0

    except Exception as e:
        LOGGER.error("run failed: %s", e)
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


def cmd_queue_add(konami_id: int) -> int:
    con = db_connect()
    try:
        ensure_schema(con)
        queue_add(con, konami_id)
        print(f"[OK] queued konami_id={konami_id}")
        return 0
    finally:
        con.close()


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description="YGOPRODeck API v7 定期取得デーモン（SQLiteロスレス保存）")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("initdb", help="SQLite初期化（テーブル作成）")

    p_add = sub.add_parser("queue-add", help="KONAMI_IDをキューに追加")
    p_add.add_argument("--konami-id", type=int, required=True)

    sub.add_parser("run", help="1回実行（タスクスケジューラで定期起動する想定）")

    args = parser.parse_args(argv)

    if args.cmd == "initdb":
        return cmd_initdb()
    if args.cmd == "queue-add":
        return cmd_queue_add(int(args.konami_id))
    if args.cmd == "run":
        return run_once()

    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
