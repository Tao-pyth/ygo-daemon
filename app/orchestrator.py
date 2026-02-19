from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class RunResult:
    """1回実行の主要成果を呼び出し元へ返すDTO。"""

    queue_done: int
    ingested_cards: int
    images_done: int
    api_calls: int
    fullsync_ran: bool
    fullsync_cards: int
    fullsync_upserted: int
    fullsync_next_offset: int | None


def execute_run_cycle(
    con: sqlite3.Connection,
    *,
    max_queue_items_per_run: int,
    api: object,
    kv_get: Callable[[sqlite3.Connection, str, str | None], str | None],
    kv_set: Callable[[sqlite3.Connection, str, str], None],
    step_check_dbver: Callable[[sqlite3.Connection, object], str],
    queue_requeue_errors: Callable[[sqlite3.Connection], None],
    queue_has_pending: Callable[[sqlite3.Connection], bool],
    step_consume_queue: Callable[[sqlite3.Connection, object, str], int],
    step_fullsync_once: Callable[[sqlite3.Connection, object], tuple[bool, int, int, int | None]],
    step_ingest_sqlite: Callable[[sqlite3.Connection, str], int],
    step_download_images: Callable[[sqlite3.Connection, object], int],
    now_iso: Callable[[], str],
) -> RunResult:
    """デーモン1サイクル分の処理を、依存注入された関数で順次実行する。

    ここは「実行順序の契約」を表す層であり、個別の取得・保存ロジックは
    `main.py` 側の step 関数へ委譲する。
    引き継ぎ時は、本関数の順序が運用ポリシー（queue優先・段階再取得）に
    直結している点を最優先で確認すること。
    """
    # Step 1: DB バージョン差分を確認し、以降ステップが参照するハッシュを確定。
    dbver_hash = step_check_dbver(con, api)

    if kv_get(con, "dbver_changed", "0") == "1":
        # dbver変化を検知した周回では、既存カードへ NEED_FETCH を付けるだけに留める。
        # これにより、1回実行でのAPI負荷を抑えつつ、後続の queue 消化で段階回収できる。
        con.execute("UPDATE cards_raw SET fetch_status='NEED_FETCH' WHERE konami_id IS NOT NULL")
        kv_set(con, "dbver_changed", "0")
        con.commit()

    # Step 2: 前回失敗分を再挑戦可能状態へ戻してから、今回の queue 処理を開始。
    queue_requeue_errors(con)

    queue_done = step_consume_queue(con, api, dbver_hash)
    has_pending_after_queue = queue_has_pending(con)
    fullsync_ran = False
    fullsync_cards = 0
    fullsync_upserted = 0
    fullsync_next_offset = None
    # Step 3: queue を優先しつつ、処理枠に余裕がある場合のみ fullsync を 1 ページ進める。
    if queue_done < max_queue_items_per_run or not has_pending_after_queue:
        fullsync_ran, fullsync_cards, fullsync_upserted, fullsync_next_offset = step_fullsync_once(con, api)

    # Step 4: 取得済み JSONL を DB へ反映。ここで cards_raw のロスレス保存を行う。
    ingested_cards = step_ingest_sqlite(con, dbver_hash)

    kv_set(con, "last_run_at", now_iso())
    con.commit()

    # Step 5: 末尾で画像取得を実施。同期本体より失敗影響を分離しやすくする。
    images_done = step_download_images(con, api)

    return RunResult(
        queue_done=queue_done,
        ingested_cards=ingested_cards,
        images_done=images_done,
        api_calls=int(getattr(api, "api_calls", 0)),
        fullsync_ran=fullsync_ran,
        fullsync_cards=fullsync_cards,
        fullsync_upserted=fullsync_upserted,
        fullsync_next_offset=fullsync_next_offset,
    )
