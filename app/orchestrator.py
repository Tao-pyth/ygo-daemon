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
    random_enqueued: int
    blacklisted: int


def execute_run_cycle(
    con: sqlite3.Connection,
    *,
    api: object,
    kv_get: Callable[[sqlite3.Connection, str, str | None], str | None],
    kv_set: Callable[[sqlite3.Connection, str, str], None],
    step_check_dbver: Callable[[sqlite3.Connection, object], str],
    queue_requeue_errors: Callable[[sqlite3.Connection], None],
    queue_has_pending: Callable[[sqlite3.Connection], bool],
    step_fill_random_queue: Callable[[sqlite3.Connection, object], tuple[int, int]],
    step_consume_queue: Callable[[sqlite3.Connection, object, str], int],
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
    dbver_hash = step_check_dbver(con, api)

    if kv_get(con, "dbver_changed", "0") == "1":
        # dbver変化を検知した周回では、既存カードへ NEED_FETCH を付けるだけに留める。
        # これにより、1回実行でのAPI負荷を抑えつつ、後続の queue 消化で段階回収できる。
        con.execute("UPDATE cards_raw SET fetch_status='NEED_FETCH' WHERE konami_id IS NOT NULL")
        kv_set(con, "dbver_changed", "0")
        con.commit()

    queue_requeue_errors(con)

    random_enqueued = 0
    blacklisted = 0
    if not queue_has_pending(con):
        # queue が空のときだけ補充を許可することで、手動投入タスクの優先度を守る。
        random_enqueued, blacklisted = step_fill_random_queue(con, api)

    queue_done = step_consume_queue(con, api, dbver_hash)
    ingested_cards = step_ingest_sqlite(con, dbver_hash)

    kv_set(con, "last_run_at", now_iso())
    con.commit()

    images_done = step_download_images(con, api)

    return RunResult(
        queue_done=queue_done,
        ingested_cards=ingested_cards,
        images_done=images_done,
        api_calls=int(getattr(api, "api_calls", 0)),
        random_enqueued=random_enqueued,
        blacklisted=blacklisted,
    )
