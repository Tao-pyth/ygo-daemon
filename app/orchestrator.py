from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class RunResult:
    queue_done: int
    ingested_cards: int
    images_done: int
    api_calls: int


def execute_run_cycle(
    con: sqlite3.Connection,
    *,
    api: object,
    kv_get: Callable[[sqlite3.Connection, str, str | None], str | None],
    kv_set: Callable[[sqlite3.Connection, str, str], None],
    step_check_dbver: Callable[[sqlite3.Connection, object], str],
    queue_requeue_errors: Callable[[sqlite3.Connection], None],
    queue_has_pending: Callable[[sqlite3.Connection], bool],
    enqueue_need_fetch_cards: Callable[[sqlite3.Connection, int], int],
    step_consume_queue: Callable[[sqlite3.Connection, object, str], int],
    step_ingest_sqlite: Callable[[sqlite3.Connection, str], int],
    step_download_images: Callable[[sqlite3.Connection, object], int],
    now_iso: Callable[[], str],
    max_need_fetch_enqueue_per_run: int,
) -> RunResult:
    """Run one daemon cycle with injected dependencies."""
    dbver_hash = step_check_dbver(con, api)

    if kv_get(con, "dbver_changed", "0") == "1":
        con.execute("UPDATE cards_raw SET fetch_status='NEED_FETCH' WHERE konami_id IS NOT NULL")
        kv_set(con, "dbver_changed", "0")
        con.commit()

    queue_requeue_errors(con)

    if not queue_has_pending(con):
        enqueue_need_fetch_cards(con, max_need_fetch_enqueue_per_run)

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
    )
