from __future__ import annotations

import argparse
from typing import Callable, Sequence


CommandHandler = Callable[[], int]
QueueAddHandler = Callable[[int | None, str | None], int]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="YGOPRODeck API v7 定期取得デーモン（SQLiteロスレス保存）")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("initdb", help="SQLite初期化（テーブル作成）")

    p_add = sub.add_parser(
        "queue-add",
        help="KONAMI_ID または キーワードをキューに追加",
        epilog=(
            "例:\n"
            "  python main.py queue-add --konami-id 12345678\n"
            "  python main.py queue-add --keyword Blue-Eyes"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    group = p_add.add_mutually_exclusive_group(required=True)
    group.add_argument("--konami-id", type=int, help="KONAMI_IDでカード詳細取得を予約")
    group.add_argument("--keyword", type=str, help="キーワードでカード詳細取得を予約")

    sub.add_parser("run", help="1回実行（タスクスケジューラで定期起動する想定）")
    return parser


def dispatch(
    argv: Sequence[str],
    *,
    cmd_initdb: CommandHandler,
    cmd_queue_add: QueueAddHandler,
    cmd_run_once: CommandHandler,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "initdb":
        return cmd_initdb()
    if args.cmd == "queue-add":
        return cmd_queue_add(args.konami_id, args.keyword)
    if args.cmd == "run":
        return cmd_run_once()
    return 2
