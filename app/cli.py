from __future__ import annotations

import argparse
from typing import Callable, Sequence

from app.config import load_help_text


CommandHandler = Callable[[], int]
QueueAddHandler = Callable[[int | None, str | None], int]
DictBuildHandler = Callable[[int | None, int | None, bool, str | None], int]
DumpHandler = Callable[[str | None, str, str], int]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=load_help_text(
            "cli_description.txt",
            fallback="YGOPRODeck API v7 定期取得デーモン（SQLiteロスレス保存）",
        )
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("initdb", help=load_help_text("initdb_help.txt", fallback="SQLite初期化（テーブル作成）"))

    p_add = sub.add_parser(
        "queue-add",
        help=load_help_text("queue_add_help.txt", fallback="KONAMI_ID または キーワードをキューに追加"),
        epilog=load_help_text(
            "queue_add_epilog.txt",
            fallback=(
                "例:\n"
                "  python main.py queue-add --konami-id 12345678\n"
                "  python main.py queue-add --keyword Blue-Eyes"
            ),
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    group = p_add.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--konami-id",
        type=int,
        help=load_help_text("queue_add_konami_help.txt", fallback="KONAMI_IDでカード詳細取得を予約"),
    )
    group.add_argument(
        "--keyword",
        type=str,
        help=load_help_text("queue_add_keyword_help.txt", fallback="キーワードでカード詳細取得を予約"),
    )

    sub.add_parser("run", help=load_help_text("run_help.txt", fallback="1回実行（タスクスケジューラで定期起動する想定）"))
    p_dict = sub.add_parser("dict-build", help="辞書生成を1回実行")
    p_dict.add_argument("--max-runtime-sec", type=int, default=None)
    p_dict.add_argument("--batch-size", type=int, default=None)
    p_dict.add_argument("--dry-run", action="store_true")
    p_dict.add_argument("--log-level", type=str, default=None)

    p_dict_dump = sub.add_parser("dict-dump", help="辞書関連管理テーブルを一括ダンプ")
    p_dict_dump.add_argument("--tables", type=str, default=None, help="カンマ区切りテーブル名（省略時は標準3テーブル）")
    p_dict_dump.add_argument("--out", type=str, required=True, help="出力ファイルパス")
    p_dict_dump.add_argument("--format", choices=["jsonl", "csv"], default="jsonl")

    p_db_dump = sub.add_parser("db-dump", help="管理テーブルを指定して全件ダンプ")
    p_db_dump.add_argument("--tables", type=str, required=True, help="カンマ区切りテーブル名")
    p_db_dump.add_argument("--out", type=str, required=True, help="出力ファイルパス")
    p_db_dump.add_argument("--format", choices=["jsonl", "csv"], default="jsonl")
    return parser


def dispatch(
    argv: Sequence[str],
    *,
    cmd_initdb: CommandHandler,
    cmd_queue_add: QueueAddHandler,
    cmd_run_once: CommandHandler,
    cmd_dict_build: DictBuildHandler,
    cmd_dict_dump: DumpHandler,
    cmd_db_dump: DumpHandler,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "initdb":
        return cmd_initdb()
    if args.cmd == "queue-add":
        return cmd_queue_add(args.konami_id, args.keyword)
    if args.cmd == "run":
        return cmd_run_once()
    if args.cmd == "dict-build":
        return cmd_dict_build(args.max_runtime_sec, args.batch_size, args.dry_run, args.log_level)
    if args.cmd == "dict-dump":
        return cmd_dict_dump(args.tables, args.out, args.format)
    if args.cmd == "db-dump":
        return cmd_db_dump(args.tables, args.out, args.format)
    return 2
