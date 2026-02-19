# ygo-daemon

YGOPRODeck API v7 のカード情報を**少しずつ安全に取得し、SQLite に保存する定期実行ツール**です。  
この README は、初級者でも「何を実行すると何が起きるか」を追いやすいように、**運用の入口 → 実行フロー → 日次確認 → 障害対応**の順で整理しています。

---

## 0. このプロジェクトで最初に知っておくこと

- 本ツールは 1 回で全件同期しません。`python main.py run` を定期実行して進めます。
- API から受け取ったカード JSON は、`cards_raw.json` に**原文のまま（ロスレス）保存**します。
- 手動投入キュー（`request_queue`）を full sync より優先して処理します。
- 取り込みに失敗した JSONL は `data/failed/` に退避し、取得済みデータを失わない設計です。

---

## 1. 実行イメージ（`python main.py run`）

1. ロック取得（多重実行防止）
2. DB 接続・マイグレーション適用
3. `checkDBVer` 実行
4. `dbver` 変化時は既存カードへ `NEED_FETCH` を付与（段階再取得へ移行）
5. `ERROR` キューを `PENDING` に戻す
6. キューを上限まで処理し、staging JSONL を出力
7. キューが空、または処理枠に余りがあるときだけ full sync を 1 ページ進める
8. `meta.next_page_offset` で次回 offset を更新
9. staging JSONL を SQLite へ ingest（失敗時は `data/failed/` へ退避）
10. カード画像を取得
11. ロック解放

> ポイント: 1 サイクルあたりの処理量を制限し、API 負荷と障害時の影響範囲を小さくしています。

---

## 2. セットアップ

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

---

## 3. CLI クイックリファレンス

```bash
python main.py initdb
python main.py queue-add --konami-id 12345678
python main.py queue-add --keyword "Blue-Eyes"
python main.py run
python main.py dict-build
python main.py dict-dump --out data/exports/dict_dump.jsonl --format jsonl
python main.py db-dump --tables kv_store,request_queue --out data/exports/db_dump.csv --format csv
```

- `queue-add` は `--konami-id` / `--keyword` を排他で指定します。
- `run` は単発実行です（繰り返しはスケジューラ側で設定）。
- `dict-build` は辞書構築処理を増分で進めます。
- `dict-dump` は辞書関連テーブルを一括出力します。
- `db-dump` は管理系テーブルを一括出力します（`cards_raw` / `cards_index` は対象外）。

---

## 4. 主要ディレクトリ（迷ったときの参照順）

### 4.1 実装

- `main.py` : API 呼び出し、queue/full sync、ingest、画像取得の実処理
- `app/orchestrator.py` : 1 サイクルの処理順序（運用契約）
- `app/infra/migrate.py` : SQL マイグレーション適用
- `app/db/migrations/` : スキーマ定義
- `app/dict_builder.py` : 辞書構築ロジック

### 4.2 設定・資料

- `config/app.conf` : API URL、リトライ、実行上限
- `config/Help/` : CLI ヘルプ文言
- `docs/` : 技術仕様、運用メモ、minutes
- `tests/` : テスト

### 4.3 実行時データ

- `data/db/ygo.sqlite3` : SQLite 本体
- `data/lock/daemon.lock` : 実行ロック
- `data/staging/*.jsonl` : 取得直後データ
- `data/failed/` : ingest 失敗ファイル
- `data/logs/daemon.log` : 実行ログ
- `data/image/card/` : 画像保存先

---

## 5. 日次運用チェック

- `data/lock/daemon.lock` が残っていないか
- `request_queue` の `ERROR` 件数が増えていないか
- `ingest_files` の `FAILED` 件数が増えていないか
- `data/failed/` に滞留ファイルがないか

---

## 6. 障害時の一次対応

### 6.1 ロック残留

1. プロセス重複起動がないことを確認
2. `data/lock/daemon.lock` を削除
3. `python main.py run` を単発実行して復旧確認

### 6.2 ingest 失敗（`data/failed/` 退避）

1. `daemon.log` と `ingest_files.last_error` を確認
2. 退避 JSONL の破損有無（UTF-8 / JSON 行形式）を確認
3. 必要に応じて `data/staging/` に戻し、`ingest_files` と整合を取って再実行

---

## 7. 開発時チェック

```bash
pytest
ruff check .
```

---

## 8. 既知課題（2026-02-19 時点）

1. **ロックが単純ファイル方式で stale 判定がない**
   - 異常終了時は手動介入が必要
2. **`data/failed/` 再投入の標準手順が未自動化**
   - 手順が文書依存で、運用者スキル差の影響を受ける
3. **監視観点（閾値・アラート）が未定義**
   - `ERROR` / `FAILED` 増加を能動検知しにくい

補足の検討メモは `docs/minutes/` の最新ファイルを参照してください。
