# ygo-daemon

YGOPRODeck API v7 のカード情報を**段階的に取得**し、SQLite へ保存する定期実行型デーモンです。  
この README は、運用担当者・後任開発者向けの**引き継ぎ資料**として、まず何を見ればよいかが分かる構成に整理しています。

---

## 1. まず押さえるべき運用原則

### 1.1 ロスレス保存（最重要）

- `cardinfo.php` 呼び出しは常に `misc=yes`
- API カード JSON は `cards_raw.json` に**原文のまま保存**
- `cards_index` は検索高速化用の副次テーブル（原本の代替ではない）

> 差分判定のためのハッシュ計算では JSON を安定化（`sort_keys=True`）しますが、保存する JSON 自体は加工しません。

### 1.2 1回実行で「少しだけ進める」

- `run` 1回で全件完了を目指さない（Task Scheduler / cron 前提）
- queue（手動投入）を fullsync より優先
- ingest 失敗時はファイルを `data/failed/` に退避し、取得済みデータを失わない

---

## 2. 実行フロー（`python main.py run`）

1. ロック取得（多重実行防止）
2. DB 接続・マイグレーション適用
3. `checkDBVer` 実行
4. `dbver` 変化時は `cards_raw.fetch_status=NEED_FETCH` を付与し段階再取得へ切替
5. `ERROR` キューを `PENDING` に戻す
6. queue を上限件数まで処理し、staging JSONL を出力
7. queue が空、または処理枠に余りがある場合のみ fullsync 1ページ実行
8. `meta.next_page_offset` に応じて次回 offset を更新
9. staging JSONL を SQLite に取り込み（失敗時は `data/failed/` へ退避）
10. カード画像を取得
11. ロック解放

---

## 3. セットアップ

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

---

## 4. CLI クイックリファレンス

```bash
python main.py initdb
python main.py queue-add --konami-id 12345678
python main.py queue-add --keyword "Blue-Eyes"
python main.py run
python main.py dict-build
```

- `queue-add` は `--konami-id` / `--keyword` の排他指定
- `run` は1回だけ実行（繰り返しはスケジューラ側で設定）
- `dict-build` は辞書構築処理を増分実行

---


## 4.1 `dict-build` の処理概要（`run_incremental_build`）

`python main.py dict-build` は `main.py` の `cmd_dict_build` から `run_incremental_build(...)` を呼び出し、辞書構築を**増分**で進めます。

- `DictBuilderConfig` へ以下を注入して実行
  - lock path / log path / log level
  - 最大実行時間、バッチサイズ
  - ルールセットバージョン、受け入れ閾値
  - dry-run フラグ
- 戻り値 `stats`（処理件数・採用/却下件数・停止理由）をログと標準出力へ要約
- `stop_reason == "exception"` の場合のみ終了コード `1`

> 運用上は `run`（同期処理）とは別ジョブとしてスケジュールし、長時間化を避けるため `--max-runtime-sec` と `--batch-size` を環境に合わせて調整してください。

---

## 5. ディレクトリ早見表

### 5.1 主要コード

- `main.py` : 実処理（API / queue / fullsync / ingest / image）
- `app/orchestrator.py` : 1サイクルの実行順序（運用契約）
- `app/infra/migrate.py` : SQL マイグレーション実行
- `app/db/migrations/` : スキーマ定義
- `app/dict_builder.py` : 辞書構築ロジック

### 5.2 設定・補助資料

- `config/app.conf` : API URL、リトライ、実行上限など
- `config/Help/` : CLI ヘルプ文言
- `docs/` : 技術仕様・議事メモ（minutes）
- `tests/` : テスト

### 5.3 実行時データ

- `data/db/ygo.sqlite3` : SQLite 本体
- `data/lock/daemon.lock` : 実行ロック
- `data/staging/*.jsonl` : 取得直後データ
- `data/failed/` : ingest 失敗ファイル
- `data/logs/daemon.log` : 実行ログ
- `data/image/card/` : 画像保存先

---

## 6. 日次運用チェック（推奨）

- `data/lock/daemon.lock` が残留していないか
- `request_queue` の `ERROR` 件数が増えていないか
- `ingest_files` の `FAILED` 件数が増えていないか
- `data/failed/` のファイルが滞留していないか

---

## 7. 障害時の一次対応

### 7.1 lock 残留

1. プロセス重複起動がないことを確認
2. `data/lock/daemon.lock` を削除
3. `python main.py run` を単発実行して復旧確認

### 7.2 ingest 失敗（`data/failed/` 退避）

1. `daemon.log` と `ingest_files.last_error` を確認
2. 退避 JSONL の破損有無（UTF-8 / JSON 行形式）を確認
3. 必要に応じて `data/staging/` へ戻し、`ingest_files` 状態と整合を取って再実行

---

## 8. 開発時チェック

```bash
pytest
ruff check .
```

---

## 9. 既知課題（2026-02-19 時点）

1. **ロックが単純ファイル方式で stale 判定がない**
   - 異常終了後は手動介入が必要
2. **`data/failed/` 再投入の標準手順が未自動化**
   - 手順は README / minutes に依存し、運用者スキル差の影響を受けやすい
3. **監視観点（閾値・アラート条件）が未定義**
   - ERROR/FAILED の増加を能動検知できる体制が未整備

詳細な経緯は `docs/minutes/` の最新記録を参照してください。
