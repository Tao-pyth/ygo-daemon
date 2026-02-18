# ygo-daemon

YGOPRODeck API v7 からカード情報を定期同期し、SQLite に保存するバッチ型デーモンです。  
後任プログラマ向けに、**現行実装と仕様上の前提**を併記しています。

## 概要

本プロジェクトの中核方針は以下です。

- `cardinfo.php` を `misc=yes` 付きで取得する
- APIレスポンスのカードJSONを `cards_raw.json` にロスレス保存する
- キュー（`request_queue`）を優先消化する
- 1回の実行で段階的に進める（定期実行前提）

## 現在の実行モデル（`python main.py run`）

1. 排他ロック取得
2. DB接続 + マイグレーション適用
3. `checkDBVer` による更新検知
4. 変化があれば既存カードを `NEED_FETCH` に戻す
5. キューを処理（`ERROR` は再投入）
6. staging JSONL を SQLite へ取り込み
7. 画像ダウンロード
8. ロック解放

> 補足: 現行は「キュー空時に `NEED_FETCH` を再投入」する実装です。  
> 仕様書にある `offset/num` ベース全件同期の1ページ進行は、今後の拡張対象です。

## セットアップ

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

## 実行コマンド

```bash
python main.py initdb
python main.py queue-add --konami-id 12345678
python main.py queue-add --keyword "Blue-Eyes"
python main.py run
```

- `queue-add` は `--konami-id` と `--keyword` の排他指定です（どちらか1つのみ）。
- `run` は 1回実行です。定期実行（Task Scheduler / cron）で繰り返す前提です。

## ディレクトリ構成

- `main.py` : 実行オーケストレーション（現時点では主要処理を集約）
- `app/infra/migrate.py` : SQLマイグレーション適用
- `app/db/migrations/` : スキーマ定義
- `app/keyword_fetch.py` : キーワード取得・保存・画像DLの補助ロジック
- `config/app.conf` : API URL・タイムアウト・実行上限などの設定集約
- `config/Help/` : CLIヘルプメッセージ定義
- `docs/` : 技術仕様・議事メモ
- `tests/` : 単体テスト

## 永続化先

- `data/db/ygo.sqlite3` : SQLite本体
- `data/state/` : ロックファイル
- `data/staging/` : API取得直後の JSONL
- `data/logs/` : ログ
- `data/image/card/` : カード画像（`{card_id}.jpg`）

## 開発・品質チェック

```bash
pytest
ruff check .
```

## 既知課題（後任向け）

- キュー空時の `offset/num` 全件同期ルートが未接続
- ingest失敗時 JSONL を `failed/` へ退避する処理が未実装
- ロックが単純ファイル方式で stale 対策が弱い

詳細は `docs/minutes/2026-02-18_01_現状整理.md` を参照してください。
