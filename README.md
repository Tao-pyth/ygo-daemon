# ygo-daemon

YGOPRODeck API v7 からカード情報を定期同期し、SQLite に保存するバッチ型デーモンです。  
本READMEは、運用担当・後任開発者が **「何が実装済みで、どこが未実装か」** を素早く把握できるように整理しています。

---

## 1. プロジェクトの目的

- `cardinfo.php` を常に `misc=yes` 付きで取得する
- APIレスポンスを `cards_raw.json` に **ロスレス保存** する
- キュー（`request_queue`）を優先消化する
- 1回の実行で少しずつ進める（Task Scheduler / cron 前提）

> 補足: APIレスポンスの「原本保持」が最優先です。解析用の `cards_index` は副次テーブルとして扱います。

---

## 2. 実行フロー（`python main.py run`）

1. ロック取得（多重実行防止）
2. DB接続・マイグレーション
3. `checkDBVer` で更新検知
4. `dbver` 変化時は `cards_raw.fetch_status=NEED_FETCH` へ更新
5. `ERROR` キューを `PENDING` に戻す
6. キューが空なら `NEED_FETCH` カードを再投入
7. キューを消化して staging JSONL を生成
8. staging JSONL を SQLite へ取り込み
9. カード画像を取得
10. ロック解放

### 2.1 重要な設計意図

- `dbver` 変化時も即時全件再取得しません。`NEED_FETCH` への印付けのみ行い、定期実行で段階回収します。
- 1回実行での処理量を上限化し、失敗時影響を局所化します。
- queue優先を維持しつつ、queueが空のときだけ再同期対象を補充します。

---

## 3. セットアップ

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

---

## 4. CLIコマンド

```bash
python main.py initdb
python main.py queue-add --konami-id 12345678
python main.py queue-add --keyword "Blue-Eyes"
python main.py run
```

- `queue-add` は `--konami-id` と `--keyword` の排他指定（どちらか1つのみ）
- `run` は1回実行。スケジューラで繰り返し呼び出す前提

---

## 5. ディレクトリ構成

### 5.1 アプリケーション

- `main.py` : エントリポイント。CLI・API呼び出し・取り込み処理の統合
- `app/orchestrator.py` : 1サイクル実行順序の制御
- `app/infra/migrate.py` : SQLマイグレーション適用
- `app/db/migrations/` : DBスキーマ定義
- `app/keyword_fetch.py` : キーワード検索関連の補助ロジック

### 5.2 設定・ドキュメント

- `config/app.conf` : API・リトライ・上限値など運用パラメータ
- `config/Help/` : CLIヘルプ本文
- `docs/` : 技術仕様・議事録
- `tests/` : ユニットテスト

### 5.3 実行時データ

- `data/db/ygo.sqlite3` : SQLite本体
- `data/lock/daemon.lock` : 排他ロック
- `data/staging/*.jsonl` : API取得直後の中間データ
- `data/failed/` : 取り込み失敗ファイル
- `data/logs/daemon.log` : アプリケーションログ
- `data/image/card/` : カード画像

---

## 6. 運用時チェックポイント

- `data/lock/daemon.lock` が残留していないか
- `request_queue` に `ERROR` が滞留していないか
- `ingest_files` の `FAILED` 件数が増えていないか
- `data/failed/` に退避ファイルが積み上がっていないか

---

## 7. 開発時の確認コマンド

```bash
pytest
ruff check .
```

---

## 8. 既知課題（要対応）

- 仕様書にある `offset/num` ベース全件同期が実行フローへ未接続
- ロックが単純ファイル方式で stale lock 対策が弱い
- ingest失敗時の再取り込み運用（再実行手順）の明文化不足

詳細は最新の minutes を参照してください。  
`docs/minutes/2026-02-18_02_コメント充実とREADME整理.md`
