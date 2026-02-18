# ygo-daemon

YGOPRODeck API v7 からカード情報を段階的に取得し、SQLite に保存するバッチ型デーモンです。  
この README は、**運用担当・後任開発者向けの引き継ぎ資料**として、実装済み範囲と今後の課題を短時間で把握できる構成に整理しています。

---

## 1. このプロジェクトで守る前提

### 1.1 データ保存ポリシー（最重要）

- `cardinfo.php` は常に `misc=yes` を付与して取得する
- API レスポンスは `cards_raw.json` に **ロスレス保存**（原文保持）
- 検索用の `cards_index` は副次テーブル（原本の代替ではない）

> 実装上は、比較用ハッシュを計算する際のみ `sort_keys=True` で安定化しています。保存する JSON は API で受けたカードオブジェクトをそのまま `json.dumps(..., ensure_ascii=False)` で保持します。

### 1.2 実行モデル

- 1回の `run` は「少し進める」だけ（定期起動前提）
- queue（`request_queue`）を最優先
- ingest 失敗時は JSONL を `data/failed/` に退避し、取得済みデータを失わない

---

## 2. 現在の実行フロー（`python main.py run`）

1. ロック取得（多重実行防止）
2. DB 接続・マイグレーション
3. `checkDBVer` 実行
4. `dbver` 変化時は `cards_raw.fetch_status=NEED_FETCH` を付与（段階再取得の準備）
5. `ERROR` キューを `PENDING` に戻す
6. queue を消化して staging JSONL を出力
7. queue が空（または余力あり）なら fullsync を1ページだけ実行（`offset/num`）
8. `meta.next_page_offset` で次回 offset を更新、無効値なら完了扱い
9. staging JSONL を SQLite に取り込み（失敗時は `data/failed/` へ移動）
10. カード画像を取得
11. ロック解放

### 2.1 設計意図（引き継ぎ向け）

- `dbver` 変化時に即時全件 API 取得しないのは、1実行あたりの負荷と失敗範囲を限定するため
- queue を先に処理することで、運用者の投入要求（手動追加）を最短で反映
- ingest は「ファイル単位」で管理し、失敗時の追跡と再処理をしやすくしている

---

## 3. セットアップ

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

---

## 4. CLI 早見表

```bash
python main.py initdb
python main.py queue-add --konami-id 12345678
python main.py queue-add --keyword "Blue-Eyes"
python main.py run
```

- `queue-add` は `--konami-id` と `--keyword` の排他指定
- `run` は 1 回のみ実行（cron / Task Scheduler で繰り返し呼び出す）

---

## 5. ディレクトリ構成（引き継ぎ用）

### 5.1 コード

- `main.py` : エントリポイント、API 呼び出し、queue 消化、ingest の主処理
- `app/orchestrator.py` : 1サイクルの実行順序を統制
- `app/infra/migrate.py` : SQL マイグレーション適用
- `app/db/migrations/` : スキーマ定義
- `app/keyword_fetch.py` : キーワード取得補助

### 5.2 設定・ドキュメント

- `config/app.conf` : API URL / リトライ / 実行上限など
- `config/Help/` : CLI ヘルプ文言
- `docs/` : 技術仕様・minutes
- `tests/` : テスト

### 5.3 実行時データ

- `data/db/ygo.sqlite3` : SQLite 本体
- `data/lock/daemon.lock` : ロックファイル
- `data/staging/*.jsonl` : API 取得直後のステージング
- `data/failed/` : ingest 失敗ファイル
- `data/logs/daemon.log` : 実行ログ
- `data/image/card/` : 保存済みカード画像

---

## 6. 日次運用チェック

- `data/lock/daemon.lock` が残留していないか
- `request_queue` の `ERROR` 件数が増えていないか
- `ingest_files` の `FAILED` 件数が増えていないか
- `data/failed/` の退避ファイルが滞留していないか

---

## 7. 開発時チェック

```bash
pytest
ruff check .
```

---

## 8. 既知課題（要対応）

1. **ロックが単純ファイル方式で stale 判定がない**
   - 異常終了時に手動復旧が必要
2. **ingest 失敗からの復旧手順が文書化不足**
   - `data/failed/` からの再投入フローが未整備

詳細は `docs/minutes/` の最新記録を参照してください。
