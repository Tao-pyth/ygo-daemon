# ygo-daemon

YGOPRODeck API v7 からカード情報を定期同期する Python デーモンです。

## 目的

- `cardinfo.php` を `misc=yes` 付きで取得
- API から受け取ったカード JSON を `cards_raw.json` 列へロスレス保存
- `KONAMI_ID` キュー処理を優先し、空のときだけ全件同期（`num`/`offset`）を進める
- JSONL ステージング後に SQLite へバッチ取り込み

## セットアップ

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 実行方法

```bash
python main.py initdb
python main.py queue-add --konami-id 12345678
python main.py run
```

## 開発・品質チェック

```bash
pytest
ruff check .
```

## ディレクトリ（実行後に生成）

- `data/state/` : SQLite DB、ロックファイル
- `data/staging/` : API 取得直後の JSONL
- `data/staged/` : 取り込み成功済み JSONL
- `data/failed/` : 取り込み失敗 JSONL
- `data/logs/` : ログ用ディレクトリ
