# Windows Task Scheduler ガイド（20分毎）

- 実行プログラム: `python`
- 引数: `main.py run`
- 開始フォルダ: プロジェクトルート
- トリガー: 20分毎
- 多重起動ポリシー: **新しいインスタンスを開始しない**

本アプリ側でも `data/lock/daemon.lock` によるロックを実施しています。
