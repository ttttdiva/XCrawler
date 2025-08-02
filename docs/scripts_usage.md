# EventMonitor スクリプト使用ガイド

このドキュメントでは、EventMonitorプロジェクトに含まれる各種スクリプトの使用方法を説明します。

## 目次
- [メンテナンススクリプト](#メンテナンススクリプト)
- [データ再処理スクリプト](#データ再処理スクリプト)
- [削除スクリプト](#削除スクリプト)

## メンテナンススクリプト

### reprocess_tweets.py
過去のツイートを再処理してイベント検知を行うスクリプトです。

```bash
# 特定のアカウントの全ツイートを再処理
python scripts/reprocess_tweets.py username

# 例：alicesoftplusのツイートを再処理
python scripts/reprocess_tweets.py alicesoftplus
```

**用途：**
- LLMプロンプトを改良した後の再検知
- 見逃したイベントツイートの再チェック
- データベースの不整合修正

### reprocess_videos.py
動画URLが取得できなかったツイートの動画を再取得するスクリプトです。

```bash
# データベース内の動画なしツイートを再処理
python scripts/reprocess_videos.py

# ツイートIDを直接指定して処理
python scripts/reprocess_videos.py --tweet-ids 1234567890,0987654321

# ユーザー名で絞り込み
python scripts/reprocess_videos.py --username alice_soft
```

**用途：**
- 動画取得に失敗したツイートの修復
- 新しい動画取得方法の適用
- バックアップ用の動画URL取得

## データ再処理スクリプト

### download_tweet_media.py
ツイートIDを指定してメディア（画像・動画）をダウンロードするスクリプトです。

```bash
# 単一のツイートのメディアをダウンロード
python scripts/download_tweet_media.py 1234567890

# 複数のツイートを処理
python scripts/download_tweet_media.py 1234567890 0987654321 1111111111
```

**用途：**
- 特定ツイートのメディア再取得
- 手動でのメディアバックアップ
- デバッグ・検証作業

## 削除スクリプト

### delete_log_only_accounts.py
log_onlyアカウントのデータをデータベースから削除するスクリプトです。

```bash
# ktr_micとyaponishiのデータを削除（ハードコード）
python scripts/delete_log_only_accounts.py
```

**機能：**
- log_only_tweetsテーブルからデータを削除
- 削除前に件数を表示
- SQLAlchemyを使用した安全な削除処理

**注意事項：**
- 現在はktr_micとyaponishiがハードコードされています
- 他のアカウントを削除する場合はスクリプトの編集が必要

### delete_hf_files.py
HuggingFaceリポジトリから特定アカウントのメディアファイルを削除するスクリプトです。

```bash
# ktr_micとyaponishiのメディアを削除（ハードコード）
python scripts/delete_hf_files.py
```

**機能：**
- encrypted_images/とencrypted_videos/から該当ファイルを削除
- バッチ処理で効率的に削除
- 環境変数からHuggingFace認証情報を取得

**必要な環境変数：**
```bash
HUGGINGFACE_API_KEY=your_api_key
HUGGINGFACE_REPO_NAME=EventMonitor_1  # オプション
```

## 使用上の注意

1. **バックアップ**: 削除スクリプトを実行する前に、必要に応じてデータのバックアップを取ってください。

2. **環境変数**: 各スクリプトは`.env`ファイルから設定を読み込みます。実行前に適切な環境変数が設定されていることを確認してください。

3. **実行権限**: スクリプトはプロジェクトのルートディレクトリから実行してください。
   ```bash
   cd /path/to/EventMonitor
   python scripts/script_name.py
   ```

4. **依存関係**: スクリプトを実行する前に、仮想環境をアクティベートしてください。
   ```bash
   source venv/bin/activate  # Linux/Mac
   # または
   venv\Scripts\activate  # Windows
   ```

## トラブルシューティング

### "Module not found" エラー
プロジェクトのルートディレクトリから実行していることを確認してください。

### データベース接続エラー
`data/eventmonitor.db`が存在することを確認してください。

### HuggingFace認証エラー
`.env`ファイルに`HUGGINGFACE_API_KEY`が正しく設定されていることを確認してください。

### レート制限エラー
HuggingFace APIのレート制限に達した場合は、しばらく待ってから再実行してください。