# EventMonitor

イベント参加の告知を見逃さないための自動監視・通知システム。  
Twitter/Xアカウントのツイートを定期的に取得し、イベント関連のポストをAIで検出してDiscordに通知します。

## 使い方

```bash
# 単発実行（1回だけ実行）
python main.py

# 常時稼働（推奨） - デフォルトで1時間おきに自動実行
python main.py --daemon
```

## クイックセットアップ

### 1. インストール
```bash
git clone https://github.com/yourusername/XCrawler.git
cd XCrawler
bash setup.sh
```

### 2. 最小限の設定（`.env`ファイル）
```env
# Twitter認証（必須）
TWITTER_ACCOUNT_1_TOKEN=your_auth_token
TWITTER_ACCOUNT_1_CT0=your_ct0_token

# イベント検出を使うなら（オプション）
GOOGLE_API_KEY=your_google_api_key

# Discord通知を使うなら（オプション）
DISCORD_WEBHOOK_URL=your_webhook_url
```

### 3. 監視対象の設定（`monitored_accounts.csv`）
```csv
username,display_name,event_detection_enabled,account_type
example_user,ユーザー名,1,
```

### 4. 実行
```bash
# 常時稼働（推奨）
python main.py --daemon
```

## Twitter認証の取得方法

1. Twitter/Xをブラウザで開く
2. F12で開発者ツール → Application → Cookies → x.com
3. `auth_token`と`ct0`の値をコピー

## 主な機能

- **自動ツイート収集**: 指定アカウントのツイートを定期的に取得
- **AI判定**: イベント関連ツイートを自動検出（Gemini/GPT-4）
- **Discord通知**: 検出したイベント情報を即座に通知
- **データベース管理**: SQLiteで収集データを管理
- **メディア保存**: 画像・動画を自動ダウンロード

## 詳細設定

### レート制限対策（複数アカウント推奨）

```env
# 複数アカウントでレート制限を回避
TWITTER_ACCOUNT_1_TOKEN=xxx
TWITTER_ACCOUNT_1_CT0=xxx
TWITTER_ACCOUNT_2_TOKEN=yyy
TWITTER_ACCOUNT_2_CT0=yyy
TWITTER_ACCOUNT_3_TOKEN=zzz
TWITTER_ACCOUNT_3_CT0=zzz
```

### 設定ファイル（`config.yaml`）

```yaml
# デーモンモードの実行間隔
daemon:
  interval_minutes: 60  # デフォルト1時間おき

# ツイート取得設定
tweet_settings:
  days_lookback: 36500  # 過去何日分を取得（twscrapeのみの場合およそ300件～500件程度が取得上限。全件取得する場合はplaywright有効化が必要）

# Playwright有効化（全件取得可能だが動作不安定）
playwright:
  enabled: false  # true にすると恐らく全件取得できるが処理が遅く、実装途中の為動作不安定

# イベント検出
event_detection:
  enabled: true  # false でクローラーモードに
```

### Twitter Cookie取得方法

1. **ブラウザでTwitter/Xにログイン**
2. **F12キーで開発者ツールを開く**
3. **上部タブから「Application」または「アプリケーション」を選択**
4. **左側メニューから「Cookies」→「https://x.com」を展開**
5. **以下の値をコピー**：
   - `auth_token` → `.env`の`TWITTER_ACCOUNT_1_TOKEN`に設定
   - `ct0` → `.env`の`TWITTER_ACCOUNT_1_CT0`に設定

※複数アカウントを使う場合は、ログアウトしないまま別アカウントでログイン→Cookie取得を繰り返す
※1ブラウザにつき5アカウントまでしかログインできない為、5個以上Cookieを取得する場合はブラウザユーザーを分けるか別ブラウザでログインする

## トラブルシューティング

- **認証エラー**: Cookie（auth_token, ct0）の有効期限を確認
- **レート制限**: 複数アカウントを追加（推奨3アカウント以上）
- **Playwright**: `playwright install chromium`を実行

## ライセンス

MIT License