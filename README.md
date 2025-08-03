# XCrawler

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
git clone https://github.com/ttttdiva/XCrawler.git
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
  days_lookback: 36500  # 過去何日分を取得
  
  # gallery-dl統合（メディア付きツイート全件取得）
  gallery_dl:
    enabled: true  # twscrapeの制限（700-1000件）を超えて全メディア取得

# イベント検出
event_detection:
  enabled: true  # false でクローラーモードに
```

### Twitter Cookie設定方法（必須）

#### 1. Cookie値の取得
1. **ブラウザでTwitter/Xにログイン**
2. **F12キーで開発者ツールを開く**
3. **上部タブから「Application」または「アプリケーション」を選択**
4. **左側メニューから「Cookies」→「https://x.com」を展開**
5. **以下の値をコピー**：
   - `auth_token`の値
   - `ct0`の値

#### 2. .envファイルに設定
```env
TWITTER_ACCOUNT_1_TOKEN=取得したauth_tokenの値
TWITTER_ACCOUNT_1_CT0=取得したct0の値
```

#### 3. Cookieファイルの作成
**GET Cookie.txt LOCALLY**拡張機能を使用して、
x.comのCookieをNetscape形式で`cookies/x.com_cookies.txt`にエクスポートしてください。

**参考**: 正しいファイル形式（Netscape Cookie形式）
```
# Netscape HTTP Cookie File

.x.com	TRUE	/	TRUE	1786716311	auth_token	実際の値
.x.com	TRUE	/	TRUE	1786716311	ct0	実際の値
```

#### 複数アカウント対応
複数アカウントを使用する場合：
- `.env`に`TWITTER_ACCOUNT_2_TOKEN`, `TWITTER_ACCOUNT_2_CT0`等を追加
- `cookies/x.com_cookies_2.txt`, `cookies/x.com_cookies_3.txt`等のファイルを作成

## トラブルシューティング

- **認証エラー**: Cookie（auth_token, ct0）の有効期限を確認
- **レート制限**: 複数アカウントを追加（推奨3アカウント以上）
- **Hugging Faceバックアップ**: rclone.confのパスワードをデフォルトから変更してください（セキュリティ上の理由）

## ライセンス

MIT License