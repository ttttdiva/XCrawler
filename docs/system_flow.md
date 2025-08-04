# EventMonitor システムフロー仕様

## 概要
EventMonitorは、Twitterから特定アカウントのツイートを取得し、イベント関連の投稿を検出してDiscordに通知するシステム。
全てのメディアファイルはHuggingFaceにバックアップされる。

## 主要コンポーネント

### 1. main.py
メインの処理フロー：

1. **アカウントごとのループ処理**
   - monitored_accounts.csvからアカウントリストを読み込み
   - 各アカウントについて以下を実行：

2. **ツイート取得** (TwitterMonitor)
   - gallery-dl優先でメディア付きツイートを取得
   - twscrapeで補完的にテキストツイートを取得
   - 既存のツイートはデータベースを参照してフィルタリング
   - リツイート/リポストは除外

3. **メディアダウンロード**
   - 新規ツイートの画像を`images/{username}/`にダウンロード
   - 動画を`videos/{username}/`にダウンロード
   - ファイル名は`{tweet_id}_{index}.{ext}`形式

4. **HuggingFaceバックアップ** (BackupManager)
   - RCloneで暗号化（設定されている場合）
   - HuggingFaceにアップロード
   - アップロード成功時、URLをデータベースに記録

5. **データベース保存** (DatabaseManager)
   - all_tweetsテーブルに以下を保存：
     - id, username, tweet_text, media_urls
     - local_images (ローカルパス)
     - huggingface_urls (バックアップURL)

6. **イベント検出** (EventDetector)
   - LLM（GPT-4/Gemini）でイベント関連ツイートを判定
   - event_tweetsテーブルに保存

7. **Discord通知** (DiscordNotifier)
   - イベント関連ツイートをDiscordに投稿

### 2. データベース構造

**all_tweets テーブル**
- 全ツイートを保存
- huggingface_urls: JSON配列で複数のバックアップURLを保持
- media_urls: 元のTwitterメディアURL

**event_tweets テーブル**
- イベント関連と判定されたツイートのみ

## 現在の処理フロー（改良後）

```python
# 各アカウントの処理
for account in monitored_accounts:
    # 1. ツイート取得
    tweets = await twitter_monitor.get_user_tweets(username)
    
    # 2. 新規ツイートのフィルタリング
    new_tweets = db_manager.filter_new_tweets(tweets)
    
    # 3. メディアダウンロード
    for tweet in new_tweets:
        await download_images_and_videos(tweet)
    
    # 4. バックアップ処理（先に実行）
    if backup_enabled:
        try:
            await backup_manager.backup_tweets(new_tweets)
            # 成功: huggingface_urlsが設定される
        except:
            # 失敗: huggingface_urlsは空配列
            for tweet in new_tweets:
                tweet['huggingface_urls'] = []
    
    # 5. データベース保存
    db_manager.save_all_tweets(new_tweets)
    
    # 6. イベント検出・通知（通常通り）
```

## バックアップ処理の詳細

### BackupManager.backup_tweets()
1. 各ツイートのメディアファイルを処理
2. 暗号化（有効な場合）
3. HuggingFaceにアップロード
4. 成功したURLをデータベースに即座に更新

### エラーハンドリング
- アップロード失敗時はhuggingface_urlsが空のままDBに保存
- 部分的な成功も記録される（一部の画像のみアップロード成功など）

## 再処理フロー

### retry_upload.py
定期的に実行して、バックアップ失敗分を再処理：

1. **対象の特定**
   - `media_urls`があるが`huggingface_urls`が空のレコード
   - `huggingface_urls`があるが実際のファイルが404のレコード

2. **再アップロード処理**
   - ローカルファイルが存在する場合：再アップロード
   - ローカルファイルがない場合：media_urlsから再ダウンロード→アップロード

3. **データベース更新**
   - 成功したURLでhuggingface_urlsを更新

## 問題点と課題

### 現在の実装の問題
1. **処理が複雑すぎる**
   - バックアップとDB保存のタイミングが複雑
   - エラーハンドリングが過剰

2. **データ不整合のリスク**
   - バックアップ前にDB保存すると、失敗時にhuggingface_urlsがNULL
   - バックアップ後にDB保存すると、失敗時にデータが失われる

3. **パフォーマンス**
   - アカウントごとにバックアップ処理を実行（本来は不要）

### 推奨される簡略化
1. バックアップ失敗を許容する設計
2. retry_upload.pyで定期的に再処理
3. 複雑なトランザクション処理を削除

## 簡略化案の詳細

### 処理フロー
```
1. ツイート取得・ダウンロード
   - 全アカウントのツイートを取得
   - 各ツイートの画像・動画をローカルにダウンロード
   - この時点で全データがローカルに存在

2. DB保存
   - all_tweetsテーブルに保存（huggingface_urls=[]で初期化）
   - この時点でツイート情報は永続化済み

3. バックアップ処理
   - アカウントごとに画像・動画をHuggingFaceにアップロード
   - 成功したファイルのURLをDBに即座に更新
   - 失敗しても処理は継続

4. データベースファイルのバックアップ
   - 最後に1回だけ実行
   - eventmonitor.db → HuggingFaceにアップロード
   - all_tweets.parquet → HuggingFaceにアップロード
```

### バックアップのタイミング詳細

#### 画像・動画のアップロード
- **タイミング**: 各アカウント処理の直後
- **単位**: ファイルごと
- **DB更新**: アップロード成功後、即座にhuggingface_urlsを更新
- **再試行**: 失敗時は最大3回まで再試行
- **レート制限対応**: 
  - HuggingFaceから429エラーが返ってきた場合
  - エラーメッセージから待機時間を抽出（例: "retry in 3600 seconds"）
  - 要求された時間 + 1秒待機してから再試行
- **例**:
  ```
  user1の処理:
    - 画像1.jpg → アップロード成功 → DB更新
    - 画像2.jpg → 429エラー → 3601秒待機 → 再アップロード成功 → DB更新
    - 動画1.mp4 → エラー → 再試行1 → 再試行2 → 再試行3 → 失敗 → スキップ
  user2の処理:
    - ...
  ```

#### データベースファイルのアップロード
- **タイミング**: 全アカウント処理完了後
- **頻度**: main.py実行ごとに1回
- **内容**:
  - `data/eventmonitor.db` (SQLiteファイル全体)
  - `all_tweets.parquet` (all_tweetsテーブルのエクスポート)
- **理由**: 頻繁にアップロードする必要がない

### 失敗時の挙動
- 画像・動画のアップロード失敗 → 3回再試行後、huggingface_urlsが空のままDBに残る
- DBファイルのアップロード失敗 → 次回実行時に再アップロード
- retry_upload.pyが定期的に失敗分を検出して再処理

### エラーハンドリングの詳細

#### アップロード再試行ロジック
```python
def upload_with_retry(file_path, max_retries=3):
    for attempt in range(max_retries):
        try:
            # アップロード試行
            upload_file(file_path)
            return True  # 成功
        except RateLimitError as e:
            # レート制限エラー: 待機時間を抽出
            wait_time = extract_wait_time(e.message) + 1
            logger.info(f"Rate limit hit, waiting {wait_time}s")
            time.sleep(wait_time)
            continue  # 再試行
        except Exception as e:
            # その他のエラー
            if attempt < max_retries - 1:
                logger.warning(f"Upload failed (attempt {attempt+1}), retrying...")
                time.sleep(1)  # 短い待機
            else:
                logger.error(f"Upload failed after {max_retries} attempts")
                return False  # 失敗
```

#### レート制限の待機時間抽出
- "retry in X seconds" → X + 1秒待機
- "retry in X minutes" → X * 60 + 1秒待機  
- "retry in X hours" → X * 3600 + 1秒待機

## リファクタリング実施内容（2025-01-27）

### main.py の変更
1. **処理フローの簡略化**
   - バックアップ前にDB保存する方式に変更
   - バックアップ失敗してもプロセスは継続
   - 各アカウント処理後に即座にバックアップ

2. **エラーハンドリングの簡素化**
   - 複雑なトランザクション処理を削除
   - バックアップ失敗時の特別な処理を削除

### BackupManager の変更
1. **統合されたアップロードメソッド**
   - `_upload_file_with_retry`: 3回まで再試行、レート制限対応
   - 暗号化/非暗号化を統一的に処理

2. **データベースバックアップの分離**
   - `upload_database_backup`: 全アカウント処理後に1回実行
   - SQLiteファイルとParquetファイルを別々にアップロード

3. **再試行ロジックの改善**
   - レート制限エラー検出の強化
   - 待機時間の自動抽出（+1秒バッファ付き）

### 処理の流れ
```
foreach アカウント:
  1. ツイート取得
  2. メディアダウンロード
  3. DB保存（huggingface_urls=[]）
  4. バックアップ実行
     - 成功: DBのhuggingface_urlsを更新
     - 失敗: そのまま（retry_upload.pyで後処理）
  
最後に:
  5. データベースファイルをバックアップ
```

## 設定ファイル

### config.yaml
```yaml
huggingface_backup:
  enabled: true
  repo_name: EventMonitor_1
  include_images: true
  backup_database: true
  encryption:
    enabled: true
    remote_name: hf-crypt
```

### monitored_accounts.csv
```csv
username,display_name,event_detection_enabled,account_type
user1,ユーザー1,1,
user2,ユーザー2,1,
loguser,ログ専用,0,log
```

## LLM判定の処理フロー（2025-08-04追記）

### イベント検出の仕組み
EventMonitorでは、取得したツイートをLLM（GPT-4/Gemini）でイベント関連かどうか判定します。

### LLM判定が実行される場所

1. **gallery_dl_extractor.py** (`fetch_and_analyze_tweets`メソッド)
   - gallery-dlで取得したメディア付きツイートを判定
   - **all_tweetsテーブルに存在しない新規ツイートのみ**をLLM判定
   - 判定結果としてイベントツイートのリストを返す

2. **main.py** (214-224行目)
   - twscrapeで取得したテキストツイートを判定
   - `source != 'gallery-dl'`のツイートのみ処理（gallery-dlで処理済みを除外）
   - **新規ツイート**（`filter_new_tweets`でフィルタ済み）のみを判定

### 重複判定の防止策

#### データベースによる重複チェック
- **all_tweetsテーブル**: 全ツイートの履歴を保存
- **event_tweetsテーブル**: イベントと判定されたツイートを保存
- 新規ツイートのフィルタリング: `DatabaseManager.filter_new_tweets()`でall_tweetsに存在しないツイートのみ抽出

#### 処理フローの詳細
```
1. gallery-dl処理
   ├─ 全メディアツイートを取得
   ├─ all_tweetsテーブルをチェック
   ├─ 新規ツイートのみLLM判定 ← ここで重複防止
   └─ イベントツイートを返す

2. twscrape処理
   ├─ テキストツイートを取得
   └─ gallery-dlと重複を除外

3. main.py処理
   ├─ filter_new_tweetsで新規ツイートのみ抽出
   ├─ gallery-dlのイベントツイートを統合
   └─ twscrapeツイート(source != 'gallery-dl')のみLLM判定 ← ここでも重複防止
```

### LLM判定の実装
実際のLLM判定ロジックは`event_detector.py`の`detect_event_tweets`メソッドに集約されています：
- キーワードによる簡易チェック
- LLMによる詳細分析（GPT-4/Geminiのフォールバック）
- イベント情報の抽出（スペース番号、サークル名など）

### 設計の理由
- **gallery-dlとtwscrapeが異なるタイミングでツイートを取得**: それぞれ独立して新規ツイートを判定
- **sourceフィールドで識別**: ツイートの取得元を記録し、重複処理を防止
- **LLM判定ロジックは1箇所に集約**: メンテナンス性を確保

この設計により、同じツイートが複数回LLM判定されることを防ぎつつ、異なるソースから取得したツイートを適切に処理できます。