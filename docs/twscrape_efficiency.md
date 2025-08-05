# twscrapeの効率化機能

## 概要
EventMonitorでは、twscrapeのAPI呼び出しを効率化するため、新着ツイートチェック機能を実装しています。
これにより、`force_full_fetch: false`の場合、不要なAPI呼び出しを大幅に削減できます。

## 動作モード

### 1. 通常モード（全件取得）
以下のいずれかの条件で動作：
- `force_full_fetch: true` が設定されている
- データベースに該当ユーザーの既存ツイートが存在しない

この場合、指定された期間（`days_lookback`）内のすべてのツイートを取得します。

### 2. 効率化モード（新着チェック）
以下の条件がすべて満たされた場合に動作：
- `force_full_fetch: false` が設定されている
- データベースに該当ユーザーの既存ツイートが存在する

## 新着チェック処理の詳細

### ステップ1: クイックチェック
1. 最新の5件のツイートのみを取得
2. 各ツイートのIDをデータベース内の最新ツイートIDと比較
3. 判定：
   - 既知のツイートIDに到達 → **新着なし**（空の配列を返して終了）
   - 5件すべてが新しいツイート → **新着あり**（ステップ2へ）

### ステップ2: 新着ツイート取得
1. 最初から通常モードで再取得を開始
2. 既知のツイートIDに到達するまで取得を継続
3. 取得したツイートから既存のものを除外して返す

## 設定例

```yaml
# config.yaml
tweet_settings:
  # 強制的に全ツイートを取得するか
  force_full_fetch: false  # 効率化モードを有効化
  
  # 過去何日分のツイートを取得するか
  days_lookback: 365
```

## パフォーマンス改善効果

### Before（従来の方式）
- 毎回ユーザーのすべてのツイートを取得
- API呼び出し数: 数百〜数千回/ユーザー

### After（効率化後）
- 新着がない場合: **5回のAPI呼び出しのみ**
- 新着がある場合: 新着分 + α のAPI呼び出し

## 注意事項

1. **初回実行時**
   - データベースに既存データがないため、自動的に全件取得モードで動作します

2. **force_full_fetchの使い分け**
   - 通常運用: `false`（効率化モード推奨）
   - データ復旧・初期化時: `true`（全件取得）

3. **gallery-dlとの連携**
   - gallery-dlは常に全件取得を行います（効率化の対象外）
   - twscrapeは補完的な役割のため、効率化が特に重要です

## ログ出力例

### 新着なしの場合
```
twscrape: Checking for new tweets only (quick check mode)
twscrape: No new tweets found for @username (reached known tweet 123456789)
```

### 新着ありの場合
```
twscrape: Checking for new tweets only (quick check mode)
twscrape: New tweets detected for @username, switching to normal fetch mode
twscrape: Fetched 15 unique tweets for @username
```

## 実装詳細

該当コード: `src/twitter_monitor.py` の `_get_user_tweets_twscrape_only` メソッド（行248-402）

主要な変数：
- `check_for_new_tweets_only`: 新着チェックモードの有効/無効
- `latest_tweet_id`: データベース内の最新ツイートID
- `check_limit`: クイックチェックで確認するツイート数（デフォルト: 5）