# twscrapeの効率化機能

## 概要
EventMonitorでは、twscrapeのAPI呼び出しを効率化するため、新着ツイートチェック機能を実装しています。
これにより、`twscrape.force_full_fetch: false`の場合、不要なAPI呼び出しを大幅に削減できます。

## 動作モード

### 1. 通常モード（全件取得）
以下のいずれかの条件で動作：
- `twscrape.force_full_fetch: true` が設定されている
- データベースに該当ユーザーの既存ツイートが存在しない

この場合、指定された期間（`days_lookback`）内のすべてのツイートを取得します。

### 2. 効率化モード（新着チェック）
以下の条件がすべて満たされた場合に動作：
- `twscrape.force_full_fetch: false` が設定されている
- データベースに該当ユーザーの既存ツイートが存在する

## 新着チェック処理の詳細

### ステップ1: クイックチェック
1. 最新の1件のツイートのみを取得
2. そのツイートのIDをデータベース内の最新ツイートIDと比較
3. 判定：
   - 既知のツイートID → **新着なし**（空の配列を返して終了）
   - 新しいツイートID → **新着あり**（ステップ2へ）

### ステップ2: 新着ツイート取得
1. 最初から通常モードで再取得を開始
2. 既知のツイートIDに到達するまで取得を継続
3. 取得したツイートから既存のものを除外して返す

## 設定例

```yaml
# config.yaml
tweet_settings:
  # 過去何日分のツイートを取得するか
  days_lookback: 365
  
  # twscrape設定
  twscrape:
    # twscrapeを使用するか
    enabled: true
    # 強制的に全ツイートを取得するか
    force_full_fetch: false  # 効率化モードを有効化
```

## パフォーマンス改善効果

### Before（従来の方式）
- gallery-dl: 毎回全JSON取得
- twscrape: 毎回全ツイートを取得
- API呼び出し数: 数百〜数千回/ユーザー

### After（効率化後）
- 新着がない場合: 
  - gallery-dl: JSON取得をスキップ
  - twscrape: ツイート取得をスキップ
  - API呼び出し: **1回のみ**（新着チェック）
- 新着がある場合: 
  - 通常通り取得を実行

## 注意事項

1. **初回実行時**
   - データベースに既存データがないため、自動的に全件取得モードで動作します

2. **force_full_fetchの使い分け**
   - 通常運用: 
     - `gallery_dl.force_full_fetch: false`
     - `twscrape.force_full_fetch: false`
   - データ復旧・初期化時: 
     - `gallery_dl.force_full_fetch: true`
     - `twscrape.force_full_fetch: true`

3. **gallery-dlとtwscrapeの連携**
   - 両方が独立して新着チェックを実行
   - 新着がない場合は両方とも処理をスキップ
   - 最大限の効率化を実現

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

### 共通新着チェック機能
`src/twitter_monitor.py` の `check_for_new_tweets` メソッド
- 独立した機能として実装
- gallery-dlとtwscrapeの両方から利用
- 最新の1件のツイートIDをチェック
- boolで新着の有無を返す

### gallery-dlでの利用
`get_user_tweets_with_gallery_dl_first` メソッド内
- `gallery_dl.force_full_fetch: false`の場合に新着チェック
- 新着がなければJSON取得をスキップ

### twscrapeでの利用  
`_get_user_tweets_twscrape_internal` メソッド内
- `twscrape.force_full_fetch: false`の場合に新着チェック
- 新着がなければツイート取得をスキップ