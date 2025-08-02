# Hydrus Client連携ガイド

EventMonitorで検出したイベント関連ツイートの画像を、自動的にHydrus Clientにインポートする機能です。

## 設定方法

### 1. Hydrus Client側の設定

1. Hydrus Clientを起動
2. メニューから `services > manage services` を開く
3. `local` タブの `client api` を選択
4. APIポート（デフォルト: 45869）を確認
5. `add` をクリックして新しいアクセス許可を作成
   - 名前: `EventMonitor` など
   - 必要な権限:
     - ✓ import files
     - ✓ edit file tags
     - ✓ search for files (オプション)
6. 生成された64文字のアクセスキーをコピー

### 2. EventMonitor側の設定

`config.yaml` を編集:

```yaml
# Hydrus Client連携設定
hydrus:
  # 連携を有効にするか
  enabled: true  # falseからtrueに変更
  
  # Hydrus Client APIのURL
  api_url: "http://127.0.0.1:45869"  # 必要に応じて変更
  
  # APIアクセスキー（Client APIで発行したキー）
  access_key: "ここに64文字のアクセスキーを貼り付け"
  
  # タグサービスキー（通常は変更不要）
  # tag_service_key: "6c6f63616c2074616773"
```

### 3. 動作確認

設定が完了したら、以下のコマンドでテストを実行:

```bash
python tests/test_hydrus.py
```

正常に動作すれば、テスト画像がHydrusにインポートされ、タグが付与されます。

## 自動インポートの仕組み

### インポート対象
- イベント関連と判定されたツイートの画像のみ
- 既にHydrusに存在する画像はスキップ（SHA256ハッシュで判定）

### 自動付与されるタグ

#### 基本タグ
- `source:twitter` - ソース
- `imported_by:eventmonitor` - インポート元

#### アーティスト情報
- `creator:[表示名]` - ツイートしたアーティスト名（display_name）
- `creator:[ユーザー名]` - ツイートしたアーティスト名（username、display_nameと異なる場合）

#### コンテンツ情報
- `title:[ツイート本文]` - ツイートの本文内容

#### イベント情報
- `event:[イベント名]` - 検出されたイベント名（コミケC103など）
- `date:[YYYY-MM-DD]` - ツイート日付（デフォルトでは無効）

#### 詳細情報
- ツイートURLは「known URLs」として関連付け（タグではなくURLメタデータとして保存）
- `keyword:[キーワード]` - 検出されたキーワード（参加、ブースなど）

### タグ例

実際にインポートされる際のタグ例:

```
source:twitter
imported_by:eventmonitor
creator:テストアーティスト
creator:test_artist
title:【C103 2日目参加】東ホール A-123aでお待ちしております！新刊は〇〇本です。
event:コミケC103
keyword:参加
keyword:ブース
keyword:新刊
```

注: ツイートURLは「known URLs」として関連付けられます。

## カスタマイズ

### インポート設定

```yaml
import_settings:
  # イベント関連ツイートのみインポートするか
  event_tweets_only: true
  
  # 既存ファイルのスキップ（SHA256ハッシュで判定）
  skip_existing: true
```

### タグ設定

```yaml
tag_settings:
  # 基本タグ（必ず付与）
  base_tags:
    - "source:twitter"
    - "imported_by:eventmonitor"
    - "your_custom_tag"  # カスタムタグを追加可能
  
  # タグフォーマット（{name}、{date}などが置換される）
  creator_tag_format: "creator:{name}"
  event_tag_format: "event:{name}"
  date_tag_format: "date:{date}"
  
  # オプション
  include_tweet_url: true  # ツイートURLをknown URLとして関連付け
  include_title_tag: true  # ツイート本文をtitleタグとして追加
  include_date_tag: false  # 日付タグを追加（デフォルトは無効）
  include_detected_keywords: true  # 検出キーワードをタグとして追加
```

## トラブルシューティング

### "API接続に失敗しました"
- Hydrus Clientが起動していることを確認
- APIポート番号が正しいか確認（デフォルト: 45869）
- ファイアウォールでポートがブロックされていないか確認

### "アクセスキーが無効です"
- アクセスキーが正しくコピーされているか確認（64文字）
- Hydrus Client側で権限が正しく設定されているか確認

### "画像のインポートに失敗しました"
- Hydrusのインポートフォルダに書き込み権限があるか確認
- 画像ファイルが破損していないか確認

## 運用上の注意

1. **ストレージ容量**: 大量の画像をインポートする場合は、Hydrusのストレージ容量に注意
2. **重複チェック**: SHA256ハッシュで重複をチェックするため、同じ画像は二度インポートされません
3. **タグの管理**: 自動生成されるタグが多くなる場合があるので、定期的な整理を推奨

## 今後の拡張案

- [ ] Hydrus側のタグ階層構造への対応
- [ ] 画像の自動評価（rating）設定
- [ ] Hydrusのファイルリポジトリへの自動アップロード
- [ ] 特定のタグを持つ画像の自動削除機能