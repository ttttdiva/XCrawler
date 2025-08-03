# バックアップ検証・復旧ガイド

このドキュメントでは、HuggingFaceへのバックアップ状態を検証し、問題がある場合の復旧方法を説明します。

## 1. check_hf_urls.py - バックアップ状態の検証

### 機能1: 既存URLの存在確認

データベースに記録されているHuggingFace URLが実際に存在するか確認します。

```bash
python scripts/check_hf_urls.py
```

検出される問題：
- **通常の欠落**: ファイルが削除された、またはアップロードに失敗した
- **誤ったパス**: DBに記録されたパスが間違っている（例: `videos/` → 実際は `encrypted_videos/`）

出力例：
```
総チェック数: 8863
存在: 8859
欠落: 4
  うち誤ったパス: 4 (images/またはvideos/)
```

### 機能2: バッチ処理と再開機能

大量のURLを効率的にチェックするための機能：

```bash
# バッチサイズを大きくして高速化
python scripts/check_hf_urls.py --batch-size 200 --delay 0.2

# 中断した場合、続きから再開
python scripts/check_hf_urls.py --resume
```

進捗は `data/hf_url_check_progress.json` に保存されます。

### 機能3: 未バックアップツイートの検出

メディアがあるのにHuggingFaceにバックアップされていないツイートを検出：

```bash
python scripts/check_hf_urls.py --check-missing
```

出力例：
```
=== ユーザー別未バックアップツイート数（上位20件） ===
Ixy: 12534件
Cater_Cats: 3421件
...

合計 17279 件のツイートがバックアップされていません
```

## 2. reprocess_missing_files.py - 欠落ファイルの再処理

`check_hf_urls.py`で検出された問題を修正します。

### 基本的な使用方法

```bash
# 全ての欠落ファイルを再処理
python scripts/reprocess_missing_files.py

# ドライランで確認のみ
python scripts/reprocess_missing_files.py --dry-run

# 最初の10件のみ処理
python scripts/reprocess_missing_files.py --limit 10

# 動画ファイルのみ処理
python scripts/reprocess_missing_files.py --file-type videos
```

### 処理フロー

1. **欠落ファイルの読み込み**
   - `data/hf_url_check_progress.json` から欠落URLを取得

2. **ファイル情報の解析**
   - URLからファイルタイプ、ユーザー名、ファイル名を抽出
   - 誤ったパス（`videos/xxx.mp4`）の場合も正しく判定

3. **ローカルファイルの検索**
   - 誤ったパスの場合：拡張子から正しいフォルダを判定
   - 暗号化ファイルの場合：ツイートIDから元のファイル名を推測

4. **再アップロード処理**
   - ローカルファイルを暗号化（必要な場合）
   - 正しいパスでHuggingFaceにアップロード
   - データベースのURLを更新

### 処理対象となるパターン

#### パターン1: 通常の欠落
- DB: `encrypted_videos/username/[暗号化文字列]`
- 状態: ファイルが存在しない
- 対処: ローカルファイルから再暗号化・再アップロード

#### パターン2: 誤ったパス
- DB: `videos/username/1234567890_0.mp4`
- 状態: 暗号化されていないパスが記録されている
- 対処: ローカルの `videos/username/1234567890_0.mp4` を暗号化してアップロード

#### パターン3: media_urlsが空
- DB: huggingface_urlsはあるがmedia_urlsが空（536件）
- 状態: メディアURLの記録が失われている
- 対処: twscrapeでツイートを再取得してメディアをダウンロード・再アップロード

### 出力例

```
[1/4]
処理中: ツイートID 1908900169724363186
  欠落ファイル数: 1
  再処理対象: 画像 0件, 動画 1件
    再アップロード成功: encrypted_videos/Ap04Astral/[暗号化文字列]
  完了: 成功 1件, 失敗 0件

=== 再処理結果サマリー ===
completed: 3件
error: 1件

ファイル処理結果:
  成功: 3件
  失敗: 1件
```

## 3. 未バックアップツイートの処理

17,279件の未バックアップツイートについては、別途バックアップスクリプトの作成が必要です。

### 必要な処理

1. メディアURLからファイルをダウンロード
2. ローカルに保存（`images/`または`videos/`）
3. 暗号化してHuggingFaceにアップロード
4. データベースの`huggingface_urls`を更新

### 注意事項

- 大量のダウンロード・アップロードが発生するため、レート制限に注意
- バッチ処理で段階的に実行することを推奨

## トラブルシューティング

### エラー: ローカルファイルが見つかりません

原因：
- ファイルが削除された
- 別の場所に保存されている
- media_urlsが空でメディア情報が失われている

対処：
- media_urlsが残っている場合：元のメディアURLから再ダウンロードが必要
- media_urlsが空の場合：twscrapeでツイートを再取得（自動的に処理されます）

### エラー: アップロードに失敗

原因：
- HuggingFaceのレート制限
- ネットワークエラー

対処：
- 時間を置いて再実行
- `--limit`オプションで少量ずつ処理