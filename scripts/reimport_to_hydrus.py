#!/usr/bin/env python3
"""
データベースに記録されているlocal_mediaファイルを全てHydrus Clientに再インポートするスクリプト
処理済み記録機能付きで、中断しても再開可能

使用方法:
    python scripts/reimport_to_hydrus_v2.py [--dry-run] [--limit N]

オプション:
    --dry-run: 実際にインポートせずに対象ファイルを確認
    --limit N: 処理件数を制限（デフォルト: 全件）
"""

import sys
import os
import asyncio
import argparse
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Set
import yaml
from dotenv import load_dotenv

# プロジェクトのルートディレクトリをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# .envファイルを読み込み
load_dotenv()

# pysqlite3を使用
import pysqlite3 as sqlite3

# プロジェクトのモジュールをインポート
from src.hydrus_client import HydrusClient


# 処理済みレコードを記録するファイル（logsディレクトリ内）
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)  # logsディレクトリがなければ作成
PROGRESS_FILE = logs_dir / "reimport_progress.json"


def load_progress() -> Set[str]:
    """
    処理済みのツイートIDを読み込む
    
    Returns:
        処理済みツイートIDのセット
    """
    if not PROGRESS_FILE.exists():
        return set()
    
    try:
        with open(PROGRESS_FILE, 'r') as f:
            data = json.load(f)
            return set(data.get('processed_tweet_ids', []))
    except Exception as e:
        print(f"警告: 進捗ファイルの読み込みに失敗しました: {e}")
        return set()


def save_progress(processed_ids: Set[str]) -> None:
    """
    処理済みのツイートIDを保存
    
    Args:
        processed_ids: 処理済みツイートIDのセット
    """
    try:
        with open(PROGRESS_FILE, 'w') as f:
            json.dump({
                'processed_tweet_ids': list(processed_ids),
                'last_updated': datetime.now().isoformat()
            }, f, indent=2)
    except Exception as e:
        print(f"警告: 進捗ファイルの保存に失敗しました: {e}")


def clear_progress() -> None:
    """
    進捗ファイルを削除
    """
    if PROGRESS_FILE.exists():
        try:
            PROGRESS_FILE.unlink()
            print("進捗ファイルを削除しました")
        except Exception as e:
            print(f"警告: 進捗ファイルの削除に失敗しました: {e}")


async def get_media_records(db_path: str, limit: Optional[int] = None, skip_ids: Set[str] = None) -> List[Dict[str, Any]]:
    """
    データベースからlocal_mediaがあるレコードを取得
    
    Args:
        db_path: データベースファイルパス
        limit: 取得件数制限
        skip_ids: スキップするツイートIDのセット
        
    Returns:
        レコードのリスト
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    query = """
        SELECT id, username, display_name, tweet_text, tweet_date, 
               tweet_url, local_media, created_at
        FROM all_tweets 
        WHERE local_media IS NOT NULL AND length(local_media) > 2
        ORDER BY created_at DESC
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    
    records = []
    skipped_count = 0
    for row in rows:
        record = dict(zip(columns, row))
        
        # 処理済みの場合はスキップ
        if skip_ids and record['id'] in skip_ids:
            skipped_count += 1
            continue
            
        # local_mediaをJSONパース
        try:
            record['local_media_list'] = json.loads(record['local_media'])
        except:
            record['local_media_list'] = []
        records.append(record)
    
    if skipped_count > 0:
        print(f"処理済みレコードを{skipped_count}件スキップしました")
    
    conn.close()
    return records


async def process_record(hydrus: HydrusClient, record: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
    """
    1件のレコードを処理してHydrusにインポート
    
    Args:
        hydrus: HydrusClientインスタンス
        record: データベースレコード
        dry_run: ドライランモード
        
    Returns:
        処理結果
    """
    result = {
        'tweet_id': record['id'],
        'username': record['username'],
        'total_files': len(record['local_media_list']),
        'processed': 0,
        'skipped': 0,
        'failed': 0,
        'errors': []
    }
    
    # ツイートデータを準備（HydrusClientが期待する形式）
    tweet_data = {
        'id': record['id'],
        'username': record['username'],
        'display_name': record['display_name'],
        'content': record['tweet_text'],
        'text': record['tweet_text'],  # 互換性のため両方設定
        'date': record['tweet_date']
    }
    
    for media_path in record['local_media_list']:
        file_path = Path(media_path)
        
        # ファイル存在チェック
        if not file_path.exists():
            result['skipped'] += 1
            result['errors'].append(f"ファイルが存在しません: {media_path}")
            continue
        
        # 動画ファイルはスキップ
        video_extensions = ['.mp4', '.mov', '.avi', '.webm', '.mkv', '.flv', '.wmv', '.m3u8']
        if file_path.suffix.lower() in video_extensions:
            result['skipped'] += 1
            continue
        
        # images/ディレクトリのファイルのみ処理
        if 'images/' not in str(file_path) and not str(file_path).startswith('images/'):
            result['skipped'] += 1
            continue
        
        if dry_run:
            print(f"  [DRY-RUN] Would import: {media_path}")
            result['processed'] += 1
        else:
            try:
                # ファイルをインポート（既存ファイルは自動的にスキップされ、ハッシュのみ返される）
                file_hash = await hydrus.import_file(file_path)
                
                if file_hash:
                    # 既存ファイルでも、タグとメタデータは更新する（重複チェックはimport_file内で実施済み）
                    
                    # タグを生成して追加
                    tags = hydrus._generate_tags(tweet_data)
                    tags_added = await hydrus.add_tags(file_hash, tags)
                    
                    # ツイートURLを関連付け
                    tweet_url = f"https://twitter.com/{record['username']}/status/{record['id']}"
                    await hydrus.associate_url(file_hash, tweet_url)
                    
                    # ツイート本文をnoteとして追加
                    if record['tweet_text']:
                        import re
                        cleaned_text = record['tweet_text'].strip()
                        cleaned_text = cleaned_text.replace('\t', ' ')
                        cleaned_text = re.sub(r'https?://t\.co/\S+', '', cleaned_text).strip()
                        lines = [line.strip() for line in cleaned_text.split('\n')]
                        cleaned_text = '\n'.join(line for line in lines if line)
                        
                        if cleaned_text:
                            await hydrus.add_note(file_hash, "twitter description", cleaned_text)
                    
                    result['processed'] += 1
                    # より簡潔な表示（既存ファイルかどうかは内部で判断済み）
                    print(f"  ✓ {media_path}")
                else:
                    result['failed'] += 1
                    result['errors'].append(f"インポート失敗: {media_path}")
                    print(f"  ✗ Failed: {media_path}")
                    
            except Exception as e:
                result['failed'] += 1
                result['errors'].append(f"エラー ({media_path}): {str(e)}")
                print(f"  ✗ Error: {media_path} - {e}")
    
    return result


async def main():
    parser = argparse.ArgumentParser(description='Hydrusへのメディア再インポート（再開機能付き）')
    parser.add_argument('--dry-run', action='store_true', help='実際にインポートせずに確認のみ')
    parser.add_argument('--limit', type=int, help='処理件数を制限')
    parser.add_argument('--reset', action='store_true', help='進捗をリセットして最初から実行')
    args = parser.parse_args()
    
    # リセットオプションが指定された場合
    if args.reset:
        clear_progress()
        print("進捗をリセットしました")
    
    # 処理済みIDを読み込み
    processed_ids = load_progress()
    if processed_ids:
        print(f"前回の処理を再開します（処理済み: {len(processed_ids)}件）")
    
    # 設定ファイルを読み込み
    with open('config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # データベースパス
    db_path = 'data/eventmonitor.db'
    
    # レコードを取得
    print(f"データベースから対象レコードを取得中...")
    records = await get_media_records(db_path, args.limit, processed_ids)
    print(f"対象レコード数: {len(records)}")
    
    if not records:
        print("処理対象のレコードがありません")
        if processed_ids and not args.limit:
            print("全レコードの処理が完了しています")
            clear_progress()
        return
    
    # 総ファイル数を計算
    total_files = sum(len(r['local_media_list']) for r in records)
    print(f"総ファイル数: {total_files}")
    
    if args.dry_run:
        print("\n=== DRY RUN MODE ===")
    else:
        # Hydrus接続確認
        print("\nHydrus Clientへの接続を確認中...")
    
    # Hydrusクライアントを初期化
    async with HydrusClient(config) as hydrus:
        if not hydrus.enabled:
            print("エラー: Hydrus連携が無効になっています")
            print("config.yamlでhydrus.enabledをtrueに設定してください")
            return
        
        if not args.dry_run:
            # 接続テスト
            if not hydrus._session_key:
                print("エラー: Hydrus APIに接続できませんでした")
                print("Hydrus Clientが起動していることを確認してください")
                return
            print("Hydrus APIに正常に接続しました")
        
        # 統計情報
        stats = {
            'total_records': len(records),
            'total_files': total_files,
            'processed_records': 0,
            'processed_files': 0,
            'skipped_files': 0,
            'failed_files': 0
        }
        
        # 各レコードを処理
        print(f"\n処理を開始します...")
        
        try:
            for i, record in enumerate(records, 1):
                print(f"\n[{i}/{len(records)}] @{record['username']} - ID: {record['id']} ({len(record['local_media_list'])}ファイル)")
                
                result = await process_record(hydrus, record, args.dry_run)
                
                # 統計を更新
                stats['processed_records'] += 1
                stats['processed_files'] += result['processed']
                stats['skipped_files'] += result['skipped']
                stats['failed_files'] += result['failed']
                
                # 処理済みIDを記録（ドライランでない場合）
                if not args.dry_run:
                    processed_ids.add(record['id'])
                    
                    # 10件ごとに進捗を保存
                    if i % 10 == 0:
                        save_progress(processed_ids)
                        print(f"  → 進捗を保存しました")
                
                # エラーがあれば表示
                if result['errors']:
                    print(f"  エラー: {', '.join(result['errors'][:3])}")
                
                # 進捗表示
                if i % 50 == 0:
                    print(f"\n=== 進捗: {i}/{len(records)} レコード処理済み ===")
                    print(f"  処理: {stats['processed_files']}ファイル")
                    print(f"  スキップ: {stats['skipped_files']}ファイル")
                    print(f"  失敗: {stats['failed_files']}ファイル")
        
        except KeyboardInterrupt:
            print("\n\n処理が中断されました")
            if not args.dry_run:
                save_progress(processed_ids)
                print(f"進捗を保存しました（処理済み: {len(processed_ids)}件）")
                print("次回実行時に自動的に再開されます")
            return
        
        except Exception as e:
            print(f"\n\nエラーが発生しました: {e}")
            if not args.dry_run:
                save_progress(processed_ids)
                print(f"進捗を保存しました（処理済み: {len(processed_ids)}件）")
            raise
        
        # 最終的な進捗を保存
        if not args.dry_run:
            save_progress(processed_ids)
        
        # 処理完了
        print(f"\n{'='*50}")
        print(f"処理完了！")
        print(f"  レコード: {stats['processed_records']}/{stats['total_records']}")
        print(f"  ファイル処理: {stats['processed_files']}")
        print(f"  ファイルスキップ: {stats['skipped_files']}")
        print(f"  ファイル失敗: {stats['failed_files']}")
        print(f"{'='*50}")
        
        # 全件処理完了の場合は進捗ファイルを削除
        if not args.dry_run and stats['processed_records'] == stats['total_records']:
            clear_progress()
            print("全レコードの処理が完了したため、進捗ファイルを削除しました")


if __name__ == '__main__':
    asyncio.run(main())