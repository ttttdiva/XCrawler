#!/usr/bin/env python3
"""
データベースに記録されているlocal_mediaファイルを全てHydrus Clientに再インポートするスクリプト

使用方法:
    python scripts/reimport_to_hydrus.py [--dry-run] [--limit N]

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
from typing import List, Dict, Any, Optional
import yaml

# プロジェクトのルートディレクトリをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# pysqlite3を使用
import pysqlite3 as sqlite3

# プロジェクトのモジュールをインポート
from src.hydrus_client import HydrusClient


async def get_media_records(db_path: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    データベースからlocal_mediaがあるレコードを取得
    
    Args:
        db_path: データベースファイルパス
        limit: 取得件数制限
        
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
    for row in rows:
        record = dict(zip(columns, row))
        # local_mediaをJSONパース
        try:
            record['local_media_list'] = json.loads(record['local_media'])
        except:
            record['local_media_list'] = []
        records.append(record)
    
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
    parser = argparse.ArgumentParser(description='Hydrusへのメディア再インポート')
    parser.add_argument('--dry-run', action='store_true', help='実際にインポートせずに確認のみ')
    parser.add_argument('--limit', type=int, help='処理件数を制限')
    args = parser.parse_args()
    
    # 設定ファイルを読み込み
    with open('config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # データベースパス
    db_path = 'data/eventmonitor.db'
    
    # レコードを取得
    print(f"データベースから対象レコードを取得中...")
    records = await get_media_records(db_path, args.limit)
    print(f"対象レコード数: {len(records)}")
    
    if not records:
        print("処理対象のレコードがありません")
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
        for i, record in enumerate(records, 1):
            print(f"\n[{i}/{len(records)}] Tweet ID: {record['id']} (@{record['username']})")
            print(f"  ファイル数: {len(record['local_media_list'])}")
            
            result = await process_record(hydrus, record, args.dry_run)
            
            stats['processed_records'] += 1
            stats['processed_files'] += result['processed']
            stats['skipped_files'] += result['skipped']
            stats['failed_files'] += result['failed']
            
            if result['errors']:
                print(f"  エラー:")
                for error in result['errors'][:3]:  # 最初の3つのエラーのみ表示
                    print(f"    - {error}")
            
            # 進捗表示
            if i % 10 == 0:
                print(f"\n--- 進捗: {i}/{len(records)} レコード処理済み ---")
                print(f"    処理済みファイル: {stats['processed_files']}")
                print(f"    スキップ: {stats['skipped_files']}")
                print(f"    失敗: {stats['failed_files']}")
        
        # 最終統計
        print("\n" + "="*50)
        print("処理完了")
        print("="*50)
        print(f"処理レコード数: {stats['processed_records']}/{stats['total_records']}")
        print(f"処理ファイル数: {stats['processed_files']}/{stats['total_files']}")
        print(f"スキップ: {stats['skipped_files']}")
        print(f"失敗: {stats['failed_files']}")
        
        if args.dry_run:
            print("\n※ DRY RUNモードのため、実際のインポートは行われていません")


if __name__ == "__main__":
    asyncio.run(main())