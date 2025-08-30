#!/usr/bin/env python3
"""
既にHuggingFaceにアップロード済みだがデータベースに記録されていないファイルのURLを修正するスクリプト

使用方法:
    python scripts/fix_huggingface_urls.py [--dry-run]
"""

import sys
import os
import logging
import argparse
from pathlib import Path
from typing import Dict, List
import json

# pysqlite3を標準のsqlite3より先にインポート
__import__('pysqlite3')
sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')
import sqlite3

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("HFURLsFixer")


def fix_huggingface_urls(repo_name: str = "Sageen/EventMonitor_1", dry_run: bool = False, 
                        table_name: str = "all_tweets"):
    """HuggingFace URLsを修正
    
    Args:
        repo_name: HuggingFaceリポジトリ名
        dry_run: 実際の更新を行わない場合True
        table_name: 対象テーブル名 ('all_tweets' または 'log_only_tweets')
    """
    
    logger.info(f"Repository: {repo_name}")
    logger.info(f"Table: {table_name}")
    logger.info(f"Dry run: {dry_run}")
    
    # データベース接続
    conn = sqlite3.connect('data/eventmonitor.db')
    cursor = conn.cursor()
    
    try:
        # テーブルに応じて適切なカラムを選択
        if table_name == 'log_only_tweets':
            # log_only_tweetsはmedia_urlsカラムを使用（元のTwitter URL）
            # HuggingFaceパスはimages/username/tweet_id_n.ext形式で保存されている
            cursor.execute(f"""
                SELECT id, username, media_urls
                FROM {table_name}
                WHERE (huggingface_urls IS NULL OR huggingface_urls = '' OR huggingface_urls = '[]')
                AND media_urls IS NOT NULL 
                AND media_urls != '' 
                AND media_urls != '[]'
            """)
        else:
            # all_tweetsはlocal_mediaカラムを使用
            cursor.execute(f"""
                SELECT id, username, local_media
                FROM {table_name}
                WHERE (huggingface_urls IS NULL OR huggingface_urls = '' OR huggingface_urls = '[]')
                AND local_media IS NOT NULL 
                AND local_media != '' 
                AND local_media != '[]'
            """)
        
        tweets = cursor.fetchall()
        logger.info(f"Found {len(tweets)} tweets with missing HuggingFace URLs")
        
        updated_count = 0
        batch_size = 1000  # 1000件ごとにコミット
        
        for i, (tweet_id, username, media_json) in enumerate(tweets):
            try:
                if table_name == 'log_only_tweets':
                    # media_urlsから実際のファイルを探してHF URLsを生成
                    media_urls = json.loads(media_json)
                    hf_urls = []
                    
                    # 実際のファイルを検索
                    images_dir = Path(f'images/{username}')
                    if images_dir.exists():
                        # tweet_idで始まるファイルを検索
                        for idx in range(1, len(media_urls) + 1):
                            # 各種拡張子で実際のファイルを探す
                            found = False
                            for ext in ['jpg', 'jpeg', 'png', 'gif', 'mp4', 'webm']:
                                filename = f"{tweet_id}_{idx}.{ext}"
                                file_path = images_dir / filename
                                
                                if file_path.exists():
                                    # 実際のファイルが見つかった
                                    hf_url = f"https://huggingface.co/datasets/{repo_name}/resolve/main/images/{username}/{filename}"
                                    hf_urls.append(hf_url)
                                    found = True
                                    break
                            
                            if not found:
                                # ファイルが見つからない場合は警告を出してスキップ
                                logger.warning(f"File not found for tweet {tweet_id}, index {idx}")
                    else:
                        # ディレクトリが存在しない場合はスキップ
                        logger.warning(f"Directory not found: {images_dir}")
                        continue
                else:
                    # all_tweetsの場合は従来通りlocal_mediaから処理
                    local_media = json.loads(media_json)
                    hf_urls = []
                    
                    for media_path in local_media:
                        media_file = Path(media_path)
                        
                        # ローカルファイルが存在するか確認
                        if not media_file.exists():
                            # ファイルが存在しない場合、HuggingFaceにアップロード済みと仮定
                            # HuggingFace URLを構築
                            if 'images/' in str(media_file):
                                media_type = 'images'
                            elif 'videos/' in str(media_file):
                                media_type = 'videos'
                            else:
                                continue
                            
                            # ユーザー名を取得（パスから）
                            parts = str(media_file).split('/')
                            if len(parts) >= 2:
                                file_username = parts[-2]  # images/username/file.jpg の username部分
                                filename = parts[-1]
                                
                                hf_url = f"https://huggingface.co/datasets/{repo_name}/resolve/main/{media_type}/{file_username}/{filename}"
                                hf_urls.append(hf_url)
                        else:
                            # ファイルが存在する場合もURLを生成（既にアップロード済みの可能性）
                            if 'images/' in str(media_file):
                                media_type = 'images'
                            elif 'videos/' in str(media_file):
                                media_type = 'videos'
                            else:
                                continue
                            
                            parts = str(media_file).split('/')
                            if len(parts) >= 2:
                                file_username = parts[-2]
                                filename = parts[-1]
                                
                                hf_url = f"https://huggingface.co/datasets/{repo_name}/resolve/main/{media_type}/{file_username}/{filename}"
                                hf_urls.append(hf_url)
                
                if hf_urls:
                    if not dry_run:
                        if table_name == 'log_only_tweets':
                            # log_only_tweetsの場合はuploaded_to_hfもTrueに更新
                            cursor.execute(
                                f'UPDATE {table_name} SET huggingface_urls = ?, uploaded_to_hf = 1 WHERE id = ?',
                                (json.dumps(hf_urls), tweet_id)
                            )
                        else:
                            cursor.execute(
                                f'UPDATE {table_name} SET huggingface_urls = ? WHERE id = ?',
                                (json.dumps(hf_urls), tweet_id)
                            )
                    updated_count += 1
                    
                    if updated_count % 100 == 0:
                        logger.info(f"Processed {updated_count}/{len(tweets)} tweets...")
                    
                    # バッチごとにコミット
                    if updated_count % batch_size == 0 and not dry_run:
                        conn.commit()
                        logger.info(f"Committed {updated_count} updates")
                
            except Exception as e:
                logger.error(f"Failed to process tweet {tweet_id}: {e}")
                continue
        
        # 最後のコミット
        if not dry_run:
            conn.commit()
        
        logger.info(f"{'Would update' if dry_run else 'Updated'} {updated_count} tweets with HuggingFace URLs")
        
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description='既存メディアのHuggingFace URLsをデータベースに記録'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='実際の更新を行わず、更新対象を確認のみ'
    )
    parser.add_argument(
        '--repo',
        default='Sageen/EventMonitor_1',
        help='HuggingFaceリポジトリ名（デフォルト: Sageen/EventMonitor_1）'
    )
    parser.add_argument(
        '--table',
        choices=['all_tweets', 'log_only_tweets', 'both'],
        default='both',
        help='処理対象のテーブル（デフォルト: both）'
    )
    
    args = parser.parse_args()
    
    try:
        if args.table == 'both':
            # 両方のテーブルを処理
            logger.info("Processing all_tweets table...")
            fix_huggingface_urls(args.repo, args.dry_run, 'all_tweets')
            
            logger.info("\nProcessing log_only_tweets table...")
            fix_huggingface_urls(args.repo, args.dry_run, 'log_only_tweets')
            
            logger.info("\nBoth tables processed successfully")
        else:
            # 指定されたテーブルのみ処理
            fix_huggingface_urls(args.repo, args.dry_run, args.table)
            logger.info("Process completed successfully")
    except Exception as e:
        logger.error(f"Process failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()