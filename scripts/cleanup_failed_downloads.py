#!/usr/bin/env python3
"""
ダウンロードに失敗したメディア付きツイートのレコードを削除するスクリプト

media_urlsは存在するがlocal_mediaが空のレコードを削除します。
これにより、次回実行時に再ダウンロードが試行されます。

使用方法:
    python scripts/cleanup_failed_downloads.py [username]
    
    username: 対象のTwitterユーザー名（省略時は全ユーザー）
"""

import sys
import os
import argparse
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# pysqlite3を使用
import pysqlite3 as sqlite3

def cleanup_failed_downloads(username=None):
    """
    ダウンロードに失敗したメディア付きツイートを削除
    
    Args:
        username: 特定のユーザーのみ処理する場合に指定
    """
    db_path = project_root / 'data' / 'eventmonitor.db'
    
    if not db_path.exists():
        print(f"Error: Database not found at {db_path}")
        return
    
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    try:
        # 条件を構築
        base_condition = """
            media_urls IS NOT NULL 
            AND media_urls != '[]'
            AND (local_media IS NULL OR local_media = '[]')
        """
        
        if username:
            condition = f"username = '{username}' AND {base_condition}"
            target_desc = f"@{username}"
        else:
            condition = base_condition
            target_desc = "all users"
        
        # 対象レコード数を確認
        cursor.execute(f"""
            SELECT COUNT(*) FROM all_tweets 
            WHERE {condition}
        """)
        count = cursor.fetchone()[0]
        
        if count == 0:
            print(f"No failed downloads found for {target_desc}")
            return
        
        print(f"Found {count} tweets with failed media downloads for {target_desc}")
        
        # ユーザーごとの内訳を表示
        if not username:
            cursor.execute(f"""
                SELECT username, COUNT(*) as cnt 
                FROM all_tweets 
                WHERE {condition}
                GROUP BY username
                ORDER BY cnt DESC
            """)
            print("\nBreakdown by user:")
            for user, cnt in cursor.fetchall():
                print(f"  @{user}: {cnt} tweets")
        
        # 確認プロンプト
        response = input(f"\nDelete these {count} records? (y/N): ")
        if response.lower() != 'y':
            print("Cancelled")
            return
        
        # 削除実行
        cursor.execute(f"""
            DELETE FROM all_tweets 
            WHERE {condition}
        """)
        
        conn.commit()
        deleted = cursor.rowcount
        print(f"\nSuccessfully deleted {deleted} records")
        
        # 削除後の統計を表示
        if username:
            cursor.execute("""
                SELECT COUNT(*) FROM all_tweets WHERE username = ?
            """, (username,))
            remaining = cursor.fetchone()[0]
            print(f"Remaining records for @{username}: {remaining}")
        else:
            cursor.execute("SELECT COUNT(*) FROM all_tweets")
            total = cursor.fetchone()[0]
            print(f"Total remaining records: {total}")
            
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(
        description='Clean up tweets with failed media downloads'
    )
    parser.add_argument(
        'username',
        nargs='?',
        help='Twitter username (without @). If omitted, processes all users'
    )
    parser.add_argument(
        '--no-confirm',
        action='store_true',
        help='Skip confirmation prompt'
    )
    
    args = parser.parse_args()
    
    # ユーザー名から@を除去
    username = args.username
    if username and username.startswith('@'):
        username = username[1:]
    
    cleanup_failed_downloads(username)

if __name__ == '__main__':
    main()