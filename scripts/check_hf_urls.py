#!/usr/bin/env python3
"""
HuggingFace URLの存在確認をバッチ処理で実行するスクリプト
中間結果を保存して再開可能
"""

import sys
__import__('pysqlite3')
sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')

import json
import sqlite3
import requests
from pathlib import Path
from collections import defaultdict
import time
from urllib.parse import urlparse
import argparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

class HFURLChecker:
    def __init__(self, batch_size=50, delay=0.5):
        self.batch_size = batch_size
        self.delay = delay
        self.session = requests.Session()
        self.results_file = Path("data/hf_url_check_results.json")
        self.progress_file = Path("data/hf_url_check_progress.json")
        
    def load_progress(self):
        """進捗状況を読み込み"""
        if self.progress_file.exists():
            with open(self.progress_file, 'r') as f:
                return json.load(f)
        return {
            'checked_urls': {},
            'last_index': 0,
            'stats': {
                'total': 0,
                'checked': 0,
                'existing': 0,
                'missing': 0
            }
        }
    
    def save_progress(self, progress):
        """進捗状況を保存"""
        with open(self.progress_file, 'w') as f:
            json.dump(progress, f, indent=2)
    
    def extract_urls_from_db(self):
        """データベースからURLを抽出"""
        db_path = Path("data/eventmonitor.db")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, huggingface_urls 
            FROM all_tweets 
            WHERE huggingface_urls IS NOT NULL 
            AND huggingface_urls != '[]'
        """)
        
        all_urls = []
        tweet_url_map = {}
        
        for tweet_id, hf_urls_json in cursor.fetchall():
            try:
                urls = json.loads(hf_urls_json)
                if urls:
                    all_urls.extend(urls)
                    tweet_url_map[tweet_id] = urls
            except json.JSONDecodeError:
                pass
        
        conn.close()
        
        unique_urls = list(set(all_urls))
        return unique_urls, tweet_url_map
    
    def check_empty_media_urls(self):
        """media_urlsが空なのにhuggingface_urlsがあるツイートをチェック"""
        db_path = Path("data/eventmonitor.db")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # huggingface_urlsがあるのにmedia_urlsが空のツイートを検索
        cursor.execute("""
            SELECT id, username, huggingface_urls, created_at
            FROM all_tweets 
            WHERE huggingface_urls IS NOT NULL 
            AND huggingface_urls != '[]'
            AND (media_urls IS NULL OR media_urls = '[]')
        """)
        
        empty_media_tweets = []
        for tweet_id, username, hf_urls_json, created_at in cursor.fetchall():
            try:
                hf_urls = json.loads(hf_urls_json)
                if hf_urls:
                    empty_media_tweets.append({
                        'tweet_id': tweet_id,
                        'username': username,
                        'huggingface_urls': hf_urls,
                        'created_at': created_at
                    })
            except:
                pass
        
        if empty_media_tweets:
            print(f"\n=== media_urlsが空でhuggingface_urlsがあるツイート ===")
            print(f"合計: {len(empty_media_tweets)}件")
            
            # ユーザー別集計
            user_counts = defaultdict(int)
            for tweet in empty_media_tweets:
                user_counts[tweet['username']] += 1
            
            print("\nユーザー別（上位10件）:")
            for username, count in sorted(user_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
                print(f"{username}: {count}件")
        
        conn.close()
        return empty_media_tweets
    
    def check_missing_backups(self):
        """HuggingFace URLが空のツイートをチェック"""
        db_path = Path("data/eventmonitor.db")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # media_urlsがあるのにhuggingface_urlsが空のツイートを検索
        cursor.execute("""
            SELECT COUNT(*) 
            FROM all_tweets 
            WHERE media_urls IS NOT NULL 
            AND media_urls != '[]'
            AND (huggingface_urls IS NULL OR huggingface_urls = '[]')
        """)
        
        total_count = cursor.fetchone()[0]
        
        if total_count > 0:
            # ユーザー別に集計
            cursor.execute("""
                SELECT username, COUNT(*) as count
                FROM all_tweets 
                WHERE media_urls IS NOT NULL 
                AND media_urls != '[]'
                AND (huggingface_urls IS NULL OR huggingface_urls = '[]')
                GROUP BY username
                ORDER BY count DESC
                LIMIT 20
            """)
            
            print("\n=== ユーザー別未バックアップツイート数（上位20件） ===")
            for username, count in cursor.fetchall():
                print(f"{username}: {count}件")
                
            # 最新のサンプルを表示
            cursor.execute("""
                SELECT id, username, created_at, media_urls
                FROM all_tweets 
                WHERE media_urls IS NOT NULL 
                AND media_urls != '[]'
                AND (huggingface_urls IS NULL OR huggingface_urls = '[]')
                ORDER BY created_at DESC
                LIMIT 5
            """)
            
            print("\n=== 最新の未バックアップツイート（5件） ===")
            for tweet_id, username, created_at, media_urls in cursor.fetchall():
                media_count = len(json.loads(media_urls))
                print(f"ID: {tweet_id}, User: {username}, Media: {media_count}件, Date: {created_at}")
        
        conn.close()
        return total_count
    
    def check_url(self, url):
        """URLの存在確認"""
        try:
            response = self.session.head(url, timeout=10, allow_redirects=True)
            return response.status_code in [200, 302, 304]
        except:
            return False
    
    def extract_file_info(self, url):
        """URLからファイル情報を抽出"""
        parsed = urlparse(url)
        path_parts = parsed.path.split('/')
        
        info = {
            'url': url,
            'repo': None,
            'file_type': None,
            'username': None,
            'filename': None,
            'full_path': None
        }
        
        if 'datasets' in path_parts:
            try:
                idx = path_parts.index('datasets')
                info['repo'] = f"{path_parts[idx+1]}/{path_parts[idx+2]}"
                
                main_idx = path_parts.index('main')
                file_path_parts = path_parts[main_idx+1:]
                info['full_path'] = '/'.join(file_path_parts)
                
                if file_path_parts[0] in ['encrypted_images', 'encrypted_videos', 'images', 'videos']:
                    info['file_type'] = file_path_parts[0]
                    if len(file_path_parts) > 1:
                        info['username'] = file_path_parts[1]
                    if len(file_path_parts) > 2:
                        info['filename'] = file_path_parts[2]
            except:
                pass
        
        return info
    
    def check_batch(self, urls, start_index=0):
        """バッチ処理でURL確認"""
        progress = self.load_progress()
        
        # 初回実行時は統計情報を初期化
        if progress['stats']['total'] == 0:
            progress['stats']['total'] = len(urls)
        
        # 開始位置を設定
        if start_index > 0:
            progress['last_index'] = start_index
        
        print(f"総URL数: {len(urls)}")
        print(f"開始位置: {progress['last_index']}")
        print(f"既にチェック済み: {len(progress['checked_urls'])}")
        
        # バッチ処理
        for i in range(progress['last_index'], len(urls), self.batch_size):
            batch = urls[i:i+self.batch_size]
            batch_results = []
            
            print(f"\nバッチ {i//self.batch_size + 1}: {i}-{min(i+self.batch_size, len(urls))}/{len(urls)}")
            
            for url in batch:
                # 既にチェック済みならスキップ
                if url in progress['checked_urls']:
                    continue
                
                exists = self.check_url(url)
                progress['checked_urls'][url] = exists
                
                if exists:
                    progress['stats']['existing'] += 1
                else:
                    info = self.extract_file_info(url)
                    
                    # 誤ったパスかどうか判定（images/やvideos/で始まる場合）
                    if info['file_type'] in ['images', 'videos']:
                        print(f"  誤ったパス: {info['full_path']} (暗号化されていないパス)")
                        if 'wrong_path' not in progress['stats']:
                            progress['stats']['wrong_path'] = 0
                        progress['stats']['wrong_path'] += 1
                    else:
                        print(f"  欠落: {info['full_path']}")
                    
                    progress['stats']['missing'] += 1
                
                progress['stats']['checked'] += 1
                
                # 進捗を定期的に保存
                if progress['stats']['checked'] % 10 == 0:
                    progress['last_index'] = i
                    self.save_progress(progress)
            
            # バッチ間の遅延
            time.sleep(self.delay)
            
            # 進捗状況を表示
            print(f"進捗: {progress['stats']['checked']}/{progress['stats']['total']} "
                  f"(存在: {progress['stats']['existing']}, 欠落: {progress['stats']['missing']})")
        
        # 最終結果を保存
        progress['last_index'] = len(urls)
        self.save_progress(progress)
        
        return progress
    
    def generate_report(self, progress, tweet_url_map, empty_media_tweets):
        """最終レポートを生成"""
        missing_urls = [url for url, exists in progress['checked_urls'].items() if not exists]
        
        # ファイルタイプ別集計
        missing_by_type = defaultdict(list)
        for url in missing_urls:
            info = self.extract_file_info(url)
            missing_by_type[info['file_type']].append(info)
        
        # 影響を受けるツイートを特定
        affected_tweets = set()
        for tweet_id, urls in tweet_url_map.items():
            if any(url in missing_urls for url in urls):
                affected_tweets.add(tweet_id)
        
        # 誤ったパスの統計を含める
        wrong_path_count = progress['stats'].get('wrong_path', 0)
        
        report = {
            'summary': {
                'total_checked': progress['stats']['checked'],
                'existing': progress['stats']['existing'],
                'missing': progress['stats']['missing'],
                'wrong_path': wrong_path_count,
                'affected_tweets': len(affected_tweets),
                'empty_media_urls': len(empty_media_tweets)
            },
            'missing_by_type': {
                file_type: len(infos) for file_type, infos in missing_by_type.items()
            },
            'missing_urls': missing_urls[:100],  # 最初の100件のみ
            'empty_media_tweets': empty_media_tweets[:100]  # 最初の100件のみ
        }
        
        # レポートを保存
        with open(self.results_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        # コンソールに表示
        print("\n=== 最終レポート ===")
        print(f"総チェック数: {report['summary']['total_checked']}")
        print(f"存在: {report['summary']['existing']}")
        print(f"欠落: {report['summary']['missing']}")
        if report['summary']['wrong_path'] > 0:
            print(f"  うち誤ったパス: {report['summary']['wrong_path']} (images/またはvideos/)")
        print(f"影響を受けるツイート数: {report['summary']['affected_tweets']}")
        if report['summary']['empty_media_urls'] > 0:
            print(f"media_urlsが空のツイート: {report['summary']['empty_media_urls']}")
        
        print("\n=== ファイルタイプ別欠落数 ===")
        for file_type, count in report['missing_by_type'].items():
            print(f"{file_type or '不明'}: {count}")
        
        print(f"\n詳細な結果は {self.results_file} に保存されました")
        
        if report['summary']['missing'] > 0 or report['summary']['empty_media_urls'] > 0:
            print("\n欠落ファイルを再処理するには: python scripts/reprocess_missing_files.py")

def main():
    parser = argparse.ArgumentParser(description='HuggingFace URLの存在確認')
    parser.add_argument('--batch-size', type=int, default=50, help='バッチサイズ')
    parser.add_argument('--delay', type=float, default=0.5, help='バッチ間の遅延（秒）')
    parser.add_argument('--resume', action='store_true', help='前回の続きから再開')
    parser.add_argument('--start', type=int, default=0, help='開始インデックス')
    args = parser.parse_args()
    
    checker = HFURLChecker(batch_size=args.batch_size, delay=args.delay)
    
    # 1. 未バックアップツイートをチェック
    print("=== 未バックアップツイートのチェック ===")
    missing_count = checker.check_missing_backups()
    print(f"\n合計 {missing_count} 件のツイートがバックアップされていません")
    if missing_count > 0:
        print("これらを処理するには: python scripts/backup_missing_tweets.py")
    
    # 前回の続きから再開する場合
    if args.resume:
        progress = checker.load_progress()
        if progress['last_index'] > 0:
            print(f"\n前回の続きから再開します（インデックス: {progress['last_index']}）")
    
    # 2. media_urlsが空のツイートをチェック
    print("\n=== media_urlsが空のツイートのチェック ===")
    empty_media_tweets = checker.check_empty_media_urls()
    
    # 3. 既存URLの存在確認
    print("\n=== 既存HuggingFace URLのチェック ===")
    urls, tweet_url_map = checker.extract_urls_from_db()
    
    if not urls:
        print("HuggingFace URLが記録されているツイートがありません")
        return
    
    print(f"{len(urls)}個のユニークなURLを確認します")
    
    # バッチ処理実行
    progress = checker.check_batch(urls, start_index=args.start)
    
    # レポート生成
    checker.generate_report(progress, tweet_url_map, empty_media_tweets)

if __name__ == "__main__":
    main()