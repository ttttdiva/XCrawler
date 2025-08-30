#!/usr/bin/env python3
"""
ダウンロード済みメディアをHuggingFaceに再アップロードするスクリプト

使用方法:
    python scripts/upload_to_huggingface.py [アカウント名]
    
    # 特定アカウントのメディアをアップロード
    python scripts/upload_to_huggingface.py sageen
    
    # 全監視アカウントのメディアをアップロード（アカウント名省略時）
    python scripts/upload_to_huggingface.py
    
    # 暗号化なしでアップロード
    python scripts/upload_to_huggingface.py --no-encrypt
    
    # アップロード後に削除
    python scripts/upload_to_huggingface.py --delete-after
"""

import sys
import os
import logging
import argparse
from pathlib import Path
from typing import Dict, Optional, List
import yaml
import csv
import json
from huggingface_hub import HfApi, upload_folder, create_repo
from dotenv import load_dotenv

# pysqlite3を標準のsqlite3より先にインポート
__import__('pysqlite3')
sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')
import sqlite3

# .envファイルから環境変数を読み込む
load_dotenv()

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("MonitoringHFUploader")


class MonitoringAccountUploader:
    """監視アカウントのメディアをHuggingFaceにアップロード"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """初期化"""
        # 設定ファイル読み込み
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        # HuggingFace API設定
        self.api_key = os.getenv('HUGGINGFACE_API_KEY')
        if not self.api_key:
            logger.error("HUGGINGFACE_API_KEY not found in environment variables")
            sys.exit(1)
        
        self.api = HfApi(token=self.api_key)
        
        # config.yamlから設定を取得
        hf_config = self.config.get('huggingface_backup', {})
        self.repo_name = hf_config.get('repo_name', 'Sageen/EventMonitor_1')
        self.default_encrypt = hf_config.get('rclone_encryption', {}).get('enabled', False)
        
        # リポジトリの存在確認・作成
        self._ensure_repo_exists()
        
        # 進捗ファイルのパス（logsディレクトリ内）
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)  # logsディレクトリがなければ作成
        self.progress_file = logs_dir / "huggingface_upload_progress.json"
        self.progress = self._load_progress()
        
        # 監視アカウントリスト取得
        # monitored_accounts.csvから読み込む
        self.monitoring_accounts = []
        csv_path = "monitored_accounts.csv"
        
        if os.path.exists(csv_path):
            import csv
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # account_typeが空欄またはmonitoringの場合は監視アカウント
                    # logの場合はログ専用アカウント
                    account_type = row.get('account_type', '').strip()
                    if account_type != 'log':  # logでなければ監視アカウント
                        self.monitoring_accounts.append(row['username'])
        
        logger.info(f"Found {len(self.monitoring_accounts)} monitoring accounts")
        logger.info(f"Default encryption setting from config.yaml: {self.default_encrypt}")
    
    def _load_progress(self) -> Dict:
        """進捗ファイルを読み込む"""
        if self.progress_file.exists():
            with open(self.progress_file, 'r') as f:
                return json.load(f)
        return {"completed_accounts": [], "completed_folders": []}
    
    def _save_progress(self):
        """進捗を保存"""
        with open(self.progress_file, 'w') as f:
            json.dump(self.progress, f, indent=2)
    
    def _ensure_repo_exists(self):
        """リポジトリの存在確認・作成"""
        try:
            # リポジトリ情報を取得
            self.api.repo_info(repo_id=self.repo_name, repo_type="dataset")
            logger.info(f"Repository {self.repo_name} already exists")
        except Exception:
            # リポジトリが存在しない場合は作成
            try:
                create_repo(
                    repo_id=self.repo_name,
                    token=self.api_key,
                    repo_type="dataset",
                    private=False
                )
                logger.info(f"Created new repository: {self.repo_name}")
            except Exception as e:
                logger.error(f"Failed to create repository: {e}")
                raise
    
    def _update_database_urls(self, username: str, media_type: str, files: List[Path]):
        """データベースのHuggingFace URLsを更新
        
        Args:
            username: アカウント名
            media_type: 'images' または 'videos'
            files: アップロードされたファイルリスト
        """
        try:
            conn = sqlite3.connect('data/eventmonitor.db')
            cursor = conn.cursor()
            
            updated_count = 0
            for file_path in files:
                # ファイル名からツイートIDを抽出（例: 1928829183066620300_1.jpg -> 1928829183066620300）
                tweet_id = file_path.stem.split('_')[0]
                
                # HuggingFace URLを構築
                hf_url = f"https://huggingface.co/datasets/{self.repo_name}/resolve/main/{media_type}/{username}/{file_path.name}"
                
                # 既存のHuggingFace URLsを取得
                cursor.execute('SELECT huggingface_urls FROM all_tweets WHERE id = ?', (tweet_id,))
                result = cursor.fetchone()
                
                if result:
                    existing_urls = json.loads(result[0]) if result[0] else []
                    
                    # 新しいURLを追加（重複を避ける）
                    if hf_url not in existing_urls:
                        existing_urls.append(hf_url)
                        
                        # データベースを更新
                        cursor.execute('UPDATE all_tweets SET huggingface_urls = ? WHERE id = ?', 
                                     (json.dumps(existing_urls), tweet_id))
                        updated_count += 1
            
            conn.commit()
            conn.close()
            
            if updated_count > 0:
                logger.info(f"Updated HuggingFace URLs for {updated_count} tweets in database")
            
        except Exception as e:
            logger.error(f"Failed to update database: {e}")
            # データベース更新に失敗してもアップロード自体は成功しているので続行
    
    def upload_account_media(self, username: str, encrypt: bool = None, delete_after: bool = False):
        """特定アカウントのメディアをアップロード
        
        Args:
            username: アカウント名
            encrypt: 暗号化するか（Noneの場合はconfig.yamlの設定に従う）
            delete_after: アップロード後に削除するか
        """
        # 既に完了しているアカウントはスキップ
        if username in self.progress.get("completed_accounts", []):
            logger.info(f"Skipping already completed account: {username}")
            return
        
        # encryptがNoneの場合はconfig.yamlの設定を使用
        if encrypt is None:
            encrypt = self.default_encrypt
        logger.info(f"Processing account: {username} (encrypt: {encrypt}, delete_after: {delete_after})")
        
        # メディアフォルダを特定（images/username と videos/username）
        images_path = Path(f"images/{username}")
        videos_path = Path(f"videos/{username}")
        
        if not images_path.exists() and not videos_path.exists():
            logger.warning(f"No media folders found for {username}: images/{username} or videos/{username}")
            return
        
        # HuggingFaceに直接アップロード
        try:
            # imagesフォルダのアップロード
            images_key = f"{username}/images"
            if images_path.exists() and images_key not in self.progress.get("completed_folders", []):
                image_files = list(images_path.glob("*"))
                files_count = len(image_files)
                if files_count > 0:
                    logger.info(f"Uploading {files_count} files from images/{username}")
                    upload_folder(
                        folder_path=str(images_path),
                        repo_id=self.repo_name,
                        repo_type="dataset",
                        path_in_repo=f"images/{username}",
                        token=self.api_key
                    )
                    logger.info(f"Images upload completed for {username}")
                    
                    # データベースのHuggingFace URLsを更新
                    self._update_database_urls(username, 'images', image_files)
                    
                    # 進捗を記録
                    self.progress["completed_folders"].append(images_key)
                    self._save_progress()
                    
                    if delete_after:
                        import shutil
                        shutil.rmtree(images_path)
                        logger.info(f"Deleted {images_path}")
            
            # videosフォルダのアップロード
            videos_key = f"{username}/videos"
            if videos_path.exists() and videos_key not in self.progress.get("completed_folders", []):
                video_files = list(videos_path.glob("*"))
                files_count = len(video_files)
                if files_count > 0:
                    logger.info(f"Uploading {files_count} files from videos/{username}")
                    upload_folder(
                        folder_path=str(videos_path),
                        repo_id=self.repo_name,
                        repo_type="dataset",
                        path_in_repo=f"videos/{username}",
                        token=self.api_key
                    )
                    logger.info(f"Videos upload completed for {username}")
                    
                    # データベースのHuggingFace URLsを更新
                    self._update_database_urls(username, 'videos', video_files)
                    
                    # 進捗を記録
                    self.progress["completed_folders"].append(videos_key)
                    self._save_progress()
                    
                    if delete_after:
                        import shutil
                        shutil.rmtree(videos_path)
                        logger.info(f"Deleted {videos_path}")
            
            # アカウント完了を記録
            self.progress["completed_accounts"].append(username)
            self._save_progress()
            logger.info(f"All uploads completed for {username}")
            
        except Exception as e:
            logger.error(f"Failed to upload media for {username}: {e}")
            raise
    
    def upload_all_accounts(self, encrypt: bool = None, delete_after: bool = False):
        """全監視アカウントのメディアをアップロード"""
        # encryptがNoneの場合はconfig.yamlの設定を使用
        if encrypt is None:
            encrypt = self.default_encrypt
        
        # 未完了のアカウントのみ処理
        completed = set(self.progress.get("completed_accounts", []))
        remaining = [acc for acc in self.monitoring_accounts if acc not in completed]
        
        logger.info(f"Total accounts: {len(self.monitoring_accounts)}, Completed: {len(completed)}, Remaining: {len(remaining)}")
        
        if not remaining:
            logger.info("All accounts already processed")
            return
        
        for i, username in enumerate(remaining, 1):
            logger.info(f"[{i}/{len(remaining)}] Processing: {username}")
            try:
                self.upload_account_media(username, encrypt, delete_after)
            except Exception as e:
                logger.error(f"Failed to process {username}: {e}")
                continue
        
        logger.info("All accounts processed")
        
        # 完了したら進捗ファイルを削除
        if self.progress_file.exists():
            self.progress_file.unlink()
            logger.info("Progress file deleted")
    
    def get_upload_stats(self, username: Optional[str] = None) -> Dict:
        """アップロード統計を取得"""
        stats = {
            'total_files': 0,
            'total_size_mb': 0,
            'by_type': {
                'images': 0,
                'videos': 0
            },
            'by_account': {}
        }
        
        accounts = [username] if username else self.monitoring_accounts
        
        for account in accounts:
            images_path = Path(f"images/{account}")
            videos_path = Path(f"videos/{account}")
            
            if not images_path.exists() and not videos_path.exists():
                continue
            
            account_stats = {
                'files': 0,
                'size_mb': 0,
                'images': 0,
                'videos': 0
            }
            
            # imagesフォルダ内のファイルをカウント
            if images_path.exists():
                for file_path in images_path.iterdir():
                    if file_path.is_file():
                        account_stats['files'] += 1
                        account_stats['size_mb'] += file_path.stat().st_size / (1024 * 1024)
                        account_stats['images'] += 1
            
            # videosフォルダ内のファイルをカウント
            if videos_path.exists():
                for file_path in videos_path.iterdir():
                    if file_path.is_file():
                        account_stats['files'] += 1
                        account_stats['size_mb'] += file_path.stat().st_size / (1024 * 1024)
                        account_stats['videos'] += 1
            
            stats['by_account'][account] = account_stats
            stats['total_files'] += account_stats['files']
            stats['total_size_mb'] += account_stats['size_mb']
            stats['by_type']['images'] += account_stats['images']
            stats['by_type']['videos'] += account_stats['videos']
        
        return stats


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(
        description='監視アカウントのメディアをHuggingFaceにアップロード'
    )
    parser.add_argument(
        'username',
        nargs='?',
        help='アカウント名（省略時は全監視アカウント）'
    )
    parser.add_argument(
        '--no-encrypt',
        action='store_true',
        help='暗号化を無効にする（config.yamlの設定を上書き）'
    )
    parser.add_argument(
        '--encrypt',
        action='store_true',
        help='暗号化を有効にする（config.yamlの設定を上書き）'
    )
    parser.add_argument(
        '--delete-after',
        action='store_true',
        help='アップロード後にローカルファイルを削除'
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='アップロード統計を表示（アップロードは実行しない）'
    )
    parser.add_argument(
        '--reset-progress',
        action='store_true',
        help='進捗をリセットして最初から実行'
    )
    
    args = parser.parse_args()
    
    # 進捗リセット
    if args.reset_progress:
        progress_file = Path("logs") / "huggingface_upload_progress.json"
        if progress_file.exists():
            progress_file.unlink()
            logger.info("Progress reset")
    
    # アップローダー初期化
    uploader = MonitoringAccountUploader()
    
    # 統計表示モード
    if args.stats:
        stats = uploader.get_upload_stats(args.username)
        
        print("\n=== Upload Statistics ===")
        print(f"Total files: {stats['total_files']}")
        print(f"Total size: {stats['total_size_mb']:.2f} MB")
        print(f"Images: {stats['by_type']['images']}")
        print(f"Videos: {stats['by_type']['videos']}")
        
        if stats['by_account']:
            print("\n=== By Account ===")
            for account, account_stats in stats['by_account'].items():
                print(f"\n{account}:")
                print(f"  Files: {account_stats['files']}")
                print(f"  Size: {account_stats['size_mb']:.2f} MB")
                print(f"  Images: {account_stats['images']}")
                print(f"  Videos: {account_stats['videos']}")
        
        return
    
    # 暗号化設定を決定
    # --encrypt と --no-encrypt が両方指定された場合はエラー
    if args.encrypt and args.no_encrypt:
        logger.error("--encrypt and --no-encrypt cannot be used together")
        sys.exit(1)
    
    # 暗号化設定：明示的に指定されていればそれを使用、なければconfig.yamlの設定
    if args.encrypt:
        encrypt = True
    elif args.no_encrypt:
        encrypt = False
    else:
        encrypt = None  # config.yamlの設定を使用
    
    try:
        if args.username:
            # 指定アカウントが監視アカウントか確認
            if args.username not in uploader.monitoring_accounts:
                logger.error(f"{args.username} is not a monitoring account")
                logger.info(f"Available monitoring accounts: {', '.join(uploader.monitoring_accounts)}")
                return
            
            uploader.upload_account_media(
                args.username,
                encrypt=encrypt,
                delete_after=args.delete_after
            )
        else:
            # 全監視アカウント処理
            uploader.upload_all_accounts(
                encrypt=encrypt,
                delete_after=args.delete_after
            )
        
        logger.info("Upload completed successfully")
        
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()