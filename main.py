#!/usr/bin/env python3
import asyncio
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

import yaml
from dotenv import load_dotenv
import csv

# WSL環境でのSQLite互換性のためのパッチ
try:
    import pysqlite3
    sys.modules['sqlite3'] = pysqlite3
except ImportError:
    pass

from src.twitter_monitor import TwitterMonitor
from src.event_detector import EventDetector
from src.database import DatabaseManager
from src.discord_notifier import DiscordNotifier
from src.utils import setup_logging
from src.backup_manager import BackupManager
from src.hydrus_client import HydrusClient
from src.log_only_hf_uploader import LogOnlyHFUploader


class EventMonitor:
    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)
        # ロガーは後で初期化（ログディレクトリが決まってから）
        self.logger = None
        
        # コンポーネントの初期化
        self.db_manager = DatabaseManager(self.config)
        self.twitter_monitor = TwitterMonitor(self.config, self.db_manager)
        self.event_detector = EventDetector(self.config)
        self.discord_notifier = DiscordNotifier(self.config)
        self.backup_manager = BackupManager(self.config, self.db_manager)
        self.hydrus_client = HydrusClient(self.config.get('hydrus', {}))
        self.log_only_uploader = LogOnlyHFUploader(self.config, self.db_manager)
        
    def _load_config(self, config_path: str) -> dict:
        """設定ファイルを読み込む"""
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # CSVファイルから監視対象アカウントを読み込む
        config['monitored_accounts'] = self._load_monitored_accounts_from_csv("monitored_accounts.csv")
        
        return config
    
    def _load_monitored_accounts_from_csv(self, csv_path: str) -> List[Dict[str, str]]:
        """CSVファイルから監視対象アカウントを読み込む"""
        accounts = []
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    accounts.append({
                        'username': row['username'],
                        'display_name': row['display_name'],
                        'event_detection_enabled': row.get('event_detection_enabled', '1') == '1',
                        'account_type': row.get('account_type', '').strip()  # 空欄がデフォルト（通常監視）
                    })
        except FileNotFoundError:
            raise FileNotFoundError(f"監視対象アカウントのCSVファイルが見つかりません: {csv_path}")
        except Exception as e:
            raise Exception(f"CSVファイルの読み込みに失敗しました: {e}")
        
        return accounts
    
    async def run_once(self):
        """一度だけ実行する（手動実行用）"""
        # temp_images_backupディレクトリが残っていたら削除
        import shutil
        temp_images_dir = Path("temp_images_backup")
        if temp_images_dir.exists():
            shutil.rmtree(temp_images_dir)
        
        # imagesディレクトリを作成
        images_dir = Path("images")
        images_dir.mkdir(exist_ok=True)
        
        # dataディレクトリを作成
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)
        
        # ロガーを初期化（dataディレクトリにログを保存）
        if self.logger is None:
            self.logger = setup_logging(self.config['system']['log_level'], data_dir)
        
        self.logger.info("EventMonitor started (single run)")
        
        # HydrusClientをコンテキストマネージャーとして使用
        async with self.hydrus_client:
            try:
                # 0. 未アップロード分を最初に処理
                if self.backup_manager.backup_config.get('enabled', False):
                    try:
                        self.logger.info("Checking for unprocessed media in database...")
                        await self.backup_manager.upload_remaining_media()
                        self.logger.info("Unprocessed media upload completed")
                    except Exception as e:
                        self.logger.error(f"Unprocessed media upload failed: {e}", exc_info=True)
                        # エラーが発生しても新規ツイートの処理は継続
                
                # 監視対象アカウントのループ
                for account in self.config['monitored_accounts']:
                    username = account['username']
                    display_name = account.get('display_name', username)
                    account_type = account.get('account_type', '')
                    
                    # account_typeの表示（空欄の場合は"監視"、"log"の場合は"ログ専用"）
                    type_display = "ログ専用" if account_type == 'log' else "監視"
                    self.logger.info(f"Checking account: {display_name} (@{username}) - Type: {type_display}")
                    
                    # ツイートを取得
                    tweets = await self.twitter_monitor.get_user_tweets(
                        username,
                        days_lookback=self.config['tweet_settings']['days_lookback'],
                        force_full_fetch=self.config['tweet_settings'].get('force_full_fetch', False)
                    )
                    
                    if not tweets:
                        self.logger.info(f"No tweets found for @{username}")
                        continue
                    
                    # ツイート情報とusernameをtweet_dataに追加
                    for tweet in tweets:
                        tweet['username'] = username
                        tweet['display_name'] = display_name
                    
                    # account_typeに基づいて処理を分岐
                    if account_type == 'log':
                        # ログ専用アカウントの処理
                        await self._process_log_only_account(tweets, username, display_name)
                        continue
                    
                    # 通常アカウントの処理（簡略化版）
                    # 1. 新規ツイートのフィルタリング
                    force_full_fetch = self.config['tweet_settings'].get('force_full_fetch', False)
                    if force_full_fetch:
                        # force_full_fetchの場合は全てのツイートを処理（重複チェックしない）
                        new_tweets = tweets
                        self.logger.warning(f"Force full fetch enabled - processing ALL {len(tweets)} tweets without duplicate check")
                    else:
                        new_tweets = self.db_manager.filter_new_tweets(tweets, username)
                    
                    if not new_tweets:
                        self.logger.info(f"No new tweets for @{username} (all already in DB)")
                        continue
                    
                    self.logger.info(f"Found {len(new_tweets)} new tweets for @{username}")
                    
                    # 2. メディアのダウンロード
                    self.logger.info(f"Downloading media for @{username}...")
                    for tweet in new_tweets:
                        all_media_paths = []
                        
                        if tweet.get('media'):
                            downloaded_paths = await self.twitter_monitor.download_tweet_images(tweet)
                            all_media_paths.extend(downloaded_paths)
                            self.logger.info(f"Downloaded {len(downloaded_paths)} images for tweet {tweet['id']}")
                        
                        if tweet.get('videos'):
                            downloaded_video_paths = await self.twitter_monitor.download_tweet_videos(tweet)
                            all_media_paths.extend(downloaded_video_paths)
                            self.logger.info(f"Downloaded {len(downloaded_video_paths)} videos for tweet {tweet['id']}")
                        
                        # local_mediaに画像と動画の両方を格納
                        tweet['local_media'] = all_media_paths
                    
                    # 3. データベースに保存（huggingface_urls=[]で初期化）
                    all_saved = self.db_manager.save_all_tweets(new_tweets, username)
                    self.logger.info(f"Saved {all_saved} tweets to all_tweets table for @{username}")
                    
                    # 4. Hydrus連携（event_tweets_onlyがfalseの場合、全ツイートを対象）
                    self.logger.info(f"Hydrus enabled: {self.hydrus_client.enabled}")
                    self.logger.info(f"event_tweets_only: {self.hydrus_client.import_settings.get('event_tweets_only', True)}")
                    if self.hydrus_client.enabled and not self.hydrus_client.import_settings.get('event_tweets_only', True):
                        self.logger.info(f"Processing {len(new_tweets)} tweets for Hydrus import")
                        for tweet in new_tweets:
                            self.logger.info(f"Tweet {tweet['id']} has local_media: {bool(tweet.get('local_media'))}")
                            if tweet.get('local_media'):
                                try:
                                    self.logger.info(f"Calling import_tweet_images for tweet {tweet['id']}")
                                    imported = await self.hydrus_client.import_tweet_images(
                                        tweet,
                                        tweet['local_media']
                                    )
                                    self.logger.info(f"import_tweet_images returned: {imported}")
                                    if imported:
                                        self.logger.info(f"Imported {len(imported)} images to Hydrus for tweet {tweet['id']}")
                                except Exception as e:
                                    self.logger.error(f"Failed to import to Hydrus: {e}")
                    
                    # 5. イベント検知が有効な場合のみLLMで判定
                    # config.yamlのevent_detection.enabledとアカウント個別の設定の両方をチェック
                    event_detection_enabled = (
                        self.config['event_detection'].get('enabled', True) and
                        account.get('event_detection_enabled', True)
                    )
                    
                    if event_detection_enabled:
                        # 新規ツイートのみLLMで判定
                        event_tweets = await self.event_detector.detect_event_tweets(new_tweets)
                        
                        if not event_tweets:
                            self.logger.info(f"No event-related tweets found for @{username}")
                            continue
                        
                        # イベント関連ツイートをevent_tweetsテーブルに保存
                        self.db_manager.save_event_tweets(event_tweets, username)
                        
                        # Discord通知とHydrus連携
                        for tweet in event_tweets:
                            await self.discord_notifier.send_notification(
                                tweet, 
                                username, 
                                display_name
                            )
                            
                            # Hydrus連携（イベントツイートの場合、event_tweets_onlyの設定に関わらず処理）
                            if self.hydrus_client.enabled and tweet.get('local_media'):
                                # event_tweets_onlyがtrueの場合、またはすでに処理済みでない場合のみ
                                if self.hydrus_client.import_settings.get('event_tweets_only', True):
                                    try:
                                        imported = await self.hydrus_client.import_tweet_images(
                                            tweet,
                                            tweet['local_media']
                                        )
                                        if imported:
                                            self.logger.info(f"Imported {len(imported)} images to Hydrus for tweet {tweet['id']}")
                                    except Exception as e:
                                        self.logger.error(f"Failed to import to Hydrus: {e}")
                        
                        self.logger.info(f"Processed {len(event_tweets)} new event tweets for @{username}")
                    else:
                        if not self.config['event_detection'].get('enabled', True):
                            self.logger.info("Event detection is globally disabled (crawler mode only)")
                        else:
                            self.logger.info(f"Event detection is disabled for @{username}, skipping LLM analysis")
                    
                    # 6. HuggingFaceバックアップ処理（全ての重要な処理が完了した後）
                    if self.backup_manager.backup_config.get('enabled', False):
                        try:
                            self.logger.info(f"Starting HuggingFace backup for {len(new_tweets)} tweets from @{username}")
                            await self.backup_manager.backup_tweets(new_tweets)
                            self.logger.info(f"HuggingFace backup completed for @{username}")
                        except Exception as e:
                            self.logger.error(f"HuggingFace backup failed for @{username}: {e}", exc_info=True)
                            # 失敗してもプロセスは継続
                    
            except Exception as e:
                self.logger.error(f"Error in run_once: {e}", exc_info=True)
                raise
            finally:
                # TwitterMonitorのクリーンアップ
                await self.twitter_monitor.cleanup()
            
            # 7. 全アカウント処理後、データベースファイルをバックアップ
            if self.backup_manager.backup_config.get('enabled', False):
                try:
                    self.logger.info("Uploading database backup...")
                    await self.backup_manager.upload_database_backup()
                    self.logger.info("Database backup completed")
                except Exception as e:
                    self.logger.error(f"Database backup failed: {e}")
                
            # 古い画像のクリーンアップ
            await self._cleanup_old_images()
    
    async def _cleanup_old_images(self):
        """古い画像ファイルを削除"""
        try:
            # クリーンアップが無効な場合はスキップ
            if not self.config.get('image_settings', {}).get('cleanup_enabled', True):
                self.logger.debug("Image cleanup is disabled")
                return
            
            retention_days = self.config.get('image_settings', {}).get('retention_days', 30)
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            
            images_dir = Path("images")
            if not images_dir.exists():
                return
            
            deleted_count = 0
            for user_dir in images_dir.iterdir():
                if user_dir.is_dir():
                    for image_file in user_dir.glob("*.jpg"):
                        # ファイルの更新時刻を確認
                        if datetime.fromtimestamp(image_file.stat().st_mtime) < cutoff_date:
                            image_file.unlink()
                            deleted_count += 1
            
            if deleted_count > 0:
                self.logger.info(f"Cleaned up {deleted_count} old images (older than {retention_days} days)")
        except Exception as e:
            self.logger.error(f"Error during image cleanup: {e}")
    
    async def run_continuous(self):
        """継続的に実行する（デーモンモード）"""
        # dataディレクトリを作成
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)
        
        # 初回実行時のロガー初期化
        if self.logger is None:
            self.logger = setup_logging(self.config['system']['log_level'], data_dir)
        
        self.logger.info("EventMonitor started (continuous mode)")
        
        interval = self.config['system']['check_interval'] * 60  # 分を秒に変換
        
        while True:
            try:
                await self.run_once()
                self.logger.info(f"Waiting {self.config['system']['check_interval']} minutes until next check...")
                await asyncio.sleep(interval)
            except KeyboardInterrupt:
                self.logger.info("EventMonitor stopped by user")
                break
            except Exception as e:
                self.logger.error(f"Error in continuous run: {e}", exc_info=True)
                # エラーが発生しても継続
                await asyncio.sleep(interval)
    
    async def _process_log_only_account(self, tweets: List[Dict[str, Any]], username: str, display_name: str):
        """ログ専用アカウントの処理（シンプルで高速）"""
        self.logger.info(f"Processing log-only account: @{username}")
        
        # 新規ツイートをフィルタリング
        new_tweets = self.db_manager.filter_log_only_tweets(tweets, username)
        
        if not new_tweets:
            self.logger.info(f"No new tweets for log-only account @{username}")
            return
        
        self.logger.info(f"Found {len(new_tweets)} new tweets for log-only account @{username}")
        
        # log_only_tweetsテーブルに保存
        saved_count = self.db_manager.save_log_only_tweets(new_tweets, username)
        self.logger.info(f"Saved {saved_count} tweets to log_only_tweets table for @{username}")
        
        # LogOnlyHFUploaderで画像処理（ダウンロード→HFアップロード→即削除）
        if self.log_only_uploader.enabled:
            try:
                await self.log_only_uploader.process_tweets(new_tweets, username)
                self.logger.info(f"Log-only upload completed for @{username}")
            except Exception as e:
                self.logger.error(f"Log-only upload failed for @{username}: {e}")
                # 失敗してもプロセスは継続
        
        # 注意: 以下の処理は全てスキップ
        # - イベント検知（LLM処理）
        # - event_tweetsテーブルへの保存
        # - Discord通知
        # - Hydrus Clientへのインポート
        # - 画像の永続保存


async def main():
    # .envファイルを読み込む
    load_dotenv()
    
    # コマンドライン引数をチェック
    if len(sys.argv) > 1 and sys.argv[1] == "--daemon":
        monitor = EventMonitor()
        await monitor.run_continuous()
    else:
        monitor = EventMonitor()
        await monitor.run_once()


if __name__ == "__main__":
    import sys
    import gc
    
    # Python 3.11以降の非同期ジェネレーター警告を抑制
    if sys.version_info >= (3, 11):
        import warnings
        warnings.filterwarnings("ignore", category=RuntimeWarning, 
                              message=".*asynchronous generator.*")
    
    try:
        # asyncio.run()を使用（Python 3.7+）
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped by user")
    finally:
        # すべてのtempディレクトリをクリーンアップ
        import shutil
        temp_dirs_to_cleanup = [
            "temp_images_backup",
            ".rclone_temp",
            "temp_upload",
            "test_upload_temp",
            "eventmonitor_encrypted_files"
        ]
        
        for temp_dir in temp_dirs_to_cleanup:
            temp_path = Path(temp_dir)
            if temp_path.exists():
                try:
                    shutil.rmtree(temp_path)
                    print(f"Cleaned up temp directory: {temp_dir}")
                except Exception as e:
                    print(f"Failed to clean up {temp_dir}: {e}")
        
        # temp_uploadで始まるディレクトリも削除
        try:
            for temp_path in Path(".").glob("temp_upload_*"):
                if temp_path.is_dir():
                    shutil.rmtree(temp_path)
                    print(f"Cleaned up temp directory: {temp_path}")
        except Exception as e:
            print(f"Failed to clean up temp_upload_* directories: {e}")
        
        # ガベージコレクションを強制実行
        gc.collect()
        
        # 少し待機
        import time
        time.sleep(0.5)