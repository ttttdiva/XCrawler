import sys
# pysqlite3を標準のsqlite3より先にインポート
__import__('pysqlite3')
sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')

import logging
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
from huggingface_hub import HfApi, upload_file, create_repo
import pandas as pd
import re
import yaml
import time
import random
from .rclone_client import RcloneClient, RcloneConfig


class BackupManager:
    """Hugging Faceへのバックアップ管理クラス"""
    
    def __init__(self, config: dict, db_manager=None):
        self.config = config
        self.logger = logging.getLogger("EventMonitor.Backup")
        self.backup_config = config.get('huggingface_backup', {})
        self.db_manager = db_manager
        
        # バックアップが有効かチェック
        if not self.backup_config.get('enabled', False):
            self.logger.info("Hugging Face backup is disabled")
            return
            
        # HfApiの初期化
        try:
            import os
            token = os.getenv('HUGGINGFACE_API_KEY')
            if not token:
                self.logger.warning("HUGGINGFACE_API_KEY not found, backup disabled")
                self.backup_config['enabled'] = False
                return
                
            self.api = HfApi(token=token)
            self.repo_name = self.backup_config.get('repo_name', 'event-monitor-tweets')
            
            # ユーザー情報を取得
            user_info = self.api.whoami()
            self.username = user_info['name']
            
            # リポジトリ名にユーザー名を含める
            if '/' not in self.repo_name:
                self.full_repo_name = f"{self.username}/{self.repo_name}"
            else:
                self.full_repo_name = self.repo_name
                
            # 元のリポジトリ名を保存（番号なしのベース名）
            self.base_repo_name = self._extract_base_repo_name(self.full_repo_name)
                
            self.logger.info(f"Initialized Hugging Face backup for {self.full_repo_name} (as dataset repository)")
            
            # rclone暗号化の初期化
            self.rclone_client = None
            if self.backup_config.get('rclone_encryption', {}).get('enabled', False):
                try:
                    rclone_config = RcloneConfig(
                        remote_name=self.backup_config['rclone_encryption']['remote_name'],
                        config_path=self.backup_config['rclone_encryption'].get('config_path')
                    )
                    self.rclone_client = RcloneClient(rclone_config)
                    self.logger.info("Initialized rclone encryption for image uploads")
                except Exception as e:
                    self.logger.warning(f"Failed to initialize rclone encryption: {e}")
                    self.logger.warning("Images will be uploaded without encryption")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Hugging Face backup: {e}")
            self.backup_config['enabled'] = False
    
    def _extract_base_repo_name(self, repo_name: str) -> str:
        """リポジトリ名から番号を除いたベース名を抽出"""
        # 例: "Sageen/EventMonitor_1" → "Sageen/EventMonitor"
        match = re.match(r'^(.+?)(_\d+)?$', repo_name)
        if match:
            return match.group(1)
        return repo_name
    
    def _get_next_repo_name(self) -> str:
        """現在のリポジトリ名から次の番号のリポジトリ名を生成"""
        match = re.match(r'^(.+?)(?:_(\d+))?$', self.full_repo_name)
        if match:
            base_name = match.group(1)
            current_num = int(match.group(2)) if match.group(2) else 1
            return f"{base_name}_{current_num + 1}"
        return f"{self.full_repo_name}_2"
    
    def _update_config_file(self, new_repo_name: str):
        """config.yamlファイルを新しいリポジトリ名で更新"""
        try:
            config_path = Path('config.yaml')
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            # リポジトリ名を更新
            config['huggingface_backup']['repo_name'] = new_repo_name
            
            # ファイルに書き戻す
            with open(config_path, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
            
            self.logger.info(f"Updated config.yaml with new repository: {new_repo_name}")
        except Exception as e:
            self.logger.error(f"Failed to update config.yaml: {e}")
    
    def _handle_upload_error(self, error: Exception) -> bool:
        """アップロードエラーを処理し、必要に応じて新しいリポジトリに切り替え
        
        Returns:
            bool: リトライすべきかどうか
        """
        error_msg = str(error)
        
        # レート制限エラーをチェック (429 Too Many Requests)
        if "429" in error_msg and "Too Many Requests" in error_msg:
            self.logger.warning(f"Rate limit error: {error_msg}")
            
            # エラーメッセージから待機時間を抽出
            wait_time = 3600  # デフォルト1時間
            
            # パターン1: "retry this action in about X hour/minute"
            match = re.search(r'retry this action in about (\d+) (hour|minute)', error_msg)
            if match:
                time_value = int(match.group(1))
                time_unit = match.group(2)
                if time_unit == "hour":
                    wait_time = time_value * 3600
                elif time_unit == "minute":
                    wait_time = time_value * 60
            
            # パターン2: "you can retry this action in X minutes"
            else:
                match = re.search(r'you can retry this action in (\d+) (minutes?|hours?)', error_msg)
                if match:
                    time_value = int(match.group(1))
                    time_unit = match.group(2)
                    if time_unit.startswith("hour"):
                        wait_time = time_value * 3600
                    elif time_unit.startswith("minute"):
                        wait_time = time_value * 60
            
            # パターン3: "X minutes" や "X hours" だけのシンプルなパターン
            if wait_time == 3600:  # まだデフォルト値の場合
                match = re.search(r'(\d+)\s+(minutes?|hours?)', error_msg)
                if match:
                    time_value = int(match.group(1))
                    time_unit = match.group(2)
                    if time_unit.startswith("hour"):
                        wait_time = time_value * 3600
                    elif time_unit.startswith("minute"):
                        wait_time = time_value * 60
            
            # レート制限待機をスキップするオプション
            skip_rate_limit_wait = self.backup_config.get('skip_rate_limit_wait', False)
            if skip_rate_limit_wait:
                self.logger.warning(f"Rate limit detected (wait time: {wait_time}s), but skipping wait as configured")
                return False  # リトライしない
            
            # 1秒のジッタを追加（レート制限の境界で確実に制限期間を超えるため）
            total_wait = wait_time + 1
            
            self.logger.info(f"Waiting {total_wait} seconds before retrying due to rate limit...")
            time.sleep(total_wait)
            return True  # リトライする
        
        # ファイル数上限エラーをチェック
        if "over the limit of 100000 files" in error_msg:
            self.logger.warning(f"Repository {self.full_repo_name} has reached file limit")
            
            # 次のリポジトリ名を生成
            new_repo_name = self._get_next_repo_name()
            self.logger.info(f"Switching to new repository: {new_repo_name}")
            
            # config.yamlを更新
            self._update_config_file(new_repo_name)
            
            # 新しいリポジトリ名を設定
            self.full_repo_name = new_repo_name
            
            # 新しいリポジトリを作成
            try:
                create_repo(
                    self.full_repo_name,
                    token=self.api.token,
                    repo_type="dataset"
                )
                self.logger.info(f"Created new dataset repository: {self.full_repo_name}")
                
                # 少し待機
                time.sleep(2)
                
                return True  # リトライする
            except Exception as create_error:
                self.logger.error(f"Failed to create new repository: {create_error}")
                return False
        
        return False  # その他のエラーはリトライしない
    
    def _handle_file_limit_error(self) -> bool:
        """ファイル数上限エラーを処理し、新しいリポジトリに切り替え
        
        Returns:
            bool: 成功したかどうか
        """
        self.logger.warning(f"Repository {self.full_repo_name} has reached file limit")
        
        # 次のリポジトリ名を生成
        new_repo_name = self._get_next_repo_name()
        self.logger.info(f"Switching to new repository: {new_repo_name}")
        
        # config.yamlを更新
        self._update_config_file(new_repo_name)
        
        # 新しいリポジトリ名を設定
        self.full_repo_name = new_repo_name
        
        # 新しいリポジトリを作成
        try:
            create_repo(
                self.full_repo_name,
                token=self.api.token,
                repo_type="dataset"
            )
            self.logger.info(f"Created new dataset repository: {self.full_repo_name}")
            
            # 少し待機
            time.sleep(2)
            
            return True
        except Exception as create_error:
            self.logger.error(f"Failed to create new repository: {create_error}")
            return False
    
    def _upload_with_retry(self, **kwargs):
        """ファイル数上限エラーとレート制限エラーに対応したアップロード処理"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                upload_file(**kwargs)
                return  # 成功したら終了
            except Exception as e:
                retry_count += 1
                if self._handle_upload_error(e):
                    # リポジトリを切り替えた場合は新しいリポジトリ名を使用
                    kwargs['repo_id'] = self.full_repo_name
                    # レート制限の場合は既に待機しているのでリトライ
                    if retry_count >= max_retries:
                        self.logger.error(f"Failed after {max_retries} retries")
                        raise
                else:
                    # リトライ不要なエラーの場合はそのまま例外を投げる
                    raise
    
    def _ensure_repo_exists(self):
        """リポジトリが存在することを確認、必要に応じて作成"""
        try:
            # リポジトリの存在確認
            try:
                self.api.repo_info(self.full_repo_name, repo_type="dataset")
                self.logger.info(f"Repository {self.full_repo_name} already exists")
            except Exception as e:
                # リポジトリが存在しない場合は作成
                self.logger.info(f"Repository not found, creating new dataset repository: {self.full_repo_name}")
                try:
                    create_repo(
                        self.full_repo_name,
                        token=self.api.token,
                        repo_type="dataset"
                    )
                    self.logger.info(f"Created dataset repository {self.full_repo_name}")
                    # リポジトリが完全に作成されるまで少し待機
                    time.sleep(2)
                except Exception as create_error:
                    self.logger.error(f"Failed to create repository: {create_error}")
                    raise
        except Exception as e:
            self.logger.error(f"Failed to ensure repository exists: {e}")
            raise
    
    async def backup_tweet_and_save(self, tweet: Dict[str, Any], username: str, is_log_only: bool = False) -> bool:
        """単一ツイートのメディアをHugging Faceにバックアップし、成功したらDBに保存
        
        Args:
            tweet: バックアップ対象のツイート
            username: ユーザー名
            is_log_only: Trueの場合log_only_tweetsテーブル、Falseの場合all_tweetsテーブル
            
        Returns:
            bool: 処理成功の場合True
        """
        if not self.backup_config.get('enabled', False):
            # バックアップ無効の場合はDBに保存だけして成功とする
            if self.db_manager:
                if is_log_only:
                    return self.db_manager.save_single_log_only_tweet(tweet, username)
                else:
                    return self.db_manager.save_single_tweet(tweet, username)
            return True
            
        try:
            tweet_id = tweet.get('id')
            hf_urls = []
            
            # メディアのアップロード（画像・動画）
            if tweet.get('local_media'):
                for media_path in tweet['local_media']:
                    media_file = Path(media_path)
                    if media_file.exists():
                        # パスからメディアタイプを判定（images/ or videos/）
                        if 'videos/' in str(media_file):
                            media_type = 'video'
                        else:
                            media_type = 'image'  # images/またはその他
                        
                        hf_url = await self._upload_file_with_retry(media_file, media_type)
                        if hf_url:
                            hf_urls.append(hf_url)
                        else:
                            # 1つでもアップロード失敗したら全体を失敗とする
                            self.logger.error(f"Failed to upload media for tweet {tweet_id}")
                            return False
            
            # HF URLsをツイートデータに追加
            tweet['huggingface_urls'] = hf_urls
            
            # DBに保存
            if self.db_manager:
                if is_log_only:
                    saved = self.db_manager.save_single_log_only_tweet(tweet, username)
                else:
                    saved = self.db_manager.save_single_tweet(tweet, username)
                
                if not saved:
                    self.logger.error(f"Failed to save tweet {tweet_id} to database")
                    return False
                    
                # DB保存成功後にHF URLsを更新
                if hf_urls:
                    self._update_tweet_hf_urls_batch(tweet_id, hf_urls, is_log_only=is_log_only)
            
            return True
                
        except Exception as e:
            self.logger.error(f"Backup failed for tweet {tweet.get('id')}: {e}")
            return False
    
    async def backup_tweets(self, new_tweets: List[Dict[str, Any]], is_log_only: bool = False):
        """新規ツイートのメディアをHugging Faceにバックアップ
        
        Args:
            new_tweets: バックアップ対象のツイート
            is_log_only: Trueの場合log_only_tweetsテーブル、Falseの場合all_tweetsテーブル
        """
        if not self.backup_config.get('enabled', False):
            return
            
        try:
            # リポジトリの存在確認・作成
            self._ensure_repo_exists()
            
            # 新規ツイートのメディアをアップロード
            uploaded_count = 0
            failed_count = 0
            
            for tweet in new_tweets:
                tweet_id = tweet.get('id')
                hf_urls = []
                
                # メディアのアップロード（画像・動画）
                if tweet.get('local_media'):
                    for media_path in tweet['local_media']:
                        media_file = Path(media_path)
                        if media_file.exists():
                            # パスからメディアタイプを判定（images/ or videos/）
                            if 'videos/' in str(media_file):
                                media_type = 'video'
                            else:
                                media_type = 'image'  # images/またはその他
                            
                            hf_url = await self._upload_file_with_retry(media_file, media_type)
                            if hf_url:
                                hf_urls.append(hf_url)
                                uploaded_count += 1
                            else:
                                failed_count += 1
                
                # データベースのHuggingFace URLsを更新（is_log_onlyパラメータを渡す）
                if hf_urls and tweet_id:
                    self._update_tweet_hf_urls_batch(tweet_id, hf_urls, is_log_only=is_log_only)
            
            self.logger.info(f"Uploaded {uploaded_count} media files for {len(new_tweets)} tweets ({failed_count} failed)")
                
        except Exception as e:
            self.logger.error(f"Backup failed: {e}")
    
    async def upload_database_backup(self):
        """データベースファイルをバックアップ（最後に1回実行）"""
        try:
            await self._upload_database_file()
            await self._upload_database_as_parquet()
        except Exception as e:
            self.logger.error(f"Database backup failed: {e}")
            raise
    
    async def _upload_database_file(self):
        """SQLiteデータベースファイルをアップロード"""
        try:
            db_path = Path("data/eventmonitor.db")
            if not db_path.exists():
                self.logger.warning("Database file not found")
                return
            
            # 一時ファイルにコピー
            temp_db_file = Path("temp_eventmonitor.db")
            import shutil
            shutil.copy2(db_path, temp_db_file)
            
            # Hugging Faceにアップロード
            upload_file(
                path_or_fileobj=str(temp_db_file),
                path_in_repo="data/eventmonitor.db",
                repo_id=self.full_repo_name,
                token=self.api.token,
                repo_type="dataset"
            )
            
            # 一時ファイルを削除
            temp_db_file.unlink()
            
            self.logger.info("Uploaded database file")
            
        except Exception as e:
            self.logger.error(f"Failed to upload database file: {e}")
            raise
    
    async def _upload_database_as_parquet(self):
        """データベース全体をParquet形式でアップロード"""
        all_tweets_file = None
        try:
            import sqlite3
            
            db_path = Path("data/eventmonitor.db")
            if not db_path.exists():
                self.logger.warning("Database file not found")
                return
            
            # SQLiteデータベースからall_tweetsをParquetに変換
            conn = sqlite3.connect(db_path)
            
            # all_tweetsテーブルを読み込み
            all_tweets_df = pd.read_sql_query("SELECT * FROM all_tweets", conn)
            
            conn.close()
            
            # Parquetファイルとして保存
            all_tweets_file = Path("temp_all_tweets.parquet")
            all_tweets_df.to_parquet(all_tweets_file, engine='pyarrow', compression='snappy')
            
            # all_tweets.parquetをアップロード（ルート直下）
            upload_file(
                path_or_fileobj=str(all_tweets_file),
                path_in_repo="all_tweets.parquet",
                repo_id=self.full_repo_name,
                token=self.api.token,
                repo_type="dataset"
            )
            
            self.logger.info(f"Uploaded parquet backup: {len(all_tweets_df)} total tweets")
            
        except Exception as e:
            self.logger.error(f"Failed to upload database backup: {e}")
        finally:
            # 一時ファイルを確実に削除
            if all_tweets_file and all_tweets_file.exists():
                try:
                    all_tweets_file.unlink()
                    self.logger.debug("Cleaned up temp parquet file")
                except Exception as e:
                    self.logger.warning(f"Failed to delete temp file: {e}")
    
    async def upload_remaining_media(self):
        """DBに保存済みだがHuggingFaceに未アップロードのメディアをアップロード"""
        try:
            self.logger.info("Starting upload of remaining media to HuggingFace...")
            
            # データベースから未アップロード分を取得
            if self.db_manager:
                await self._upload_remaining_from_db()
            
            # リポジトリの存在確認
            self._ensure_repo_exists()
            
            # 画像・動画のアップロード処理
            await self._upload_images()
            
            self.logger.info("Completed upload of remaining media")
            
        except Exception as e:
            self.logger.error(f"Failed to upload remaining media: {e}")
            raise
    
    async def _upload_remaining_from_db(self):
        """データベースから未アップロード分を取得して処理"""
        try:
            from .database import AllTweets
            import json
            import asyncio
            
            session = self.db_manager._get_session()
            
            # all_tweetsテーブルで media_urls があるが huggingface_urls がない行を取得
            all_tweets_query = session.query(AllTweets).filter(
                AllTweets.media_urls.isnot(None),
                AllTweets.media_urls != '',
                AllTweets.media_urls != '[]'
            ).filter(
                (AllTweets.huggingface_urls.is_(None)) | 
                (AllTweets.huggingface_urls == '') | 
                (AllTweets.huggingface_urls == '[]')
            )
            
            unprocessed_all_tweets = all_tweets_query.all()
            
            all_count = len(unprocessed_all_tweets)
            
            if all_count == 0:
                self.logger.info("No unprocessed media found in database")
                session.close()
                return
            
            self.logger.info(f"Found {all_count} all_tweets with unprocessed media")
            
            processed_count = 0
            
            # all_tweetsの処理
            for tweet in unprocessed_all_tweets:
                try:
                    media_urls = json.loads(tweet.media_urls) if tweet.media_urls else []
                    if not media_urls:
                        continue
                    
                    # tweet_dataとして再構築
                    tweet_data = {
                        'id': tweet.id,
                        'username': tweet.username,
                        'display_name': tweet.display_name,
                        'text': tweet.tweet_text,
                        'date': tweet.tweet_date.isoformat(),
                        'url': tweet.tweet_url,
                        'media': media_urls,
                        'local_media': json.loads(tweet.local_media) if tweet.local_media else []
                    }
                    
                    # バックアップ処理を実行
                    await self.backup_tweets([tweet_data])
                    processed_count += 1
                    
                    if processed_count % 10 == 0:
                        self.logger.info(f"Processed {processed_count}/{all_count} tweets")
                        
                    # レート制限対策
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    self.logger.error(f"Failed to process all_tweet {tweet.id}: {e}")
                    continue
            
            session.close()
            self.logger.info(f"Completed processing {processed_count} tweets from database")
            
        except Exception as e:
            self.logger.error(f"Failed to upload remaining from DB: {e}")
            raise
    
    async def _upload_images(self):
        """画像と動画をアップロード（rclone暗号化対応）"""
        try:
            # 画像のアップロード
            images_dir = Path("images")
            if images_dir.exists():
                # rclone暗号化が有効な場合
                if self.rclone_client:
                    await self._upload_encrypted_images(images_dir)
                else:
                    # 通常のアップロード
                    await self._upload_plain_images(images_dir)
            
            # 動画のアップロード
            videos_dir = Path("videos")
            if videos_dir.exists():
                # rclone暗号化が有効な場合
                if self.rclone_client:
                    await self._upload_encrypted_videos(videos_dir)
                else:
                    # 通常のアップロード
                    await self._upload_plain_videos(videos_dir)
                            
        except Exception as e:
            self.logger.error(f"Failed to upload images/videos: {e}")
    
    async def _get_existing_files(self) -> set:
        """既存のファイルリストを取得（暗号化なし）"""
        try:
            from huggingface_hub import list_repo_tree
            tree = list_repo_tree(
                repo_id=self.full_repo_name,
                repo_type="dataset",
                token=self.api.token
            )
            existing_files = {item.path for item in tree if item.path.startswith("images/")}
            self.logger.info(f"Found {len(existing_files)} existing files in repository")
            return existing_files
        except Exception as e:
            self.logger.debug(f"Error getting existing files: {e}")
            return set()
    
    async def _get_existing_mapping(self) -> Dict[str, str]:
        """既存の暗号化マッピングを取得"""
        try:
            from huggingface_hub import hf_hub_download
            local_path = hf_hub_download(
                repo_id=self.full_repo_name,
                filename="encrypted_images/filename_mapping.json",
                token=self.api.token,
                repo_type="dataset"
            )
            with open(local_path, 'r', encoding='utf-8') as f:
                existing_mapping = json.load(f)
            self.logger.info(f"Loaded existing mapping with {len(existing_mapping)} files")
            return existing_mapping
        except Exception as e:
            self.logger.warning(f"No existing mapping found or error loading: {e}")
            return {}
    
    async def _save_encryption_mapping(self, original_files: Dict[Path, Path], encrypted_files: Dict[Path, Path]):
        """暗号化ファイル名のマッピングを保存"""
        try:
            # マッピングを作成
            mapping = {}
            for original_path, relative_path in original_files.items():
                if original_path in encrypted_files:
                    encrypted_path = encrypted_files[original_path]
                    mapping[str(relative_path)] = encrypted_path.name
            
            # 既存のマッピングを取得
            existing_mapping = {}
            try:
                from huggingface_hub import hf_hub_download
                local_path = hf_hub_download(
                    repo_id=self.full_repo_name,
                    filename="encrypted_images/filename_mapping.json",
                    token=self.api.token,
                    repo_type="dataset"
                )
                with open(local_path, 'r', encoding='utf-8') as f:
                    existing_mapping = json.load(f)
            except:
                pass
            
            # マッピングを更新
            existing_mapping.update(mapping)
            
            # 一時ファイルに保存
            temp_file = Path("temp_filename_mapping.json")
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(existing_mapping, f, ensure_ascii=False, indent=2)
            
            # Hugging Faceにアップロード
            self._upload_with_retry(
                path_or_fileobj=str(temp_file),
                path_in_repo="encrypted_images/filename_mapping.json",
                repo_id=self.full_repo_name,
                token=self.api.token,
                repo_type="dataset"
            )
            
            # 一時ファイルを削除
            temp_file.unlink()
            
            self.logger.info(f"Saved encryption mapping for {len(mapping)} files")
            
        except Exception as e:
            self.logger.error(f"Failed to save encryption mapping: {e}")
    
    async def _update_tweet_hf_urls(self, image_file: Path, hf_path: str):
        """画像ファイルのツイートIDからHugging Face URLを更新"""
        try:
            # ファイル名からツイートIDを抽出
            tweet_id = image_file.stem.split('_')[0]
            
            # Hugging Face URLを構築
            hf_url = f"https://huggingface.co/datasets/{self.full_repo_name}/resolve/main/{hf_path}"
            
            # データベースを更新
            from .database import DatabaseManager
            db_manager = DatabaseManager(self.config)
            
            # 既存のHugging Face URLsを取得
            import pysqlite3 as sqlite3
            conn = sqlite3.connect('data/eventmonitor.db')
            cursor = conn.cursor()
            cursor.execute('SELECT huggingface_urls FROM all_tweets WHERE id = ?', (tweet_id,))
            result = cursor.fetchone()
            
            existing_urls = []
            if result and result[0]:
                existing_urls = json.loads(result[0])
            
            # 新しいURLを追加
            if hf_url not in existing_urls:
                existing_urls.append(hf_url)
                cursor.execute('UPDATE all_tweets SET huggingface_urls = ? WHERE id = ?', 
                             (json.dumps(existing_urls), tweet_id))
                conn.commit()
                self.logger.debug(f"Updated HF URLs for tweet {tweet_id}")
            
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Failed to update tweet HF URLs: {e}")
    
    def _update_tweet_hf_urls_batch(self, tweet_id: str, hf_urls: List[str], is_log_only: bool = False):
        """複数のHuggingFace URLを一度に更新
        
        Args:
            tweet_id: ツイートID
            hf_urls: HuggingFace URLs
            is_log_only: Trueの場合log_only_tweetsテーブル、Falseの場合all_tweetsテーブル
        """
        try:
            import pysqlite3 as sqlite3
            conn = sqlite3.connect('data/eventmonitor.db')
            cursor = conn.cursor()
            
            # テーブル名を決定
            table_name = 'log_only_tweets' if is_log_only else 'all_tweets'
            
            # 既存のURLsを取得
            cursor.execute(f'SELECT huggingface_urls FROM {table_name} WHERE id = ?', (tweet_id,))
            result = cursor.fetchone()
            
            existing_urls = []
            if result and result[0]:
                existing_urls = json.loads(result[0])
            
            # 新しいURLsを追加（重複を避ける）
            for url in hf_urls:
                if url not in existing_urls:
                    existing_urls.append(url)
            
            # データベースを更新
            cursor.execute(f'UPDATE {table_name} SET huggingface_urls = ? WHERE id = ?', 
                         (json.dumps(existing_urls), tweet_id))
            conn.commit()
            conn.close()
            
            self.logger.debug(f"Updated {len(hf_urls)} HF URLs for tweet {tweet_id} in {table_name}")
            
        except Exception as e:
            self.logger.error(f"Failed to update tweet HF URLs batch in {table_name}: {e}")
    
    async def _upload_file_with_retry(self, file_path: Path, file_type: str, max_retries: int = 3) -> Optional[str]:
        """ファイルをアップロード（3回まで再試行、レート制限対応）"""
        for attempt in range(max_retries):
            try:
                # 暗号化が有効な場合
                if self.rclone_client:
                    hf_url = await self._upload_encrypted_file_internal(file_path, file_type)
                else:
                    hf_url = await self._upload_plain_file_internal(file_path, file_type)
                
                if hf_url:
                    return hf_url
                    
            except Exception as e:
                error_msg = str(e)
                
                # レート制限エラーの処理
                if "429" in error_msg or "rate limit" in error_msg.lower():
                    wait_time = self._extract_wait_time(error_msg)
                    self.logger.warning(f"Rate limit hit, waiting {wait_time}s before retry")
                    time.sleep(wait_time)
                    continue
                
                # ファイル数上限エラーの処理
                if "over the limit of 100000 files" in error_msg:
                    if self._handle_file_limit_error():
                        # 新しいリポジトリで再試行
                        continue
                    else:
                        # リポジトリ作成に失敗
                        return None
                
                # その他のエラー
                if attempt < max_retries - 1:
                    self.logger.warning(f"Upload failed for {file_path} (attempt {attempt + 1}/{max_retries}), retrying...")
                    time.sleep(1)  # 短い待機
                else:
                    self.logger.error(f"Upload failed for {file_path} after {max_retries} attempts: {e}")
                    
        return None
    
    def _extract_wait_time(self, error_msg: str) -> int:
        """エラーメッセージから待機時間を抽出"""
        wait_time = 3600  # デフォルト1時間
        
        # パターンマッチング
        patterns = [
            r"retry in (\d+) seconds",
            r"retry in (\d+) minutes",
            r"retry in (\d+) hours",
            r"you can retry this action in (\d+) (minutes?|hours?|seconds?)",
            r"(\d+)\s+(minutes?|hours?|seconds?)"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, error_msg, re.IGNORECASE)
            if match:
                value = int(match.group(1))
                unit = match.group(2) if match.lastindex > 1 else "seconds"
                
                if "hour" in unit:
                    wait_time = value * 3600
                elif "minute" in unit:
                    wait_time = value * 60
                else:
                    wait_time = value
                break
        
        return wait_time + 1  # +1秒のバッファ
    
    async def _upload_encrypted_file_internal(self, file_path: Path, file_type: str) -> Optional[str]:
        """ファイルを暗号化してアップロード（内部メソッド）"""
        # ファイルタイプに基づいてディレクトリを決定
        if file_type == 'image':
            base_dir = Path("images")
            hf_dir = "encrypted_images"
        else:
            base_dir = Path("videos") 
            hf_dir = "encrypted_videos"
        
        # ユーザー名を取得
        username = file_path.parent.name
        
        # 暗号化
        encrypted_files = self.rclone_client.encrypt_files_batch([file_path], base_dir)
        
        if not encrypted_files:
            return None
        
        # 暗号化されたファイルをアップロード
        encrypted_file = list(encrypted_files.values())[0]
        hf_path = f"{hf_dir}/{username}/{encrypted_file.name}"
        
        upload_file(
            path_or_fileobj=str(encrypted_file),
            path_in_repo=hf_path,
            repo_id=self.full_repo_name,
            token=self.api.token,
            repo_type="dataset"
        )
        
        # 一時ファイルをクリーンアップ
        self.rclone_client.cleanup_temp_files(encrypted_files)
        
        # HuggingFace URLを返す
        return f"https://huggingface.co/datasets/{self.full_repo_name}/resolve/main/{hf_path}"
    
    async def _upload_plain_file_internal(self, file_path: Path, file_type: str) -> Optional[str]:
        """ファイルを暗号化せずにアップロード（内部メソッド）"""
        # ファイルタイプに基づいてディレクトリを決定
        if file_type == 'image':
            hf_dir = "images"
        else:
            hf_dir = "videos"
        
        # ユーザー名を取得
        username = file_path.parent.name
        
        # HuggingFaceのパス
        hf_path = f"{hf_dir}/{username}/{file_path.name}"
        
        # アップロード
        upload_file(
            path_or_fileobj=str(file_path),
            path_in_repo=hf_path,
            repo_id=self.full_repo_name,
            token=self.api.token,
            repo_type="dataset"
        )
        
        # HuggingFace URLを返す
        return f"https://huggingface.co/datasets/{self.full_repo_name}/resolve/main/{hf_path}"
    
    async def _is_already_uploaded(self, tweet_id: str) -> bool:
        """データベースから該当ツイートが既にアップロード済みかチェック（互換性のため残す）"""
        try:
            import pysqlite3 as sqlite3
            conn = sqlite3.connect('data/eventmonitor.db')
            cursor = conn.cursor()
            
            cursor.execute("SELECT huggingface_urls FROM all_tweets WHERE id = ?", (tweet_id,))
            result = cursor.fetchone()
            
            conn.close()
            
            # データベースにhuggingface_urlsが設定されている場合は既にアップロード済み
            if result and result[0]:
                urls = json.loads(result[0])
                if len(urls) > 0:
                    return True
            
            # huggingface_urlsが未設定でも、ローカルファイルが存在しない場合は
            # 既に暗号化・アップロード済みの可能性が高い
            return False
                
        except Exception as e:
            self.logger.debug(f"Error checking upload status for tweet {tweet_id}: {e}")
            return False
    
    async def _is_file_already_uploaded(self, file_path: Path, hf_path: str) -> bool:
        """個別ファイルが既にアップロード済みかチェック"""
        try:
            # ファイル名からツイートIDを抽出
            tweet_id = file_path.stem.split('_')[0]
            
            import pysqlite3 as sqlite3
            conn = sqlite3.connect('data/eventmonitor.db')
            cursor = conn.cursor()
            
            cursor.execute("SELECT huggingface_urls FROM all_tweets WHERE id = ?", (tweet_id,))
            result = cursor.fetchone()
            
            conn.close()
            
            if result and result[0]:
                urls = json.loads(result[0])
                # HuggingFace URLを構築して存在チェック
                expected_url = f"https://huggingface.co/datasets/{self.full_repo_name}/resolve/main/{hf_path}"
                if expected_url in urls:
                    self.logger.debug(f"File already uploaded: {file_path.name} -> {hf_path}")
                    return True
            
            return False
                
        except Exception as e:
            self.logger.debug(f"Error checking file upload status for {file_path}: {e}")
            return False
    
    async def _upload_plain_images(self, images_dir: Path):
        """暗号化なしで画像をアップロード"""
        upload_count = 0
        
        # 各ユーザーディレクトリの画像をアップロード
        for user_dir in images_dir.iterdir():
            if user_dir.is_dir():
                # ディレクトリ名はdisplay_name
                display_name = user_dir.name
                
                for image_file in user_dir.glob("*.jpg"):
                    try:
                        # Hugging Faceのパス
                        hf_path = f"images/{display_name}/{image_file.name}"
                        
                        # 個別ファイルが既にアップロード済みかチェック
                        if await self._is_file_already_uploaded(image_file, hf_path):
                            continue
                        
                        # アップロード
                        self._upload_with_retry(
                            path_or_fileobj=str(image_file),
                            path_in_repo=hf_path,
                            repo_id=self.full_repo_name,
                            token=self.api.token,
                            repo_type="dataset"
                        )
                        
                        # データベースのHugging Face URLsを更新
                        await self._update_tweet_hf_urls(image_file, hf_path)
                        
                        upload_count += 1
                        self.logger.debug(f"Uploaded image: {hf_path}")
                        
                    except Exception as e:
                        self.logger.error(f"Failed to upload image {image_file}: {e}")
        
        self.logger.info(f"Uploaded {upload_count} new images")
    
    async def _upload_encrypted_images(self, images_dir: Path):
        """暗号化して画像をアップロード"""
        # 暗号化マッピングを一度だけ取得
        existing_mapping = await self._get_existing_mapping()
        self.logger.info(f"Existing mapping contains {len(existing_mapping)} entries")
        
        # 各ユーザーディレクトリの画像を収集
        files_to_encrypt = {}
        total_files_checked = 0
        skipped_files = 0
        
        self.logger.info("Scanning image directories...")
        
        for user_dir in images_dir.iterdir():
            if user_dir.is_dir():
                # ディレクトリ名はdisplay_name
                display_name = user_dir.name
                user_files = list(user_dir.glob("*.jpg"))
                
                if user_files:
                    self.logger.info(f"Checking {len(user_files)} files in {display_name}...")
                
                # 各ユーザーディレクトリでローカルファイルが存在するもののみ処理
                for image_file in user_files:
                    total_files_checked += 1
                    
                    # Progress indicator every 100 files
                    if total_files_checked % 100 == 0:
                        self.logger.info(f"Progress: Checked {total_files_checked} files, {len(files_to_encrypt)} to encrypt, {skipped_files} skipped")
                    
                    # 相対パスを作成
                    relative_path = f"{display_name}/{image_file.name}"
                    
                    # 既に暗号化マッピングに存在するかチェック
                    if relative_path in existing_mapping:
                        self.logger.debug(f"Image already uploaded: {relative_path}")
                        skipped_files += 1
                        continue
                    
                    # 個別ファイルが既にアップロード済みかチェック
                    # 暗号化ファイルの場合でも、元のファイル名でHuggingFace URLを構築
                    hf_path = f"encrypted_images/{display_name}/{image_file.name}"
                    if await self._is_file_already_uploaded(image_file, hf_path):
                        skipped_files += 1
                        continue
                    
                    # 画像ファイルが実際に存在し、読み取り可能かチェック
                    if not image_file.exists() or not image_file.is_file():
                        self.logger.debug(f"Local file not found or not accessible: {image_file}")
                        skipped_files += 1
                        continue
                    
                    # ファイルサイズチェック（0バイトファイルは除外）
                    try:
                        if image_file.stat().st_size == 0:
                            self.logger.debug(f"Empty file skipped: {image_file}")
                            skipped_files += 1
                            continue
                    except:
                        self.logger.debug(f"Cannot access file stats: {image_file}")
                        skipped_files += 1
                        continue
                    
                    # デバッグログ
                    self.logger.debug(f"Will encrypt: {relative_path} (tweet_id: {tweet_id})")
                    files_to_encrypt[image_file] = Path(relative_path)
        
        self.logger.info(f"Scan complete: {total_files_checked} files checked, {len(files_to_encrypt)} to encrypt, {skipped_files} skipped")
        
        if not files_to_encrypt:
            self.logger.info("No new images to upload")
            return
        
        # ファイルを暗号化
        # files_to_encrypt のキーのリストを作成
        file_paths = list(files_to_encrypt.keys())
        base_dir = Path("images")
        
        self.logger.info(f"Starting batch encryption of {len(file_paths)} files...")
        encrypted_files = self.rclone_client.encrypt_files_batch(file_paths, base_dir)
        
        if not encrypted_files:
            self.logger.error("No files were successfully encrypted")
            return
        
        self.logger.info(f"Encrypted {len(encrypted_files)} files, starting upload...")
        
        # 暗号化されたファイルをアップロード
        upload_count = 0
        failed_uploads = 0
        total_to_upload = len(encrypted_files)
        
        for original_file, encrypted_file in encrypted_files.items():
            relative_path = files_to_encrypt[original_file]
            try:
                # encrypted_images/username/encrypted_filename
                hf_path = f"encrypted_images/{relative_path.parent}/{encrypted_file.name}"
                
                # Progress indicator
                if upload_count % 10 == 0 and upload_count > 0:
                    self.logger.info(f"Upload progress: {upload_count}/{total_to_upload} completed, {failed_uploads} failed")
                
                # バッチアップロード時の遅延（10ファイルごとに短い待機）
                if upload_count > 0 and upload_count % 10 == 0:
                    delay = 1 + random.random() * 2  # 1-3秒のランダムな遅延
                    self.logger.debug(f"Batch delay: waiting {delay:.1f} seconds after {upload_count} uploads")
                    time.sleep(delay)
                
                self._upload_with_retry(
                    path_or_fileobj=str(encrypted_file),
                    path_in_repo=hf_path,
                    repo_id=self.full_repo_name,
                    token=self.api.token,
                    repo_type="dataset"
                )
                
                # データベースのHugging Face URLsを更新
                await self._update_tweet_hf_urls(original_file, hf_path)
                
                upload_count += 1
                self.logger.debug(f"Uploaded encrypted image: {hf_path}")
                
            except Exception as e:
                failed_uploads += 1
                self.logger.error(f"Failed to upload encrypted image {encrypted_file}: {e}")
        
        self.logger.info(f"Upload complete: {upload_count}/{total_to_upload} successful, {failed_uploads} failed")
        
        # 暗号化マッピングを保存
        await self._save_encryption_mapping(files_to_encrypt, encrypted_files)
        
        # 一時ファイルをクリーンアップ（アップロード完了後）
        self.rclone_client.cleanup_temp_files(encrypted_files)
        
        # eventmonitor_encrypted_files ディレクトリをクリーンアップ
        self.rclone_client.cleanup()
    
    async def _upload_plain_videos(self, videos_dir: Path):
        """暗号化なしで動画をアップロード"""
        upload_count = 0
        
        # 各ユーザーディレクトリの動画をアップロード
        for user_dir in videos_dir.iterdir():
            if user_dir.is_dir():
                # ディレクトリ名はdisplay_name
                display_name = user_dir.name
                
                # 動画ファイルの拡張子パターン
                video_patterns = ["*.mp4", "*.gif", "*.m3u8", "*.webm", "*.mov", "*.avi"]
                
                for pattern in video_patterns:
                    for video_file in user_dir.glob(pattern):
                        try:
                            # Hugging Faceのパス
                            hf_path = f"videos/{display_name}/{video_file.name}"
                            
                            # 個別ファイルが既にアップロード済みかチェック
                            if await self._is_file_already_uploaded(video_file, hf_path):
                                continue
                            
                            # アップロード
                            self._upload_with_retry(
                                path_or_fileobj=str(video_file),
                                path_in_repo=hf_path,
                                repo_id=self.full_repo_name,
                                token=self.api.token,
                                repo_type="dataset"
                            )
                            
                            # データベースのHugging Face URLsを更新
                            await self._update_tweet_hf_urls(video_file, hf_path)
                            
                            upload_count += 1
                            self.logger.debug(f"Uploaded video: {hf_path}")
                            
                        except Exception as e:
                            self.logger.error(f"Failed to upload video {video_file}: {e}")
        
        self.logger.info(f"Uploaded {upload_count} new videos")
    
    async def _upload_encrypted_videos(self, videos_dir: Path):
        """暗号化して動画をアップロード"""
        # 暗号化マッピングを一度だけ取得
        existing_mapping = await self._get_existing_mapping()
        self.logger.info(f"Existing video mapping contains {len(existing_mapping)} entries")
        
        # 各ユーザーディレクトリの動画を収集
        files_to_encrypt = {}
        
        # 動画ファイルの拡張子パターン
        video_patterns = ["*.mp4", "*.gif", "*.m3u8", "*.webm", "*.mov", "*.avi"]
        
        for user_dir in videos_dir.iterdir():
            if user_dir.is_dir():
                # ディレクトリ名はdisplay_name
                display_name = user_dir.name
                
                # 各ユーザーディレクトリでローカルファイルが存在するもののみ処理
                for pattern in video_patterns:
                    for video_file in user_dir.glob(pattern):
                        # 相対パスを作成
                        relative_path = f"{display_name}/{video_file.name}"
                        
                        # 既に暗号化マッピングに存在するかチェック
                        if relative_path in existing_mapping:
                            self.logger.debug(f"Video already uploaded: {relative_path}")
                            continue
                        
                        # 個別ファイルが既にアップロード済みかチェック
                        hf_path = f"encrypted_videos/{display_name}/{video_file.name}"
                        if await self._is_file_already_uploaded(video_file, hf_path):
                            continue
                        
                        # 動画ファイルが実際に存在し、読み取り可能かチェック
                        if not video_file.exists() or not video_file.is_file():
                            self.logger.debug(f"Local file not found or not accessible: {video_file}")
                            continue
                        
                        # ファイルサイズチェック（0バイトファイルは除外）
                        try:
                            if video_file.stat().st_size == 0:
                                self.logger.debug(f"Empty file skipped: {video_file}")
                                continue
                        except:
                            self.logger.debug(f"Cannot access file stats: {video_file}")
                            continue
                        
                        # デバッグログ
                        self.logger.info(f"Will encrypt video: {relative_path} (tweet_id: {tweet_id})")
                        files_to_encrypt[video_file] = Path(relative_path)
        
        if not files_to_encrypt:
            self.logger.info("No new videos to upload")
            return
        
        # ファイルを暗号化
        # files_to_encrypt のキーのリストを作成
        file_paths = list(files_to_encrypt.keys())
        base_dir = Path("videos")
        self.logger.info(f"Starting batch encryption of {len(file_paths)} video files...")
        encrypted_files = self.rclone_client.encrypt_files_batch(file_paths, base_dir)
        
        if not encrypted_files:
            self.logger.error("No video files were successfully encrypted")
            return
        
        self.logger.info(f"Encrypted {len(encrypted_files)} video files, starting upload...")
        
        # 暗号化されたファイルをアップロード
        upload_count = 0
        failed_uploads = 0
        total_to_upload = len(encrypted_files)
        
        for original_file, encrypted_file in encrypted_files.items():
            relative_path = files_to_encrypt[original_file]
            try:
                # encrypted_videos/username/encrypted_filename
                hf_path = f"encrypted_videos/{relative_path.parent}/{encrypted_file.name}"
                
                # 暗号化されたファイルが実際に存在するかチェック
                if not encrypted_file.exists():
                    self.logger.error(f"Encrypted file does not exist: {encrypted_file}")
                    self.logger.error(f"Expected path: {encrypted_file.absolute()}")
                    failed_uploads += 1
                    continue
                
                # ファイルサイズチェック
                try:
                    file_size = encrypted_file.stat().st_size
                    self.logger.debug(f"Encrypted file size: {file_size} bytes for {encrypted_file.name}")
                    if file_size == 0:
                        self.logger.error(f"Encrypted file is empty: {encrypted_file}")
                        failed_uploads += 1
                        continue
                except Exception as e:
                    self.logger.error(f"Cannot access encrypted file stats: {encrypted_file}: {e}")
                    failed_uploads += 1
                    continue
                
                # Progress indicator
                if upload_count % 10 == 0 and upload_count > 0:
                    self.logger.info(f"Upload progress: {upload_count}/{total_to_upload} completed, {failed_uploads} failed")
                
                # バッチアップロード時の遅延（10ファイルごとに短い待機）
                if upload_count > 0 and upload_count % 10 == 0:
                    delay = 1 + random.random() * 2  # 1-3秒のランダムな遅延
                    self.logger.debug(f"Batch delay: waiting {delay:.1f} seconds after {upload_count} uploads")
                    time.sleep(delay)
                
                self._upload_with_retry(
                    path_or_fileobj=str(encrypted_file),
                    path_in_repo=hf_path,
                    repo_id=self.full_repo_name,
                    token=self.api.token,
                    repo_type="dataset"
                )
                
                # データベースのHugging Face URLsを更新
                await self._update_tweet_hf_urls(original_file, hf_path)
                
                upload_count += 1
                self.logger.debug(f"Uploaded encrypted video: {hf_path}")
                
            except Exception as e:
                failed_uploads += 1
                self.logger.error(f"Failed to upload encrypted video {encrypted_file}: {e}")
        
        self.logger.info(f"Upload complete: {upload_count}/{total_to_upload} successful, {failed_uploads} failed")
        
        # 暗号化マッピングを保存（動画用）
        await self._save_video_encryption_mapping(files_to_encrypt, encrypted_files)
        
        # 一時ファイルをクリーンアップ（アップロード完了後）
        self.rclone_client.cleanup_temp_files(encrypted_files)
        
        # eventmonitor_encrypted_files ディレクトリをクリーンアップ
        self.rclone_client.cleanup()
    
    async def _save_video_encryption_mapping(self, original_files: Dict[Path, Path], encrypted_files: Dict[Path, Path]):
        """動画の暗号化ファイル名のマッピングを保存"""
        try:
            # マッピングを作成
            mapping = {}
            for original_path, relative_path in original_files.items():
                if original_path in encrypted_files:
                    encrypted_path = encrypted_files[original_path]
                    mapping[str(relative_path)] = encrypted_path.name
            
            # 既存のマッピングを取得
            existing_mapping = {}
            try:
                from huggingface_hub import hf_hub_download
                local_path = hf_hub_download(
                    repo_id=self.full_repo_name,
                    filename="encrypted_videos/filename_mapping.json",
                    token=self.api.token,
                    repo_type="dataset"
                )
                with open(local_path, 'r', encoding='utf-8') as f:
                    existing_mapping = json.load(f)
            except:
                pass
            
            # マッピングを更新
            existing_mapping.update(mapping)
            
            # 一時ファイルに保存
            temp_file = Path("temp_video_filename_mapping.json")
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(existing_mapping, f, ensure_ascii=False, indent=2)
            
            # Hugging Faceにアップロード
            self._upload_with_retry(
                path_or_fileobj=str(temp_file),
                path_in_repo="encrypted_videos/filename_mapping.json",
                repo_id=self.full_repo_name,
                token=self.api.token,
                repo_type="dataset"
            )
            
            # 一時ファイルを削除
            temp_file.unlink()
            
            self.logger.info(f"Saved video encryption mapping for {len(mapping)} files")
            
        except Exception as e:
            self.logger.error(f"Failed to save video encryption mapping: {e}")