import logging
import json
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
from huggingface_hub import HfApi, upload_file
import tempfile
import aiohttp
import asyncio
import time
import re
from .rclone_client import RcloneClient, RcloneConfig


class LogOnlyHFUploader:
    """ログ専用アカウント用のHugging Faceアップローダー"""
    
    def __init__(self, config: dict, db_manager=None):
        self.config = config
        self.logger = logging.getLogger("EventMonitor.LogOnlyHF")
        self.db_manager = db_manager
        
        # レート制限対策の設定
        self.max_retries = 3
        self.base_delay = 2.0  # 基本待機時間（秒）- 各アップロード間の最小間隔
        self.upload_queue = []  # アップロード待ちのファイル
        self.last_upload_time = 0
        self.batch_upload_interval = 300  # バッチアップロードの間隔（5分）
        self.max_concurrent_uploads = 3  # 同時アップロード数の制限
        self.log_only_config = config.get('log_only_accounts', {})
        
        # 機能が有効かチェック
        if not self.log_only_config.get('enabled', False):
            self.logger.info("Log-only accounts feature is disabled")
            self.enabled = False
            return
        
        self.enabled = True
        self.delete_after_upload = self.log_only_config.get('delete_after_upload', True)
        self.batch_size = self.log_only_config.get('batch_size', 50)
        
        # HfApiの初期化
        try:
            token = os.getenv('HUGGINGFACE_API_KEY')
            if not token:
                self.logger.warning("HUGGINGFACE_API_KEY not found, log-only HF upload disabled")
                self.enabled = False
                return
            
            self.api = HfApi(token=token)
            
            # メインのバックアップ設定と同じリポジトリ名を使用
            hf_backup_config = config.get('huggingface_backup', {})
            self.repo_name = os.getenv('HUGGINGFACE_REPO_NAME', hf_backup_config.get('repo_name', 'event-monitor-tweets'))
            
            # ユーザー情報を取得
            user_info = self.api.whoami()
            self.username = user_info['name']
            
            # リポジトリ名にユーザー名を含める
            if '/' not in self.repo_name:
                self.full_repo_name = f"{self.username}/{self.repo_name}"
            else:
                self.full_repo_name = self.repo_name
            
            self.logger.info(f"Initialized log-only HF uploader for {self.full_repo_name}")
            
            # リポジトリの作成（存在しない場合）
            self._ensure_repo_exists()
            
            # rclone暗号化の初期化（通常のバックアップ設定から流用）
            self.rclone_client = None
            hf_backup_config = config.get('huggingface_backup', {})
            if hf_backup_config.get('rclone_encryption', {}).get('enabled', False):
                try:
                    rclone_config = RcloneConfig(
                        remote_name=hf_backup_config['rclone_encryption']['remote_name'],
                        config_path=hf_backup_config['rclone_encryption'].get('config_path')
                    )
                    self.rclone_client = RcloneClient(rclone_config)
                    self.logger.info("Initialized rclone encryption for log-only image uploads")
                except Exception as e:
                    self.logger.warning(f"Failed to initialize rclone encryption: {e}")
                    
        except Exception as e:
            self.logger.error(f"Failed to initialize log-only HF uploader: {e}")
            self.enabled = False
    
    def _ensure_repo_exists(self):
        """リポジトリが存在することを確認（なければ作成）"""
        try:
            # リポジトリの存在確認
            try:
                self.api.repo_info(self.full_repo_name, repo_type="dataset")
                self.logger.debug(f"Repository {self.full_repo_name} already exists")
            except:
                # リポジトリが存在しない場合は作成
                self.api.create_repo(
                    repo_id=self.full_repo_name,
                    repo_type="dataset"
                )
                self.logger.info(f"Created new dataset repository: {self.full_repo_name}")
                
                # README.mdを作成（メインリポジトリ用に内容を更新）
                readme_content = f"""# EventMonitor

This repository contains data from the EventMonitor system.

## Structure
- `encrypted_images/`: Encrypted image files from tweets
- `encrypted_videos/`: Encrypted video files from tweets
- `tweets.db`: SQLite database containing tweet metadata

Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
                readme_path = Path("temp_readme.md")
                readme_path.write_text(readme_content)
                
                upload_file(
                    path_or_fileobj=str(readme_path),
                    path_in_repo="README.md",
                    repo_id=self.full_repo_name,
                    repo_type="dataset",
                    token=self.api.token
                )
                
                readme_path.unlink()
                
        except Exception as e:
            self.logger.error(f"Failed to ensure repository exists: {e}")
            raise
    
    async def process_tweets(self, tweets: List[Dict[str, Any]], username: str) -> List[Dict[str, Any]]:
        """ツイートの画像をダウンロード、HFにアップロード、URLを更新"""
        if not self.enabled:
            return tweets
        
        processed_tweets = []
        
        # 一時ディレクトリを作成
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            for tweet in tweets:
                try:
                    tweet_copy = tweet.copy()
                    
                    # 画像と動画がある場合の処理
                    hf_urls = []
                    
                    if tweet_copy.get('media'):
                        image_urls = await self._process_tweet_images(
                            tweet_copy,
                            username,
                            temp_path
                        )
                        hf_urls.extend(image_urls)
                    
                    if tweet_copy.get('videos'):
                        video_urls = await self._process_tweet_videos(
                            tweet_copy,
                            username,
                            temp_path
                        )
                        hf_urls.extend(video_urls)
                    
                    tweet_copy['huggingface_urls'] = hf_urls
                    tweet_copy['uploaded_to_hf'] = bool(hf_urls)
                    
                    # データベースを更新
                    if self.db_manager and hf_urls:
                        self.db_manager.update_log_only_tweet_hf_urls(tweet_copy['id'], hf_urls)
                    
                    processed_tweets.append(tweet_copy)
                    
                except Exception as e:
                    self.logger.error(f"Failed to process tweet {tweet.get('id')}: {e}")
                    # エラーが発生してもツイート自体は保存する
                    tweet_copy = tweet.copy()
                    tweet_copy['huggingface_urls'] = []
                    tweet_copy['uploaded_to_hf'] = False
                    processed_tweets.append(tweet_copy)
        
        return processed_tweets
    
    async def _process_tweet_images(self, tweet: Dict[str, Any], username: str, temp_dir: Path) -> List[str]:
        """ツイートの画像をダウンロードしてHFにアップロード"""
        hf_urls = []
        
        # 画像をダウンロード
        async with aiohttp.ClientSession() as session:
            for i, media_url in enumerate(tweet.get('media', [])):
                try:
                    # ファイル名を生成
                    ext = media_url.split('.')[-1].split('?')[0]
                    filename = f"{tweet['id']}_{i}.{ext}"
                    local_path = temp_dir / filename
                    
                    # ダウンロード
                    async with session.get(media_url) as response:
                        if response.status == 200:
                            content = await response.read()
                            local_path.write_bytes(content)
                            
                            # HFにアップロード（通常のencrypted_imagesディレクトリに保存）
                            hf_path = f"encrypted_images/{username}/{tweet['id']}/{filename}"
                            
                            # rclone暗号化が有効な場合
                            if self.rclone_client:
                                # rclone_clientのtemp_dir内にファイルをコピーしてから暗号化
                                rclone_temp_path = self.rclone_client.temp_dir / filename
                                rclone_temp_path.write_bytes(content)
                                encrypted_filename = filename + '.enc'
                                encrypted_path = self.rclone_client.encrypt_file(
                                    rclone_temp_path, 
                                    self.rclone_client.temp_dir / encrypted_filename
                                )
                                if encrypted_path:
                                    upload_path = str(encrypted_path)
                                    hf_path += ".enc"
                                else:
                                    upload_path = str(local_path)
                                # 一時ファイルを削除
                                if rclone_temp_path.exists():
                                    rclone_temp_path.unlink()
                            else:
                                upload_path = str(local_path)
                            
                            # アップロード（リトライ機能付き）
                            upload_success = await self._upload_with_retry(
                                upload_path=upload_path,
                                hf_path=hf_path
                            )
                            
                            if upload_success:
                                # HF URLを記録
                                hf_url = f"https://huggingface.co/datasets/{self.full_repo_name}/resolve/main/{hf_path}"
                                hf_urls.append(hf_url)
                                
                                self.logger.debug(f"Uploaded image to HF: {hf_path}")
                            
                            # 暗号化ファイルがある場合は削除
                            if self.rclone_client and encrypted_path and Path(encrypted_path).exists():
                                Path(encrypted_path).unlink()
                                
                except Exception as e:
                    self.logger.error(f"Failed to process image {media_url}: {e}")
        
        return hf_urls
    
    async def _process_tweet_videos(self, tweet: Dict[str, Any], username: str, temp_dir: Path) -> List[str]:
        """ツイートの動画をダウンロードしてHFにアップロード"""
        hf_urls = []
        
        # 動画をダウンロード
        async with aiohttp.ClientSession() as session:
            for i, video_url in enumerate(tweet.get('videos', [])):
                try:
                    # ファイル名を生成（拡張子を適切に判断）
                    if '.m3u8' in video_url:
                        ext = 'm3u8'
                    elif '.gif' in video_url:
                        ext = 'gif'
                    else:
                        ext = 'mp4'  # デフォルト
                    
                    filename = f"{tweet['id']}_video_{i}.{ext}"
                    local_path = temp_dir / filename
                    
                    # ダウンロード
                    async with session.get(video_url) as response:
                        if response.status == 200:
                            content = await response.read()
                            local_path.write_bytes(content)
                            
                            # HFにアップロード（通常のencrypted_videosディレクトリに保存）
                            hf_path = f"encrypted_videos/{username}/{tweet['id']}/{filename}"
                            
                            # rclone暗号化が有効な場合
                            if self.rclone_client:
                                # rclone_clientのtemp_dir内にファイルをコピーしてから暗号化
                                rclone_temp_path = self.rclone_client.temp_dir / filename
                                rclone_temp_path.write_bytes(content)
                                encrypted_filename = filename + '.enc'
                                encrypted_path = self.rclone_client.encrypt_file(
                                    rclone_temp_path, 
                                    self.rclone_client.temp_dir / encrypted_filename
                                )
                                if encrypted_path:
                                    upload_path = str(encrypted_path)
                                    hf_path += ".enc"
                                else:
                                    upload_path = str(local_path)
                                # 一時ファイルを削除
                                if rclone_temp_path.exists():
                                    rclone_temp_path.unlink()
                            else:
                                upload_path = str(local_path)
                            
                            # アップロード（リトライ機能付き）
                            upload_success = await self._upload_with_retry(
                                upload_path=upload_path,
                                hf_path=hf_path
                            )
                            
                            if upload_success:
                                # HF URLを記録
                                hf_url = f"https://huggingface.co/datasets/{self.full_repo_name}/resolve/main/{hf_path}"
                                hf_urls.append(hf_url)
                                
                                self.logger.debug(f"Uploaded video to HF: {hf_path}")
                            
                            # 暗号化ファイルがある場合は削除
                            if self.rclone_client and encrypted_path and Path(encrypted_path).exists():
                                Path(encrypted_path).unlink()
                                
                except Exception as e:
                    self.logger.error(f"Failed to process video {video_url}: {e}")
        
        return hf_urls
    
    
    def _handle_upload_error(self, error: Exception) -> tuple[bool, float]:
        """アップロードエラーを処理し、待機時間を返す
        
        Returns:
            tuple[bool, float]: (リトライすべきか, 待機時間(秒))
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
                    if "hour" in time_unit:
                        wait_time = time_value * 3600
                    else:
                        wait_time = time_value * 60
            
            return True, wait_time
        
        # その他のエラー
        return False, 0
    
    async def _upload_with_retry(self, upload_path: str, hf_path: str) -> bool:
        """リトライ機能付きのアップロード処理"""
        for attempt in range(self.max_retries):
            try:
                # レート制限対策：前回のアップロードから一定時間待機
                current_time = time.time()
                time_since_last = current_time - self.last_upload_time
                if time_since_last < self.base_delay:
                    await asyncio.sleep(self.base_delay - time_since_last)
                
                # アップロード実行
                upload_file(
                    path_or_fileobj=upload_path,
                    path_in_repo=hf_path,
                    repo_id=self.full_repo_name,
                    repo_type="dataset",
                    token=self.api.token
                )
                
                self.last_upload_time = time.time()
                return True
                
            except Exception as e:
                should_retry, wait_time = self._handle_upload_error(e)
                
                if should_retry:
                    # レート制限エラーの場合
                    self.logger.warning(
                        f"Rate limit hit for {hf_path}. "
                        f"Waiting {wait_time:.1f}s before retry {attempt + 1}/{self.max_retries}"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                
                # その他のエラー
                if attempt < self.max_retries - 1:
                    wait_time = self.base_delay * (attempt + 1)
                    self.logger.warning(
                        f"Upload failed for {hf_path}: {e}. "
                        f"Retrying in {wait_time:.1f}s ({attempt + 1}/{self.max_retries})"
                    )
                    await asyncio.sleep(wait_time)
                else:
                    self.logger.error(f"Failed to upload {hf_path} after {self.max_retries} attempts: {e}")
                    return False
        
        return False