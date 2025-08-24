import logging
import json
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
from huggingface_hub import HfApi, upload_file, create_repo
import tempfile
import aiohttp
import asyncio
import time
import re
import yaml
from .rclone_client import RcloneClient, RcloneConfig


class LogOnlyHFUploader:
    """ログ専用アカウント用のHugging Faceアップローダー"""
    
    def __init__(self, config: dict, db_manager=None, backup_manager=None):
        self.config = config
        self.logger = logging.getLogger("EventMonitor.LogOnlyHF")
        self.db_manager = db_manager
        self.backup_manager = backup_manager  # BackupManagerの参照を保持
        
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
        
        # アップロードモード設定
        self.upload_mode = self.log_only_config.get('upload_mode', 'immediate')
        
        # バッチモード設定
        batch_mode_config = self.log_only_config.get('batch_mode', {})
        self.batch_encrypt = batch_mode_config.get('encrypt_before_upload', True)
        self.batch_delete_after = batch_mode_config.get('delete_after_batch_upload', False)
        
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
            self.repo_name = hf_backup_config.get('repo_name', 'event-monitor-tweets')
            
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
            
            self.logger.info(f"Initialized log-only HF uploader for {self.full_repo_name}")
            
            # リポジトリの作成（存在しない場合）
            self._ensure_repo_exists()
            
            # rclone暗号化の初期化（通常のバックアップ設定から流用）
            self.rclone_client = None
            hf_backup_config = config.get('huggingface_backup', {})
            if hf_backup_config.get('rclone_encryption', {}).get('enabled', False):
                try:
                    rclone_config = RcloneConfig(
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
                    repo_type="dataset",
                    exist_ok=True  # 既存のリポジトリがあってもOK
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
    
    def _reload_repo_name_from_config(self):
        """config.yamlから最新のリポジトリ名を再読み込み"""
        try:
            config_path = Path('config.yaml')
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            new_repo_name = config.get('huggingface_backup', {}).get('repo_name', self.repo_name)
            
            # リポジトリ名が変更されていたら更新
            if new_repo_name != self.repo_name:
                self.logger.info(f"Repository name changed in config: {self.repo_name} -> {new_repo_name}")
                self.repo_name = new_repo_name
                
                # フルリポジトリ名を再構築
                if '/' not in self.repo_name:
                    self.full_repo_name = f"{self.username}/{self.repo_name}"
                else:
                    self.full_repo_name = self.repo_name
                
                self.logger.info(f"Using updated repository: {self.full_repo_name}")
        except Exception as e:
            self.logger.error(f"Failed to reload config: {e}")
    
    async def process_downloaded_media(self, media_paths: Dict[str, List[str]], username: str):
        """gallery-dlでダウンロード済みのメディアをHFにアップロード後削除
        
        Args:
            media_paths: {tweet_id: [file_paths]} の辞書
            username: Twitter username
        """
        if not self.enabled:
            return
        
        # BackupManagerから最新のリポジトリ名を取得
        if self.backup_manager and hasattr(self.backup_manager, 'full_repo_name'):
            if self.backup_manager.full_repo_name != self.full_repo_name:
                self.logger.info(f"Using BackupManager's repository: {self.backup_manager.full_repo_name}")
                self.full_repo_name = self.backup_manager.full_repo_name
        
        total_files = sum(len(files) for files in media_paths.values())
        self.logger.info(f"Processing {total_files} files from {len(media_paths)} tweets")
        
        processed_count = 0
        failed_count = 0
        
        for tweet_id, file_paths in media_paths.items():
            # DBから処理済みかチェック
            if self.db_manager:
                existing_urls = self.db_manager.get_log_only_tweet_hf_urls(tweet_id)
                if existing_urls:
                    self.logger.debug(f"Tweet {tweet_id} already uploaded to HF, skipping")
                    # 既にアップロード済みなら、ファイルだけ削除
                    for file_path in file_paths:
                        try:
                            Path(file_path).unlink()
                            self.logger.debug(f"Deleted already-uploaded file: {file_path}")
                        except Exception as e:
                            self.logger.error(f"Failed to delete file {file_path}: {e}")
                    continue
            
            hf_urls = []
            for file_path in file_paths:
                file_path = Path(file_path)
                if not file_path.exists():
                    self.logger.warning(f"File not found: {file_path}")
                    continue
                
                try:
                    # ファイル名から判定（動画か画像か）
                    is_video = file_path.suffix.lower() in {'.mp4', '.mov', '.avi', '.webm', '.mkv', '.flv', '.wmv', 
                                                            '.m4v', '.mpg', '.mpeg', '.3gp', '.3g2', '.ts', '.vob',
                                                            '.ogv', '.f4v', '.asf', '.rm', '.rmvb', '.m2ts', '.mts',
                                                            '.m3u8', '.m3u', '.gif', '.gifv'}
                    
                    # HFパスを決定
                    if is_video:
                        hf_path = f"encrypted_videos/{username}/{tweet_id}/{file_path.name}"
                    else:
                        hf_path = f"encrypted_images/{username}/{tweet_id}/{file_path.name}"
                    
                    # rclone暗号化が有効な場合
                    upload_path = str(file_path)
                    if self.rclone_client:
                        encrypted_path = file_path.parent / f"{file_path.name}.enc"
                        encrypted_path = self.rclone_client.encrypt_file(file_path, encrypted_path)
                        if encrypted_path:
                            upload_path = str(encrypted_path)
                            hf_path += ".enc"
                    
                    # アップロード（リトライ機能付き）
                    upload_success = await self._upload_with_retry(
                        upload_path=upload_path,
                        hf_path=hf_path
                    )
                    
                    if upload_success:
                        # HF URLを記録
                        hf_url = f"https://huggingface.co/datasets/{self.full_repo_name}/resolve/main/{hf_path}"
                        hf_urls.append(hf_url)
                        processed_count += 1
                        self.logger.debug(f"Uploaded and will delete: {file_path.name}")
                        
                        # 元ファイルを削除
                        file_path.unlink()
                        
                        # 暗号化ファイルがある場合も削除
                        if self.rclone_client and upload_path != str(file_path):
                            Path(upload_path).unlink()
                    else:
                        failed_count += 1
                        self.logger.error(f"Failed to upload {file_path.name}")
                        
                except Exception as e:
                    self.logger.error(f"Error processing {file_path}: {e}")
                    failed_count += 1
            
            # DBにHF URLを保存
            if self.db_manager and hf_urls:
                self.db_manager.update_log_only_tweet_hf_urls(tweet_id, hf_urls)
                self.logger.debug(f"Updated DB for tweet {tweet_id} with {len(hf_urls)} URLs")
        
        self.logger.info(f"Completed: {processed_count} uploaded, {failed_count} failed")
    
    async def process_tweets(self, tweets: List[Dict[str, Any]], username: str) -> List[Dict[str, Any]]:
        """ツイートの画像をダウンロード、HFにアップロード、URLを更新"""
        if not self.enabled:
            return tweets
        
        # BackupManagerから最新のリポジトリ名を取得
        if self.backup_manager and hasattr(self.backup_manager, 'full_repo_name'):
            if self.backup_manager.full_repo_name != self.full_repo_name:
                self.logger.info(f"Using BackupManager's repository: {self.backup_manager.full_repo_name}")
                self.full_repo_name = self.backup_manager.full_repo_name
        
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
                    # ファイル名を生成（URLパラメータを除去）
                    # URLからファイル名部分を抽出
                    if '/' in media_url:
                        file_part = media_url.split('/')[-1]
                    else:
                        file_part = media_url
                    
                    # パラメータを除去（?format=jpg&name=orig など）
                    if '?' in file_part:
                        file_part = file_part.split('?')[0]
                    
                    # 拡張子を判定
                    if '.' in file_part:
                        ext = file_part.split('.')[-1]
                    else:
                        # 拡張子がない場合はjpgをデフォルトとする
                        ext = 'jpg'
                    
                    # 有効な画像拡張子かチェック
                    valid_image_extensions = ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp']
                    if ext.lower() not in valid_image_extensions:
                        ext = 'jpg'  # 不明な拡張子の場合はjpgをデフォルト
                    
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
                    # ファイル名を生成（URLパラメータを除去）
                    # URLからファイル名部分を抽出
                    if '/' in video_url:
                        file_part = video_url.split('/')[-1]
                    else:
                        file_part = video_url
                    
                    # パラメータを除去
                    if '?' in file_part:
                        file_part = file_part.split('?')[0]
                    
                    # 拡張子を判定
                    if '.m3u8' in file_part:
                        ext = 'm3u8'
                    elif '.gif' in file_part:
                        ext = 'gif'
                    elif '.' in file_part:
                        ext = file_part.split('.')[-1]
                        # 有効な動画拡張子かチェック
                        valid_video_extensions = ['mp4', 'mov', 'avi', 'webm', 'mkv', 'flv', 'wmv', 'm4v', 'gif', 'm3u8']
                        if ext.lower() not in valid_video_extensions:
                            ext = 'mp4'  # 不明な拡張子の場合はmp4をデフォルト
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
    
    
    def _handle_file_limit_and_create_new_repo(self) -> bool:
        """ファイル上限エラーを処理し、新しいリポジトリに切り替え"""
        self.logger.warning(f"Repository {self.full_repo_name} has reached file limit")
        
        # 次のリポジトリ名を生成
        new_repo_name = self._get_next_repo_name()
        self.logger.info(f"Switching to new repository: {new_repo_name}")
        
        # config.yamlを更新
        self._update_config_file(new_repo_name)
        
        # 新しいリポジトリ名を設定
        self.full_repo_name = new_repo_name
        
        # BackupManagerの変数も更新
        if self.backup_manager and hasattr(self.backup_manager, 'full_repo_name'):
            self.backup_manager.full_repo_name = new_repo_name
            self.logger.info(f"Updated BackupManager's repository to: {new_repo_name}")
        
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
    
    def _handle_upload_error(self, error: Exception) -> tuple[bool, float]:
        """アップロードエラーを処理し、待機時間を返す
        
        Returns:
            tuple[bool, float]: (リトライすべきか, 待機時間(秒))
        """
        error_msg = str(error)
        
        # ファイル数上限エラーをチェック
        if "over the limit of 100000 files" in error_msg:
            if self._handle_file_limit_and_create_new_repo():
                return True, 2.0  # 新しいリポジトリでリトライ
            return False, 0
        
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
    
    async def batch_upload_account_folder(self, username: str, account_type: str = 'log'):
        """アカウントの全ダウンロード完了後にフォルダを一括アップロード
        
        Args:
            username: Twitterアカウント名
            account_type: 'log' または 'monitoring'
        """
        if not self.enabled:
            return
        
        # バッチモードが無効なら何もしない
        if self.upload_mode != 'batch':
            self.logger.debug(f"Batch mode not enabled (current: {self.upload_mode})")
            return
        
        try:
            # BackupManagerを使用してバッチアップロード
            if self.backup_manager:
                # アカウントのデータフォルダを特定
                base_folder = Path('.')  # プロジェクトルート
                
                # 暗号化設定を決定
                encrypt = (account_type == 'log' and self.batch_encrypt)
                delete_after = (account_type == 'log' and self.batch_delete_after)
                
                self.logger.info(f"Starting batch upload for {username} (type: {account_type}, encrypt: {encrypt})")
                
                # BackupManagerにbatch_upload_folderを呼び出し
                await self.backup_manager.batch_upload_folder(
                    folder_path=base_folder,
                    account_type=account_type,
                    encrypt=encrypt,
                    delete_after=delete_after,
                    username=username
                )
                
                self.logger.info(f"Batch upload completed for {username}")
            else:
                self.logger.error("BackupManager not available for batch upload")
                
        except Exception as e:
            self.logger.error(f"Batch upload failed for {username}: {e}")
            raise
    
    def should_use_batch_mode(self) -> bool:
        """バッチモードを使用するか判定"""
        return self.enabled and self.upload_mode == 'batch'