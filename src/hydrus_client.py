import aiohttp
import asyncio
import hashlib
import json
import logging
import os
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)
logger.info("HydrusClient module loaded with updated code")


class HydrusClient:
    """Hydrus Client APIとの連携を管理するクラス"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初期化
        
        Args:
            config: hydrus設定セクション
        """
        logger.info("HydrusClient.__init__ called")
        self.enabled = config.get('enabled', False)
        self.api_url = config.get('api_url', 'http://127.0.0.1:45869')
        # 環境変数を優先、なければconfig.yamlから取得
        self.access_key = os.environ.get('HYDRUS_ACCESS_KEY') or config.get('access_key')
        self.tag_service_key = config.get('tag_service_key', '6c6f63616c2074616773')  # "local tags"
        
        self.import_settings = config.get('import_settings', {})
        self.tag_settings = config.get('tag_settings', {})
        
        self.session: Optional[aiohttp.ClientSession] = None
        self._session_key: Optional[str] = None
        
        if self.enabled and not self.access_key:
            logger.warning("Hydrus連携が有効ですが、access_keyが設定されていません")
            self.enabled = False
    
    async def __aenter__(self):
        """非同期コンテキストマネージャーのエンター"""
        if self.enabled:
            self.session = aiohttp.ClientSession()
            await self._get_session_key()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """非同期コンテキストマネージャーのイグジット"""
        if self.session:
            await self.session.close()
    
    async def _get_session_key(self) -> Optional[str]:
        """セッションキーを取得（24時間有効）"""
        if not self.enabled:
            return None
            
        try:
            headers = {'Hydrus-Client-API-Access-Key': self.access_key}
            async with self.session.get(f"{self.api_url}/session_key", headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self._session_key = data.get('session_key')
                    logger.info("Hydrus APIセッションキーを取得しました")
                    return self._session_key
                else:
                    logger.error(f"セッションキー取得エラー: {resp.status}")
                    return None
        except Exception as e:
            logger.error(f"Hydrus API接続エラー: {e}")
            return None
    
    def _get_headers(self) -> Dict[str, str]:
        """APIリクエスト用のヘッダーを取得"""
        if self._session_key:
            return {'Hydrus-Client-API-Session-Key': self._session_key}
        else:
            return {'Hydrus-Client-API-Access-Key': self.access_key}
    
    async def import_file(self, file_path: Path) -> Optional[str]:
        """
        ファイルをHydrusにインポート
        
        Args:
            file_path: インポートするファイルのパス
            
        Returns:
            成功時はファイルのSHA256ハッシュ、失敗時はNone
        """
        if not self.enabled:
            return None
        
        # 動画ファイルの拡張子チェック
        video_extensions = ['.mp4', '.mov', '.avi', '.webm', '.mkv', '.flv', '.wmv', '.m3u8']
        if file_path.suffix.lower() in video_extensions:
            logger.info(f"動画ファイルはスキップします: {file_path}")
            return None
            
        try:
            # ファイルハッシュを計算
            file_hash = self._calculate_file_hash(file_path)
            
            # 既存チェック
            if self.import_settings.get('skip_existing', True):
                exists = await self._check_file_exists(file_hash)
                if exists:
                    logger.info(f"ファイルは既にHydrusに存在: {file_path}")
                    return file_hash
            
            # ファイルをインポート
            headers = self._get_headers()
            headers['Content-Type'] = 'application/octet-stream'
            
            # ファイルをバイナリデータとして読み込む
            with open(file_path, 'rb') as f:
                file_data = f.read()
            
            async with self.session.post(
                f"{self.api_url}/add_files/add_file",
                headers=headers,
                data=file_data
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    status = result.get('status')
                    logger.info(f"Import status for {file_path}: {status}")
                    if status in [1, 2]:  # 1=success, 2=already in db
                        if status == 2:
                            logger.info(f"ファイルは既にDBに存在: {file_path}")
                        else:
                            logger.info(f"ファイルをインポートしました: {file_path}")
                        return result.get('hash')
                    elif status == 3:  # 3=previously deleted
                        logger.warning(f"ファイルは以前削除されました。削除を解除して再インポートします: {file_path}")
                        file_hash = result.get('hash')
                        logger.debug(f"Previously deleted file hash: {file_hash}")
                        
                        # 削除を解除して再インポート
                        if await self._undelete_file(file_hash):
                            logger.info(f"削除解除成功。既存のタグを確認します: {file_hash}")
                            
                            # 削除解除後、既存のタグを確認（デバッグ用）
                            existing_tags = await self._get_file_tags(file_hash)
                            logger.info(f"既存のタグ数: {len(existing_tags) if existing_tags else 0}")
                            if existing_tags:
                                logger.debug(f"既存のタグ: {existing_tags}")
                                # title:タグの存在確認
                                title_tags = [tag for tag in existing_tags if tag.startswith('title:')]
                                if title_tags:
                                    logger.warning(f"既存のtitle:タグが見つかりました: {title_tags}")
                                else:
                                    logger.info("既存のtitle:タグは見つかりませんでした")
                            
                            return file_hash
                        else:
                            logger.error(f"削除解除に失敗: {file_path}")
                            return None
                    else:
                        logger.error(f"インポート失敗: {result}")
                        return None
                else:
                    logger.error(f"インポートAPIエラー: {resp.status}")
                    return None
                    
        except Exception as e:
            logger.error(f"ファイルインポートエラー: {e}")
            return None
    
    async def add_tags(self, file_hash: str, tags: List[str]) -> bool:
        """
        ファイルにタグを追加
        
        Args:
            file_hash: ファイルのSHA256ハッシュ
            tags: 追加するタグのリスト
            
        Returns:
            成功時True、失敗時False
        """
        if not self.enabled or not tags:
            return False
            
        try:
            logger.debug(f"タグ追加開始: {file_hash}")
            logger.debug(f"追加するタグ: {tags}")
            
            # title:タグの存在確認
            title_tags_to_add = [tag for tag in tags if tag.startswith('title:')]
            if title_tags_to_add:
                logger.info(f"title:タグを追加します: {title_tags_to_add[0][:100]}...")
            else:
                logger.warning("追加するタグにtitle:タグが含まれていません")
            
            headers = self._get_headers()
            headers['Content-Type'] = 'application/json'
            
            data = {
                'hashes': [file_hash],
                'service_keys_to_actions_to_tags': {
                    self.tag_service_key: {
                        '0': tags  # 0 = add action
                    }
                },
                'override_previously_deleted_mappings': True  # 削除されたタグマッピングを上書き
            }
            
            async with self.session.post(
                f"{self.api_url}/add_tags/add_tags",
                headers=headers,
                json=data
            ) as resp:
                if resp.status == 200:
                    logger.info(f"タグを追加しました: {len(tags)}個")
                    
                    # 追加後のタグを確認（デバッグ用）
                    updated_tags = await self._get_file_tags(file_hash)
                    if updated_tags is not None:
                        new_title_tags = [tag for tag in updated_tags if tag.startswith('title:')]
                        if new_title_tags:
                            logger.info(f"追加後のtitle:タグ: {new_title_tags[0][:100]}...")
                        else:
                            logger.error("タグ追加後もtitle:タグが見つかりません！")
                    
                    return True
                else:
                    logger.error(f"タグ追加APIエラー: {resp.status}")
                    error_text = await resp.text()
                    logger.error(f"エラー詳細: {error_text}")
                    return False
                    
        except Exception as e:
            logger.error(f"タグ追加エラー: {e}")
            return False
    
    async def import_tweet_images(self, tweet_data: Dict[str, Any], 
                                 local_media: List[str]) -> List[Tuple[str, str]]:
        """
        ツイートの画像をタグ付きでインポート
        
        Args:
            tweet_data: ツイートデータ
            local_media: ローカル画像パスのリスト
            
        Returns:
            インポートされたファイルの(パス, ハッシュ)のリスト
        """
        logger.info(f"import_tweet_images called for tweet {tweet_data.get('id')} with {len(local_media) if local_media else 0} images")
        if not self.enabled or not local_media:
            return []
            
        imported = []
        
        # ツイートURLを生成
        tweet_id = tweet_data.get('id')
        username = tweet_data.get('username')
        tweet_url = f"https://twitter.com/{username}/status/{tweet_id}" if tweet_id and username else None
        
        for image_path in local_media:
            file_path = Path(image_path)
            if not file_path.exists():
                logger.warning(f"画像ファイルが見つかりません: {image_path}")
                continue
            
            # 動画ファイルはスキップ
            video_extensions = ['.mp4', '.mov', '.avi', '.webm', '.mkv', '.flv', '.wmv', '.m3u8']
            if file_path.suffix.lower() in video_extensions:
                logger.info(f"動画ファイルはhydrusインポートをスキップします: {file_path}")
                continue
                
            # ファイルをインポート（または既存ファイルのハッシュを取得）
            logger.info(f"Importing file: {file_path}")
            file_hash = await self.import_file(file_path)
            logger.info(f"Import returned hash: {file_hash}")
            if not file_hash:
                logger.error(f"Failed to get file hash for: {file_path}")
                continue
                
            # ツイートURLをknown URLとして関連付け（常に実行）
            if tweet_url:
                logger.info(f"Associating URL to file: {tweet_url}")
                await self.associate_url(file_hash, tweet_url)
                
            # タグを生成（既存ファイルでも常に実行）
            logger.info(f"Generating tags for tweet {tweet_id}")
            tags = self._generate_tags(tweet_data)
            logger.info(f"Generated tags: {tags}")
            
            # タグを追加（既存ファイルでも常に実行）
            logger.info(f"Adding tags to file {file_hash}")
            if await self.add_tags(file_hash, tags):
                imported.append((image_path, file_hash))
                logger.info(f"Successfully added tags to file: {file_hash}")
            else:
                logger.error(f"Failed to add tags to file {file_hash}")
                
            # ツイート全文をnoteとして追加
            tweet_text = tweet_data.get('content') or tweet_data.get('text', '')
            if tweet_text:
                # URLを除去してクリーンなテキストにする
                cleaned_text = tweet_text.replace('\n', ' ').replace('\t', ' ').strip()
                # 連続する空白を1つに圧縮
                cleaned_text = ' '.join(cleaned_text.split())
                # t.coリンクを除去（TwitterのURL短縮）
                cleaned_text = re.sub(r'https?://t\.co/\S+', '', cleaned_text).strip()
                # 再度連続する空白を1つに圧縮
                cleaned_text = ' '.join(cleaned_text.split())
                
                if cleaned_text:
                    logger.info(f"Adding cleaned tweet text as note")
                    await self.add_note(file_hash, "twitter description", cleaned_text)
                
        return imported
    
    def _generate_tags(self, tweet_data: Dict[str, Any]) -> List[str]:
        """ツイートデータからタグを生成"""
        tags = []
        logger.debug(f"Generating tags for tweet {tweet_data.get('id')}: {tweet_data.get('content', '')[:50]}...")
        logger.debug(f"Tweet data keys: {list(tweet_data.keys())}")
        
        # 基本タグ
        tags.extend(self.tag_settings.get('base_tags', []))
        
        # クリエイター名タグ（usernameとdisplay_name両方）
        creator_format = self.tag_settings.get('creator_tag_format', 'creator:{name}')
        
        # display_nameでタグ追加
        display_name = tweet_data.get('display_name', '')
        if display_name:
            tags.append(creator_format.format(name=display_name))
        
        # usernameでもタグ追加（display_nameと異なる場合）
        username = tweet_data.get('username', '')
        if username and username != display_name:
            tags.append(creator_format.format(name=username))
        
        # タイトルタグ（ツイート本文）
        include_title = self.tag_settings.get('include_title_tag', True)
        logger.debug(f"include_title_tag setting: {include_title}")
        if include_title:
            # contentまたはtextフィールドを確認
            tweet_text = tweet_data.get('content') or tweet_data.get('text', '')
            logger.debug(f"Tweet text for title tag: {tweet_text[:100] if tweet_text else 'EMPTY'}")
            if tweet_text:
                # 改行やタブを半角スペースに置換し、前後の空白を削除
                cleaned_text = tweet_text.replace('\n', ' ').replace('\t', ' ').strip()
                # 連続する空白を1つに圧縮
                cleaned_text = ' '.join(cleaned_text.split())
                
                # t.coリンクを除去（TwitterのURL短縮）
                cleaned_text = re.sub(r'https?://t\.co/\S+', '', cleaned_text).strip()
                # 再度連続する空白を1つに圧縮
                cleaned_text = ' '.join(cleaned_text.split())
                
                if cleaned_text:
                    # 最初の行のみを取得（改行で分割して最初の要素）
                    first_line = cleaned_text.split(' ')[0:10]  # 最初の10単語まで
                    first_line_text = ' '.join(first_line)
                    if len(cleaned_text) > len(first_line_text):
                        first_line_text += "..."  # 省略記号を追加
                    
                    title_tag = f"title:{first_line_text}"
                    tags.append(title_tag)
                    logger.debug(f"Added title tag: {title_tag}")
                else:
                    logger.warning("Cleaned text is empty after processing")
            else:
                logger.warning(f"No content/text found in tweet data for tweet {tweet_data.get('id')}")
        
        # 日付タグ（config.yamlで無効化されていない場合のみ）
        if self.tag_settings.get('include_date_tag', False):
            date_format = self.tag_settings.get('date_tag_format', 'date:{date}')
            created_at = tweet_data.get('created_at')
            if created_at:
                if isinstance(created_at, str):
                    try:
                        dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        date_str = dt.strftime('%Y-%m-%d')
                        tags.append(date_format.format(date=date_str))
                    except:
                        pass
        
        # ツイートURLタグは削除（known URLsとして関連付けるため）
        
        # イベント関連情報（event_infoがある場合）
        event_info = tweet_data.get('event_info', {})
        
        # イベント名タグ
        event_format = self.tag_settings.get('event_tag_format', 'event:{name}')
        detected_events = event_info.get('detected_events', [])
        for event in detected_events:
            if event:
                tags.append(event_format.format(name=event))
        
        # 検出されたキーワード
        if self.tag_settings.get('include_detected_keywords', True):
            keywords = event_info.get('detected_keywords', [])
            for keyword in keywords:
                if keyword:
                    tags.append(f"keyword:{keyword}")
        
        # 重複を削除
        unique_tags = list(set(tags))
        # タグ数をログに記録（デバッグ用）
        if unique_tags:
            logger.info(f"Generated {len(unique_tags)} tags for tweet")
            logger.info(f"All tags: {unique_tags}")
            # title:タグが含まれているかチェック
            title_tags = [tag for tag in unique_tags if tag.startswith('title:')]
            if title_tags:
                logger.info(f"Title tag included: {title_tags[0][:100]}...")
            else:
                logger.warning("No title tag generated for this tweet")
        return unique_tags
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """ファイルのSHA256ハッシュを計算"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    async def _check_file_exists(self, file_hash: str) -> bool:
        """ファイルがHydrusに既に存在するかチェック"""
        try:
            headers = self._get_headers()
            params = {'hash': file_hash}
            
            async with self.session.get(
                f"{self.api_url}/get_files/file_metadata",
                headers=headers,
                params=params
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('metadata'):
                        # メタデータが存在する場合、実際にローカルに存在するかチェック
                        metadata = data['metadata'][0]
                        is_local = metadata.get('is_local', False)
                        if not is_local:
                            logger.debug(f"ファイルメタデータは存在するが、ローカルには存在しない: {file_hash}")
                            return False  # ローカルに存在しない場合は再インポート
                        return True
                    return False
                else:
                    return False
        except:
            return False
    
    async def _undelete_file(self, file_hash: str) -> bool:
        """削除されたファイルを復元"""
        try:
            headers = self._get_headers()
            headers['Content-Type'] = 'application/json'
            
            data = {
                'hashes': [file_hash]
            }
            
            async with self.session.post(
                f"{self.api_url}/add_files/undelete_files",
                headers=headers,
                json=data
            ) as resp:
                if resp.status == 200:
                    logger.info(f"ファイルの削除を解除しました: {file_hash}")
                    return True
                else:
                    logger.error(f"削除解除APIエラー: {resp.status}")
                    return False
        except Exception as e:
            logger.error(f"削除解除エラー: {e}")
            return False
    
    async def _get_file_tags(self, file_hash: str) -> Optional[List[str]]:
        """
        ファイルの既存タグを取得（デバッグ用）
        
        Args:
            file_hash: ファイルのSHA256ハッシュ
            
        Returns:
            タグのリスト、失敗時はNone
        """
        try:
            headers = self._get_headers()
            params = {'hash': file_hash}
            
            async with self.session.get(
                f"{self.api_url}/get_files/file_metadata",
                headers=headers,
                params=params
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('metadata'):
                        metadata = data['metadata'][0]
                        # サービスキーからタグを取得
                        service_keys_to_tags = metadata.get('service_keys_to_statuses_to_display_tags', {})
                        all_tags = []
                        
                        # local tagsサービスのタグを取得
                        if self.tag_service_key in service_keys_to_tags:
                            tag_data = service_keys_to_tags[self.tag_service_key]
                            # 現在のタグ（status 0）を取得
                            current_tags = tag_data.get('0', [])
                            all_tags.extend(current_tags)
                        
                        return all_tags
                    return []
                else:
                    logger.error(f"タグ取得APIエラー: {resp.status}")
                    return None
        except Exception as e:
            logger.error(f"タグ取得エラー: {e}")
            return None
    
    async def add_note(self, file_hash: str, note_name: str, note_text: str) -> bool:
        """
        ファイルにnoteを追加
        
        Args:
            file_hash: ファイルのSHA256ハッシュ
            note_name: noteの名前
            note_text: noteの内容
            
        Returns:
            成功時True、失敗時False
        """
        if not self.enabled or not note_text:
            return False
            
        try:
            headers = self._get_headers()
            headers['Content-Type'] = 'application/json'
            
            data = {
                'hash': file_hash,
                'notes': {
                    note_name: note_text
                }
            }
            
            async with self.session.post(
                f"{self.api_url}/add_notes/set_notes",
                headers=headers,
                json=data
            ) as resp:
                if resp.status == 200:
                    logger.info(f"noteを追加しました: {note_name}")
                    return True
                else:
                    logger.error(f"note追加APIエラー: {resp.status}")
                    error_text = await resp.text()
                    logger.error(f"エラー詳細: {error_text}")
                    return False
                    
        except Exception as e:
            logger.error(f"note追加エラー: {e}")
            return False
    
    async def associate_url(self, file_hash: str, url: str) -> bool:
        """
        URLをファイルのknown URLとして関連付け
        
        Args:
            file_hash: ファイルのSHA256ハッシュ
            url: 関連付けるURL
            
        Returns:
            成功時True、失敗時False
        """
        if not self.enabled:
            return False
            
        try:
            headers = self._get_headers()
            headers['Content-Type'] = 'application/json'
            
            data = {
                'hash': file_hash,
                'url_to_add': url
            }
            
            async with self.session.post(
                f"{self.api_url}/add_urls/associate_url",
                headers=headers,
                json=data
            ) as resp:
                if resp.status == 200:
                    logger.info(f"URLを関連付けました: {url}")
                    return True
                else:
                    logger.error(f"URL関連付けAPIエラー: {resp.status}")
                    return False
                    
        except Exception as e:
            logger.error(f"URL関連付けエラー: {e}")
            return False