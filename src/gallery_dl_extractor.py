#!/usr/bin/env python3
"""
gallery-dlを使用したメディア付きツイート取得
twscrapeの補完として全メディアツイートを取得
"""

import sys
import json
import subprocess
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional


class GalleryDLExtractor:
    """gallery-dlを使用してメディア付きツイートを取得"""
    
    def __init__(self, config: dict, event_detector=None):
        self.config = config
        self.logger = logging.getLogger("EventMonitor.GalleryDL")
        self.event_detector = event_detector
        
        # Cookie設定（ローテーション対応）
        from .gallery_dl_cookie_rotator import GalleryDLCookieRotator
        self.cookie_rotator = GalleryDLCookieRotator()
        
        # デフォルトCookie（フォールバック用）
        self.default_cookie_file = Path(config.get('twitter', {}).get('cookie_file', 'cookies/x.com_cookies.txt'))
        
        # メディア保存先
        self.media_dir = Path(config.get('media', {}).get('save_dir', 'data/media'))
        
        # ラッパースクリプトのパス
        self.wrapper_path = Path(__file__).parent / 'gallery_dl_wrapper.py'
        
    def fetch_media_tweets(self, username: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        指定ユーザーのメディア付きツイートを取得
        
        Args:
            username: Twitter username
            limit: 取得件数制限（Noneで全件）
            
        Returns:
            ツイート情報のリスト
        """
        url = f"https://x.com/{username}/media"
        
        # Cookieファイルを取得（ローテーション）
        cookie_file = self.cookie_rotator.get_next_cookie()
        if not cookie_file:
            cookie_file = self.default_cookie_file
            self.logger.warning("No cookie available for rotation, using default")
        
        # gallery-dlコマンドを構築（シンプルな配列で）
        cmd = [
            sys.executable,
            str(self.wrapper_path),
            '--cookies',
            str(cookie_file),
            '-q',  # Quietモード（プログレス表示を抑制）
            '-j'  # JSON出力
        ]
        
        if limit:
            cmd.append('--range')
            cmd.append(f'1-{limit}')
        
        cmd.append(url)
        
        self.logger.info(f"Fetching media tweets for @{username} (limit: {limit or 'all'})")
        
        try:
            # gallery-dl実行（標準エラー出力を破棄）
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,  # エラー出力を破棄
                text=True,
                timeout=300  # 5分のタイムアウト
            )
            
            if result.returncode != 0:
                self.logger.error(f"gallery-dl error: returncode={result.returncode}")
                return []
            
            # JSON出力をパース
            tweets = []
            tweet_dict = {}  # ツイートIDごとにまとめる
            
            output = result.stdout.strip()
            self.logger.debug(f"gallery-dl output length: {len(output)} chars")
            
            # 出力全体を1つの大きなJSON配列として解析
            if output:
                try:
                    # gallery-dlは複数の配列を出力するが、外側の配列として解析
                    # 最初に全体をJSON配列として解析を試みる
                    if output.startswith('['):
                        # 全体を1つのJSON配列として解析
                        all_items = json.loads(output)
                        
                        # 各アイテムを処理
                        for item in all_items:
                            if isinstance(item, list) and len(item) >= 2:
                                item_type = item[0]
                                item_data = item[1]
                                
                                # タイプ2: ツイート情報、タイプ3: メディアURL
                                if item_type == 2 and isinstance(item_data, dict):
                                    # ツイート情報を抽出
                                    tweet_info = self._extract_tweet_info(item_data)
                                    if tweet_info:
                                        tweet_id = tweet_info['id']
                                        if tweet_id not in tweet_dict:
                                            tweet_dict[tweet_id] = tweet_info
                                            self.logger.debug(f"Found tweet {tweet_id}: {tweet_info.get('text', '')[:50]}...")
                                elif item_type == 3 and len(item) >= 3:
                                    # メディアURL情報（URLとメタデータ）
                                    media_url = item[1]
                                    media_data = item[2] if len(item) > 2 else {}
                                    
                                    if isinstance(media_data, dict):
                                        tweet_id = str(media_data.get('tweet_id', ''))
                                        if tweet_id and tweet_id in tweet_dict:
                                            # 既存のツイートにメディアを追加
                                            if media_url not in tweet_dict[tweet_id]['media']:
                                                tweet_dict[tweet_id]['media'].append(media_url)
                                                self.logger.debug(f"Added media to tweet {tweet_id}: {media_url}")
                except json.JSONDecodeError as e:
                    self.logger.error(f"Failed to parse gallery-dl JSON output: {e}")
                    self.logger.debug(f"Output preview: {output[:500]}...")
                    return []
            
            # 辞書から値を取り出してリストに変換
            tweets = list(tweet_dict.values())
            
            self.logger.info(f"Retrieved {len(tweets)} media tweets for @{username}")
            
            # デバッグ用：最初の数件のツイートを表示
            for i, tweet in enumerate(tweets[:3]):
                self.logger.debug(f"Tweet {i+1}: ID={tweet['id']}, Text={tweet.get('text', '')[:50]}..., Media count={len(tweet.get('media', []))}")
            
            return tweets
            
        except subprocess.TimeoutExpired:
            self.logger.error(f"Timeout fetching tweets for @{username}")
            return []
        except Exception as e:
            self.logger.error(f"Error fetching tweets: {e}")
            return []
    
    def _extract_tweet_info(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """gallery-dlのデータからツイート情報を抽出"""
        
        try:
            # ツイートIDが必須
            tweet_id = data.get('tweet_id')
            if not tweet_id:
                return None
            
            # 日付フォーマット変換
            date_str = data.get('date', '')
            if date_str:
                # "2025-08-03 05:40:13" -> ISO format
                dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                date_iso = dt.isoformat() + 'Z'
            else:
                date_iso = datetime.now().isoformat() + 'Z'
            
            # ユーザー情報
            user_info = data.get('user', {})
            username = user_info.get('name', 'unknown')  # 'name'がユーザー名
            display_name = user_info.get('nick', username)  # 'nick'が表示名
            
            # メディアURL
            media_url = data.get('url', '')
            
            # ツイート情報を構築
            tweet = {
                'id': str(tweet_id),
                'username': username,
                'display_name': display_name,
                'text': data.get('content', ''),
                'date': date_iso,
                'url': f"https://x.com/{username}/status/{tweet_id}",
                'media': [media_url] if media_url else [],
                'source': 'gallery-dl',  # 取得元を記録
                
                # エンゲージメント情報
                'favorite_count': data.get('favorite_count', 0),
                'retweet_count': data.get('retweet_count', 0),
                'reply_count': data.get('reply_count', 0),
                'quote_count': data.get('quote_count', 0),
            }
            
            return tweet
            
        except Exception as e:
            self.logger.error(f"Error extracting tweet info: {e}")
            return None
    
    def download_media(self, username: str, output_dir: Optional[Path] = None, move_to_images: bool = True) -> bool:
        """
        メディアファイルを実際にダウンロード（一時的に保存して後で削除）
        
        Args:
            username: Twitter username
            output_dir: 出力ディレクトリ（指定しない場合はデフォルト）
            
        Returns:
            成功/失敗
        """
        if output_dir is None:
            output_dir = self.media_dir
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        url = f"https://x.com/{username}/media"
        
        # Cookieファイルを取得（ローテーション）
        cookie_file = self.cookie_rotator.get_next_cookie()
        if not cookie_file:
            cookie_file = self.default_cookie_file
        
        # gallery-dlコマンドを構築（ダウンロード用）
        cmd = [
            sys.executable,
            str(self.wrapper_path),
            '--cookies', str(cookie_file),
            '-d', str(output_dir),  # 出力先ディレクトリ
            url
        ]
        
        self.logger.info(f"Downloading media for @{username} to {output_dir}")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600  # 1時間のタイムアウト
            )
            
            if result.returncode == 0:
                # ダウンロードしたファイルを確認
                downloaded_files = []
                if output_dir.exists():
                    for file in output_dir.rglob('*'):
                        if file.is_file():
                            downloaded_files.append(file)
                
                self.logger.info(f"Successfully downloaded {len(downloaded_files)} files for @{username}")
                
                # ファイルリストをログ出力（デバッグ用）
                if downloaded_files:
                    self.logger.debug(f"Downloaded files: {[str(f.relative_to(output_dir)) for f in downloaded_files[:10]]}")
                
                # imagesディレクトリに移動
                if move_to_images and downloaded_files:
                    self._move_to_images_dir(downloaded_files, username)
                
                # ダウンロードしたファイルを削除
                self._cleanup_media_dir()
                
                return True
            else:
                self.logger.error(f"Download failed: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error(f"Timeout downloading media for @{username}")
            return False
        except Exception as e:
            self.logger.error(f"Error downloading media: {e}")
            return False
    
    def download_media_for_tweets(self, username: str, tweet_ids: List[str], output_dir: Optional[Path] = None, move_to_images: bool = True) -> Dict[str, List[str]]:
        """
        特定のツイートIDのメディアのみをダウンロード
        
        Args:
            username: Twitter username
            tweet_ids: ダウンロード対象のツイートIDリスト
            output_dir: 出力ディレクトリ（指定しない場合はデフォルト）
            
        Returns:
            ツイートIDごとのメディアファイルパスの辞書
        """
        if not tweet_ids:
            self.logger.info("No tweet IDs provided for download")
            return {}
        
        if output_dir is None:
            output_dir = self.media_dir
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Cookieファイルを取得（ローテーション）
        cookie_file = self.cookie_rotator.get_next_cookie()
        if not cookie_file:
            cookie_file = self.default_cookie_file
        
        # 一時的なURLリストファイルを作成
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as url_file:
            for tweet_id in tweet_ids:
                url = f"https://x.com/{username}/status/{tweet_id}"
                url_file.write(url + '\n')
            url_file_path = url_file.name
        
        # ダウンロード前のファイルリストを取得
        existing_files = set()
        if output_dir.exists():
            for file in output_dir.rglob('*'):
                if file.is_file():
                    existing_files.add(file)
        
        try:
            # gallery-dlコマンドを構築（バッチダウンロード用）
            cmd = [
                sys.executable,
                str(self.wrapper_path),
                '--cookies', str(cookie_file),
                '-d', str(output_dir),  # 出力先ディレクトリ
                '-q',  # Quietモード
                '--input-file', url_file_path  # URLリストファイル
            ]
            
            self.logger.info(f"Downloading media for {len(tweet_ids)} tweets in batch mode")
            
            # gallery-dlを一度だけ実行（すべてのURLを処理）
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=max(60, len(tweet_ids) * 10)  # 最低60秒、ツイート数×10秒
            )
            
            if result.returncode != 0:
                self.logger.warning(f"gallery-dl batch download had issues: {result.stderr}")
            
            # ダウンロード後のファイルリストを取得
            new_files_by_tweet = {}
            if output_dir.exists():
                # gallery-dlはtwitter/username/tweet_id_*.* 形式でファイルを保存
                for file in output_dir.rglob('*'):
                    if file.is_file() and file not in existing_files:
                        # ファイル名からツイートIDを抽出
                        filename = file.name
                        # tweet_id_番号.拡張子 形式
                        if '_' in filename:
                            tweet_id_part = filename.split('_')[0]
                            if tweet_id_part in tweet_ids:
                                if tweet_id_part not in new_files_by_tweet:
                                    new_files_by_tweet[tweet_id_part] = []
                                new_files_by_tweet[tweet_id_part].append(file)
            
            # ログ出力
            total_files = sum(len(files) for files in new_files_by_tweet.values())
            self.logger.info(f"Successfully downloaded {total_files} files for {len(new_files_by_tweet)} tweets")
            
            for tweet_id, files in new_files_by_tweet.items():
                self.logger.debug(f"Tweet {tweet_id}: {len(files)} files")
            
            # すべてのダウンロードされたファイルを収集
            all_downloaded_files = []
            for files in new_files_by_tweet.values():
                all_downloaded_files.extend(files)
            
            tweet_media_paths = new_files_by_tweet
            
        except subprocess.TimeoutExpired:
            self.logger.error(f"Timeout downloading media batch")
            tweet_media_paths = {}
            all_downloaded_files = []
        except Exception as e:
            self.logger.error(f"Error downloading media batch: {e}")
            tweet_media_paths = {}
            all_downloaded_files = []
        finally:
            # 一時ファイルを削除
            try:
                import os
                os.unlink(url_file_path)
            except:
                pass
        
        # imagesディレクトリに移動し、最終パスを更新
        final_tweet_media_paths = {}
        if move_to_images and all_downloaded_files:
            moved_paths = self._move_to_images_dir_with_mapping(all_downloaded_files, username)
            
            # ツイートIDごとに最終パスを更新
            for tweet_id, original_files in tweet_media_paths.items():
                final_paths = []
                for orig_file in original_files:
                    if orig_file in moved_paths:
                        final_paths.append(str(moved_paths[orig_file]))
                if final_paths:
                    final_tweet_media_paths[tweet_id] = final_paths
        
        # ダウンロードしたファイルを削除
        self._cleanup_media_dir()
        
        return final_tweet_media_paths
    
    def _move_to_images_dir(self, files: List[Path], username: str):
        """ダウンロードしたファイルをimagesディレクトリに移動"""
        try:
            # imagesディレクトリを作成
            images_dir = Path('images') / username
            images_dir.mkdir(parents=True, exist_ok=True)
            
            moved_count = 0
            for src_file in files:
                # ファイル名を取得（ツイートID_番号.拡張子）
                filename = src_file.name
                
                # 移動先パス
                dest_file = images_dir / filename
                
                # 既に存在する場合はスキップ
                if dest_file.exists():
                    self.logger.debug(f"File already exists in images: {dest_file}")
                    continue
                
                # ファイルを移動（コピーして元を削除）
                shutil.copy2(src_file, dest_file)
                moved_count += 1
                self.logger.debug(f"Moved {src_file} to {dest_file}")
            
            self.logger.info(f"Moved {moved_count} files to images/{username}/")
            
        except Exception as e:
            self.logger.error(f"Failed to move files to images directory: {e}")
    
    def _move_to_images_dir_with_mapping(self, files: List[Path], username: str) -> Dict[Path, Path]:
        """ダウンロードしたファイルをimagesディレクトリに移動し、マッピングを返す"""
        mapping = {}
        try:
            # imagesディレクトリを作成
            images_dir = Path('images') / username
            images_dir.mkdir(parents=True, exist_ok=True)
            
            for src_file in files:
                # ファイル名を取得（ツイートID_番号.拡張子）
                filename = src_file.name
                
                # 移動先パス
                dest_file = images_dir / filename
                
                # 既に存在する場合は既存ファイルをマッピング
                if dest_file.exists():
                    self.logger.debug(f"File already exists in images: {dest_file}")
                    mapping[src_file] = dest_file
                else:
                    # ファイルを移動（コピーして元を削除）
                    shutil.copy2(src_file, dest_file)
                    mapping[src_file] = dest_file
                    self.logger.debug(f"Moved {src_file} to {dest_file}")
            
            self.logger.info(f"Processed {len(mapping)} files to images/{username}/")
            
        except Exception as e:
            self.logger.error(f"Failed to move files to images directory: {e}")
        
        return mapping
    
    def _cleanup_media_dir(self):
        """メディアディレクトリを削除"""
        try:
            if self.media_dir.exists():
                shutil.rmtree(self.media_dir)
                self.logger.info(f"Cleaned up media directory: {self.media_dir}")
            else:
                self.logger.debug(f"Media directory does not exist: {self.media_dir}")
        except Exception as e:
            self.logger.error(f"Failed to cleanup media directory: {e}", exc_info=True)
    
    def merge_with_twscrape(self, gallery_tweets: List[Dict], twscrape_tweets: List[Dict]) -> List[Dict]:
        """
        gallery-dlとtwscrapeのツイートをマージ
        
        Args:
            gallery_tweets: gallery-dlで取得したツイート
            twscrape_tweets: twscrapeで取得したツイート
            
        Returns:
            マージされたツイートリスト（重複排除済み）
        """
        # ツイートIDをキーにした辞書を作成
        merged = {}
        
        # twscrapeのツイートを優先（より詳細な情報を持つため）
        for tweet in twscrape_tweets:
            merged[tweet['id']] = tweet
        
        # gallery-dlのツイートを追加（twscrapeにないもののみ）
        for tweet in gallery_tweets:
            if tweet['id'] not in merged:
                merged[tweet['id']] = tweet
        
        # 日付でソート（新しい順）
        sorted_tweets = sorted(
            merged.values(),
            key=lambda x: x['date'],
            reverse=True
        )
        
        self.logger.info(
            f"Merged tweets: {len(twscrape_tweets)} from twscrape, "
            f"{len(gallery_tweets)} from gallery-dl → {len(sorted_tweets)} total"
        )
        
        return sorted_tweets
    
    async def fetch_and_analyze_tweets(self, username: str, limit: Optional[int] = None) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        gallery-dlでツイートを取得してイベント判定も実行
        
        Args:
            username: Twitter username
            limit: 取得件数制限（Noneで全件）
            
        Returns:
            (全ツイート, イベント関連ツイート)のタプル
        """
        # gallery-dlでツイートを取得
        tweets = self.fetch_media_tweets(username, limit)
        
        if not tweets:
            self.logger.info(f"No tweets fetched for @{username}")
            return [], []
        
        # イベント判定が設定されていて有効な場合のみ実行
        event_tweets = []
        if self.event_detector and self.event_detector.enabled:
            self.logger.info(f"Running event detection on {len(tweets)} tweets from @{username}")
            event_tweets = await self.event_detector.detect_event_tweets(tweets)
            self.logger.info(f"Found {len(event_tweets)} event-related tweets for @{username}")
        else:
            self.logger.info("Event detection not available or disabled")
        
        return tweets, event_tweets