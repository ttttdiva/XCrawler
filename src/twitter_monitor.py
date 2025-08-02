import os
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import re
from pathlib import Path

from twscrape import API, Tweet
from dotenv import load_dotenv
import aiohttp


class TwitterMonitor:
    def __init__(self, config: dict, db_manager=None):
        self.config = config
        self.logger = logging.getLogger("EventMonitor.TwitterMonitor")
        self.api = API()
        self._accounts_initialized = False
        self._session = None
        self.db_manager = db_manager
        
    async def _initialize_accounts(self):
        """Twitter認証アカウントを初期化"""
        if self._accounts_initialized:
            return
            
        try:
            # 既存のアカウントをチェック
            existing_accounts = await self.api.pool.accounts_info()
            existing_usernames = {acc['username'] for acc in existing_accounts}
            
            total_accounts = 0
            
            # メインアカウント（オプション）
            main_token = os.getenv('TWITTER_AUTH_TOKEN')
            main_ct0 = os.getenv('TWITTER_CT0')
            
            if main_token and main_ct0 and main_token != "your_auth_token_here":
                if "twitter_main" not in existing_usernames:
                    cookies = {
                        "auth_token": main_token,
                        "ct0": main_ct0
                    }
                    await self.api.pool.add_account(
                        username="twitter_main",
                        password="dummy_password",
                        email="dummy@example.com",
                        email_password="dummy_email_password",
                        cookies=cookies
                    )
                    self.logger.info("Added main Twitter account")
                else:
                    self.logger.debug("Main Twitter account already exists")
                total_accounts += 1
            
            # 追加アカウント（レート制限対策）
            account_index = 1
            while True:
                token_key = f'TWITTER_ACCOUNT_{account_index}_TOKEN'
                ct0_key = f'TWITTER_ACCOUNT_{account_index}_CT0'
                
                token = os.getenv(token_key)
                ct0 = os.getenv(ct0_key)
                
                
                if not token or not ct0:
                    break
                    
                username = f"twitter_user_{account_index}"
                if username not in existing_usernames:
                    # Cookie文字列形式に変換
                    cookie_string = f"auth_token={token}; ct0={ct0}"
                    
                    await self.api.pool.add_account(
                        username=username,
                        password="dummy_password",
                        email=f"dummy{account_index}@example.com",
                        email_password="dummy_email_password",
                        cookies=cookie_string
                    )
                    self.logger.info(f"Added Twitter account {account_index}")
                else:
                    self.logger.debug(f"Twitter account {account_index} already exists")
                    
                total_accounts += 1
                account_index += 1
            
            # アカウントが1つも設定されていない場合はエラー
            if total_accounts == 0:
                raise ValueError("No Twitter accounts configured. Please set at least one account.")
            
            # すべてのアカウントでログイン
            await self.api.pool.login_all()
            self._accounts_initialized = True
            self.logger.info(f"Initialized {total_accounts} Twitter account(s)")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Twitter accounts: {e}")
            raise
    
    def _is_retweet(self, tweet: Tweet, username: str) -> bool:
        """リツイート/リポストかどうかを判定"""
        # 方法1: retweetedTweet属性をチェック
        if hasattr(tweet, 'retweetedTweet') and tweet.retweetedTweet is not None:
            self.logger.debug(f"Tweet {tweet.id} is a retweet (has retweetedTweet)")
            return True
        
        # 方法2: ユーザーIDが異なる場合
        if hasattr(tweet, 'user') and hasattr(tweet.user, 'id'):
            if str(tweet.user.username).lower() != username.lower():
                self.logger.debug(f"Tweet {tweet.id} is a retweet (different user)")
                return True
        
        # 方法3: URLからユーザー名を抽出して比較
        if hasattr(tweet, 'url'):
            url_match = re.search(r'twitter\.com/([^/]+)/status/', tweet.url)
            if url_match:
                url_username = url_match.group(1).lower()
                if url_username != username.lower():
                    self.logger.debug(f"Tweet {tweet.id} is a retweet (URL mismatch)")
                    return True
        
        return False
    
    async def download_tweet_images(self, tweet_data: Dict[str, Any]) -> List[str]:
        """ツイートの画像をダウンロード"""
        if not tweet_data.get('media') or not tweet_data['media']:
            return []
        
        # display_nameを使用、なければusernameをフォールバック
        display_name = tweet_data.get('display_name') or tweet_data.get('username', 'unknown')
        # ファイルシステムで使えない文字を置換
        safe_display_name = self._sanitize_filename(display_name)
        image_dir = Path('images') / safe_display_name
        image_dir.mkdir(parents=True, exist_ok=True)
        
        downloaded_paths = []
        
        # セッションの再利用
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession()
            
        for idx, image_url in enumerate(tweet_data['media']):
                try:
                    file_path = image_dir / f"{tweet_data['id']}_{idx}.jpg"
                    
                    # 既にダウンロード済みの場合はスキップ
                    if file_path.exists():
                        self.logger.debug(f"Image already exists: {file_path}")
                        downloaded_paths.append(str(file_path))
                        continue
                    
                    # 画像をダウンロード
                    async with self._session.get(image_url) as response:
                        if response.status == 200:
                            content = await response.read()
                            with open(file_path, 'wb') as f:
                                f.write(content)
                            self.logger.debug(f"Downloaded image: {file_path}")
                            downloaded_paths.append(str(file_path))
                        else:
                            self.logger.warning(f"Failed to download image: {image_url} (status: {response.status})")
                            
                except Exception as e:
                    self.logger.error(f"Error downloading image {image_url}: {e}")
        
        return downloaded_paths
    
    async def download_tweet_videos(self, tweet_data: Dict[str, Any]) -> List[str]:
        """ツイートの動画をダウンロード"""
        if not tweet_data.get('videos') or not tweet_data['videos']:
            return []
        
        # display_nameを使用、なければusernameをフォールバック
        display_name = tweet_data.get('display_name') or tweet_data.get('username', 'unknown')
        # ファイルシステムで使えない文字を置換
        safe_display_name = self._sanitize_filename(display_name)
        video_dir = Path('videos') / safe_display_name
        video_dir.mkdir(parents=True, exist_ok=True)
        
        downloaded_paths = []
        
        # セッションの再利用
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession()
            
        for idx, video_url in enumerate(tweet_data['videos']):
            try:
                # 動画ファイルの拡張子を決定
                extension = '.mp4'  # デフォルト
                if '.m3u8' in video_url:
                    extension = '.m3u8'
                elif '.gif' in video_url:
                    extension = '.gif'
                
                file_path = video_dir / f"{tweet_data['id']}_{idx}{extension}"
                
                # 既にダウンロード済みの場合はスキップ
                if file_path.exists():
                    self.logger.debug(f"Video already exists: {file_path}")
                    downloaded_paths.append(str(file_path))
                    continue
                
                # 動画をダウンロード
                async with self._session.get(video_url) as response:
                    if response.status == 200:
                        content = await response.read()
                        with open(file_path, 'wb') as f:
                            f.write(content)
                        self.logger.debug(f"Downloaded video: {file_path}")
                        downloaded_paths.append(str(file_path))
                    else:
                        self.logger.warning(f"Failed to download video: {video_url} (status: {response.status})")
                        
            except Exception as e:
                self.logger.error(f"Error downloading video {video_url}: {e}")
        
        return downloaded_paths
    
    async def get_user_tweets(self, username: str, days_lookback: int = 365, force_full_fetch: bool = False) -> List[Dict[str, Any]]:
        """指定ユーザーのツイートを取得（リツイート除く）"""
        await self._initialize_accounts()
        
        tweets = []
        since_date = datetime.now(timezone.utc) - timedelta(days=days_lookback)
        
        # データベース接続と既存データの確認
        from .database import DatabaseManager
        db_manager = DatabaseManager(self.config)
        
        # 既存のツイートIDセットを取得（重複チェック用）
        existing_tweet_ids = set()
        if db_manager:
            existing_tweet_ids = db_manager.get_existing_tweet_ids(username)
            self.logger.debug(f"Found {len(existing_tweet_ids)} existing tweets in database for @{username}")
        
        # データベースから該当ユーザーの最新ツイート日付とIDを取得
        latest_tweet_date = None
        latest_tweet_id = None
        
        if not force_full_fetch:
            latest_tweet_date = db_manager.get_latest_tweet_date(username)
            latest_tweet_id = db_manager.get_latest_tweet_id(username)
            
            if latest_tweet_date:
                # 最新ツイート日付以降のみ取得（効率化）
                since_date = latest_tweet_date
                self.logger.info(f"Found existing tweets for @{username}, fetching since {since_date.strftime('%Y-%m-%d %H:%M:%S')}")
                if latest_tweet_id:
                    self.logger.debug(f"Latest tweet ID: {latest_tweet_id}")
            else:
                self.logger.info(f"No existing tweets for @{username}, fetching since {since_date.strftime('%Y-%m-%d')}")
        else:
            self.logger.info(f"Force full fetch enabled for @{username}, fetching ALL tweets since {since_date.strftime('%Y-%m-%d')} with duplicate checking")
        
        try:
            # First, resolve username to user ID
            user = await self.api.user_by_login(username)
            if not user:
                self.logger.error(f"User @{username} not found or suspended")
                return []
            
            display_name = user.displayname  # display_nameを設定
            self.logger.info(f"Resolved @{username} to user ID: {user.id} (Name: {display_name})")
            self.logger.info(f"@{username} の総ツイート数: {user.statusesCount}")
            
            # Playwrightを使うべきか判定
            total_tweets = user.statusesCount
            playwright_threshold = self.config['tweet_settings'].get('playwright_threshold', 300)
            playwright_enabled = self.config['tweet_settings'].get('playwright', {}).get('enabled', True)
            
            # Playwrightを使用する場合の判定
            if playwright_enabled and force_full_fetch and total_tweets > playwright_threshold:
                self.logger.info(f"@{username}: 総ツイート数 {total_tweets} > 制限 {playwright_threshold}")
                self.logger.info("Playwrightモードで全ツイートを取得します")
                coverage = (playwright_threshold / total_tweets) * 100
                self.logger.info(f"twscrapeだけでは約 {coverage:.1f}% しか取得できません")
                try:
                    from .browser_fetcher import BrowserFetcher
                    
                    self.logger.info(f"Playwrightでツイート収集を開始: @{username}")
                    
                    fetcher = BrowserFetcher(self.config, self.db_manager, self)
                    tweet_ids = await fetcher.fetch_all_tweet_ids(username)
                    
                    self.logger.info(f"\n=== Playwright ID収集結果 ===")
                    self.logger.info(f"収集したツイートID数: {len(tweet_ids)}件")
                    self.logger.info(f"推定カバー率: {len(tweet_ids)/total_tweets*100:.1f}%")
                    self.logger.info(f"================================\n")
                    
                    # 収集したIDからtwscrapeでツイート詳細を取得
                    self.logger.info(f"twscrapeで詳細データを取得開始...")
                    detailed_tweets = []
                    failed_ids = []
                    
                    # バッチ処理でツイートデータを取得（並行処理に最適化）
                    batch_size = 10  # 並行処理のため小さめのバッチサイズに変更
                    for i in range(0, len(tweet_ids), batch_size):
                        batch_ids = tweet_ids[i:i+batch_size]
                        self.logger.info(f"バッチ {i//batch_size + 1}/{(len(tweet_ids) + batch_size - 1)//batch_size}: {len(batch_ids)}件のツイートを取得中...")
                        
                        # 並行処理用のタスクリストを作成
                        async def fetch_tweet_detail(tweet_id):
                            try:
                                tweet = await self.api.tweet_details(int(tweet_id))
                                if tweet:
                                    # リツイート判定
                                    if self._is_retweet(tweet, username):
                                        self.logger.debug(f"Skipping retweet: {tweet_id}")
                                        return None
                                    
                                    # ツイートデータを構築
                                    tweet_data = {
                                        'id': str(tweet.id),
                                        'text': tweet.rawContent,
                                        'date': tweet.date.isoformat(),
                                        'url': f"https://twitter.com/{username}/status/{tweet.id}",
                                        'username': username,
                                        'display_name': display_name,
                                        'media': [],
                                        'videos': []
                                    }
                                    
                                    # メディア（画像）URLを抽出
                                    if hasattr(tweet, 'media') and hasattr(tweet.media, 'photos'):
                                        tweet_data['media'] = [photo.url for photo in tweet.media.photos]
                                    
                                    # 動画URLを抽出
                                    if hasattr(tweet, 'media') and hasattr(tweet.media, 'videos'):
                                        for video in tweet.media.videos:
                                            # 最高画質のバリアントを選択
                                            best_variant = None
                                            best_bitrate = 0
                                            
                                            if hasattr(video, 'variants'):
                                                for variant in video.variants:
                                                    if hasattr(variant, 'bitrate') and variant.bitrate:
                                                        if variant.bitrate > best_bitrate:
                                                            best_bitrate = variant.bitrate
                                                            best_variant = variant
                                            
                                            if best_variant and hasattr(best_variant, 'url'):
                                                tweet_data['videos'].append(best_variant.url)
                                            elif hasattr(video, 'url'):
                                                tweet_data['videos'].append(video.url)
                                    
                                    return tweet_data
                                return None
                            except Exception as e:
                                self.logger.error(f"Failed to fetch tweet {tweet_id}: {e}")
                                return {'error': str(e), 'tweet_id': tweet_id}
                        
                        # すべてのタスクを並行実行
                        tasks = [fetch_tweet_detail(tweet_id) for tweet_id in batch_ids]
                        results = await asyncio.gather(*tasks)
                        
                        # 結果を処理
                        for result in results:
                            if result is None:
                                continue
                            elif isinstance(result, dict) and 'error' in result:
                                failed_ids.append(result['tweet_id'])
                            else:
                                detailed_tweets.append(result)
                        
                        # レート制限対策の待機（並行処理のため短縮）
                        if i + batch_size < len(tweet_ids):
                            await asyncio.sleep(0.3)
                    
                    self.logger.info(f"\n=== twscrape詳細取得結果 ===")
                    self.logger.info(f"成功: {len(detailed_tweets)}件")
                    self.logger.info(f"失敗: {len(failed_ids)}件") 
                    self.logger.info(f"最終取得率: {len(detailed_tweets)/total_tweets*100:.1f}%")
                    self.logger.info(f"================================\n")
                    
                    return detailed_tweets
                    
                except ImportError:
                    self.logger.error("Playwrightの読み込みに失敗しました")
                    self.logger.warning("twscrapeでの取得にフォールバックします")
                except Exception as e:
                    self.logger.error(f"Playwright実行中にエラーが発生: {e}")
                    self.logger.warning("twscrapeでの取得にフォールバックします")
            
            tweet_count = 0
            total_fetched = 0
            old_tweets_count = 0
            consecutive_old_tweets = 0
            max_consecutive_old = 20  # 連続して古いツイートが20個続いたら終了
            
            # APIからは必要な分だけ取得（日付でフィルタリング）
            self.logger.debug(f"Starting to fetch tweets")
            
            # twscrapeのkvパラメータで日付フィルタリングを試験的に実装
            kv = None
            if latest_tweet_date:
                # ISO形式の日時文字列でフィルタリング（試験的）
                kv = {"since_time": latest_tweet_date.isoformat()}
                self.logger.debug(f"Trying to filter tweets with kv parameter: {kv}")
            
            # force_full_fetchが有効な場合は制限を無視
            limit = -1  # デフォルトは無制限
            if not force_full_fetch:
                if latest_tweet_date and (datetime.now(timezone.utc) - latest_tweet_date).days < 7:
                    limit = 50  # 1週間以内に更新があった場合は50件まで
                    self.logger.debug(f"Recent update detected, limiting fetch to {limit} tweets")
                elif latest_tweet_date and (datetime.now(timezone.utc) - latest_tweet_date).days < 30:
                    limit = 100  # 1ヶ月以内の場合は100件まで
                    self.logger.debug(f"Recent month update detected, limiting fetch to {limit} tweets")
            else:
                # force_full_fetchの場合は明示的に大きな値を設定
                # Playwrightが無効の場合は閾値に関係なくtwscrapeで最大限取得
                if not playwright_enabled:
                    limit = 50000  # 5万件まで取得を試みる
                    self.logger.info(f"Playwright disabled, using twscrape with limit {limit} tweets")
                else:
                    limit = 50000  # 5万件まで取得を試みる
                    self.logger.info(f"Force full fetch enabled, setting limit to {limit} tweets")
                
                # force_full_fetchの場合、kvパラメータをクリア（日付フィルタを無効化）
                kv = None
                self.logger.info(f"Clearing date filters for complete fetch")
            
            # force_full_fetchの場合、アカウントプールの状態を定期的に確認
            check_interval = 500  # 500ツイートごとにチェック
            
            async for tweet in self.api.user_tweets(user.id, limit=limit, kv=kv):
                total_fetched += 1
                self.logger.debug(f"Tweet {total_fetched}: ID={tweet.id}, Date={tweet.date}, Username={getattr(tweet.user, 'username', 'N/A')}")
                
                # force_full_fetchの場合、定期的にアカウントプールの状態を確認
                if force_full_fetch and total_fetched % check_interval == 0:
                    try:
                        pool_stats = await self.api.pool.stats()
                        self.logger.info(f"Account pool stats after {total_fetched} tweets: {pool_stats}")
                    except:
                        pass
                
                # Tweet IDベースの早期終了（force_full_fetchが無効な場合のみ）
                if not force_full_fetch and latest_tweet_id and int(tweet.id) <= int(latest_tweet_id):
                    self.logger.debug(f"Reached known tweet {tweet.id}, stopping immediately")
                    break
                
                # 日付チェック - 古いツイートはスキップするが即座に終了はしない
                if tweet.date < since_date:
                    old_tweets_count += 1
                    consecutive_old_tweets += 1
                    self.logger.debug(f"Skipping old tweet: {tweet.id} (older than {days_lookback} days)")
                    
                    # force_full_fetchが無効な場合のみ、連続して古いツイートが続く場合に終了
                    if not force_full_fetch and consecutive_old_tweets >= max_consecutive_old:
                        self.logger.debug(f"Reached {max_consecutive_old} consecutive old tweets, stopping")
                        break
                    continue
                else:
                    consecutive_old_tweets = 0  # 新しいツイートが見つかったらリセット
                
                # リツイートをスキップ
                if self._is_retweet(tweet, username):
                    self.logger.debug(f"Skipping retweet: {tweet.id} (retweetedTweet: {hasattr(tweet, 'retweetedTweet') and tweet.retweetedTweet is not None})")
                    continue
                
                # 既存ツイートとの重複チェック（force_full_fetch時も実行）
                tweet_id_str = str(tweet.id)
                if tweet_id_str in existing_tweet_ids:
                    self.logger.debug(f"Skipping duplicate tweet: {tweet.id} (already in database)")
                    continue
                
                # ツイートデータを抽出
                tweet_data = {
                    'id': str(tweet.id),
                    'text': tweet.rawContent,
                    'date': tweet.date.isoformat(),
                    'url': f"https://twitter.com/{username}/status/{tweet.id}",
                    'username': username,  # Store the username for later use
                    'media': [],
                    'videos': []  # 動画URLを格納
                }
                
                # メディア（画像）URLを抽出
                if hasattr(tweet, 'media') and hasattr(tweet.media, 'photos'):
                    tweet_data['media'] = [photo.url for photo in tweet.media.photos]
                
                # 動画URLを抽出
                if hasattr(tweet, 'media') and hasattr(tweet.media, 'videos'):
                    for video in tweet.media.videos:
                        # 最高画質のバリアントを選択
                        best_variant = None
                        best_bitrate = 0
                        
                        if hasattr(video, 'variants'):
                            for variant in video.variants:
                                if hasattr(variant, 'bitrate') and variant.bitrate:
                                    if variant.bitrate > best_bitrate:
                                        best_bitrate = variant.bitrate
                                        best_variant = variant
                        
                        if best_variant and hasattr(best_variant, 'url'):
                            tweet_data['videos'].append(best_variant.url)
                        elif hasattr(video, 'url'):
                            # variantsがない場合は直接URLを使用
                            tweet_data['videos'].append(video.url)
                
                # force_full_fetchでかつ動画がない場合はスキップ
                if force_full_fetch and not tweet_data['videos']:
                    continue
                
                tweets.append(tweet_data)
                tweet_count += 1
                
                if tweet_count % 100 == 0:
                    self.logger.debug(f"Fetched {tweet_count} tweets so far...")
            
            # 重複を除去（ツイートIDでユニークにする）
            unique_tweets = []
            seen_ids = set()
            for tweet in tweets:
                if tweet['id'] not in seen_ids:
                    unique_tweets.append(tweet)
                    seen_ids.add(tweet['id'])
            
            duplicate_count = len(tweets) - len(unique_tweets)
            if duplicate_count > 0:
                self.logger.warning(f"Removed {duplicate_count} duplicate tweets from current fetch")
            
            self.logger.info(f"Fetched {len(unique_tweets)} unique tweets for @{username} (total examined: {total_fetched}, old tweets skipped: {old_tweets_count}, duplicates removed: {duplicate_count})")
            return unique_tweets
            
        except Exception as e:
            if "No account available" in str(e):
                self.logger.warning(f"Rate limit reached for @{username}: {e}")
                # レート制限エラーから次の利用可能時間を抽出
                next_available_match = re.search(r'next available at (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', str(e))
                if next_available_match:
                    next_time = next_available_match.group(1)
                    self.logger.info(f"Next available time: {next_time}")
            else:
                self.logger.error(f"Error fetching tweets for @{username}: {e}")
            
            # エラーが発生しても空のリストを返す（処理を継続）
            return tweets
    
    async def check_rate_limit_status(self):
        """レート制限の状態を確認"""
        try:
            # アカウントプールの状態を確認
            pool_stats = await self.api.pool.stats()
            self.logger.info(f"Twitter account pool stats: {pool_stats}")
            return pool_stats
        except Exception as e:
            self.logger.error(f"Failed to get rate limit status: {e}")
            return None
    
    def _sanitize_filename(self, name: str) -> str:
        """ファイル名として使えない文字を置換"""
        # Windowsで使えない文字を置換
        invalid_chars = '<>:"|?*\\/'  # バックスラッシュも含む
        for char in invalid_chars:
            name = name.replace(char, '_')
        # 先頭・末尾のスペースとピリオドを削除
        name = name.strip('. ')
        # 空の場合はデフォルト値
        return name or 'unknown'
    
    async def get_single_tweet(self, tweet_id: str) -> Optional[Dict[str, Any]]:
        """単一のツイートを取得"""
        await self._initialize_accounts()
        
        try:
            # ツイートIDで直接取得
            tweet = await self.api.tweet_details(int(tweet_id))
            if not tweet:
                self.logger.warning(f"Tweet {tweet_id} not found")
                return None
            
            # リツイート/リポストをチェック（ユーザー名が不明な場合は、URLから抽出）
            username_for_check = None
            if hasattr(tweet, 'url'):
                url_match = re.search(r'twitter\.com/([^/]+)/status/', tweet.url)
                if url_match:
                    username_for_check = url_match.group(1)
            
            if username_for_check and self._is_retweet(tweet, username_for_check):
                self.logger.debug(f"Tweet {tweet_id} is a retweet, skipping")
                return None
            
            # メディアを抽出
            images = []
            videos = []
            
            if tweet.media and tweet.media.photos:
                for photo in tweet.media.photos:
                    images.append(photo.url)
            
            if tweet.media and tweet.media.videos:
                for video in tweet.media.videos:
                    videos.append(video.bestVariant.url if video.bestVariant else None)
            
            # videosリストから None を除外
            videos = [v for v in videos if v]
            
            # ツイートデータを構築
            tweet_data = {
                'id': tweet.id,
                'username': tweet.user.username if tweet.user else 'unknown',
                'tweet_text': tweet.rawContent,
                'created_at': tweet.date.isoformat(),
                'media_urls': images + videos,
                'images': images,
                'videos': videos,
                'hashtags': [tag.text for tag in tweet.hashtags] if tweet.hashtags else [],
                'view_count': tweet.viewCount,
                'reply_count': tweet.replyCount,
                'retweet_count': tweet.retweetCount,
                'like_count': tweet.likeCount,
                'url': tweet.url
            }
            
            self.logger.info(f"Successfully fetched tweet {tweet_id} with {len(images)} images and {len(videos)} videos")
            return tweet_data
            
        except Exception as e:
            self.logger.error(f"Error fetching tweet {tweet_id}: {e}")
            return None
    
    async def cleanup(self):
        """リソースのクリーンアップ"""
        try:
            # aiohttp セッションのクローズ
            if self._session and not self._session.closed:
                await self._session.close()
                
            # 少し待機してセッションが完全に閉じられるのを待つ
            await asyncio.sleep(0.1)
            
            self.logger.debug("TwitterMonitor cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")