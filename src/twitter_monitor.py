import os
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import re
from pathlib import Path
import time

from twscrape import API, Tweet
from dotenv import load_dotenv
import aiohttp

# twscrapeの200件制限を回避するためのパッチ
import httpx
from twscrape.models import Tweet as TweetModel, to_old_rep, _write_dump
from typing import Generator

def parse_tweets_unlimited(rep: httpx.Response, limit: int = -1) -> Generator[TweetModel, None, None]:
    """
    修正版parse_tweets関数 - 200件制限を回避
    """
    res = rep if isinstance(rep, dict) else rep.json()
    obj = to_old_rep(res)
    
    ids = set()
    for x in obj["tweets"].values():
        # limitチェックを無効化（全ツイート返却）
        try:
            tmp = TweetModel.parse(x, obj)
            if tmp.id not in ids:
                ids.add(tmp.id)
                yield tmp
        except Exception as e:
            _write_dump("tweet", e, x, obj)
            continue

# モンキーパッチ適用
import twscrape.models
import twscrape.api as twscrape_api
twscrape.models.parse_tweets = parse_tweets_unlimited
twscrape_api.parse_tweets = parse_tweets_unlimited

# twscrapeのAsyncClientにタイムアウトを設定するパッチ
import twscrape.account
from httpx import AsyncClient, AsyncHTTPTransport

original_make_client = twscrape.account.Account.make_client

def make_client_with_timeout(self, proxy: str | None = None) -> AsyncClient:
    """タイムアウトを設定したAsyncClientを作成"""
    proxies = [proxy, os.getenv("TWS_PROXY"), self.proxy]
    proxies = [x for x in proxies if x is not None]
    proxy = proxies[0] if proxies else None
    
    # タイムアウトを設定（180秒に延長）
    transport = AsyncHTTPTransport(retries=2)
    client = AsyncClient(
        proxy=proxy, 
        follow_redirects=True, 
        transport=transport,
        timeout=httpx.Timeout(180.0, connect=30.0)  # 180秒のread timeout、30秒のconnect timeout
    )
    
    # saved from previous usage
    client.cookies.update(self.cookies)
    client.headers.update(self.headers)
    
    # default settings
    client.headers["user-agent"] = self.user_agent
    client.headers["content-type"] = "application/json"
    client.headers["authorization"] = twscrape.account.TOKEN
    client.headers["x-twitter-active-user"] = "yes"
    client.headers["x-twitter-client-language"] = "en"
    
    if "ct0" in client.cookies:
        client.headers["x-csrf-token"] = client.cookies["ct0"]
    
    return client

# モンキーパッチを適用
twscrape.account.Account.make_client = make_client_with_timeout


class TwitterMonitor:
    def __init__(self, config: dict, db_manager=None, event_detector=None):
        self.config = config
        self.logger = logging.getLogger("EventMonitor.TwitterMonitor")
        self.api = API()
        self._accounts_initialized = False
        self._session = None
        self.db_manager = db_manager
        self._timeout_seconds = 300  # 5分のタイムアウト
        
        # リトライカウンタを初期化
        self._tweet_retry_count = 0
        self._http_retry_count = 0
        
        # gallery-dl extractorを初期化
        from .gallery_dl_extractor import GalleryDLExtractor
        self.gallery_dl_extractor = GalleryDLExtractor(config, event_detector)
    
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
                    cookies = f"auth_token={main_token}; ct0={main_ct0}"
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
                    # Cookie辞書形式で統一
                    cookies = f"auth_token={token}; ct0={ct0}"
                    
                    await self.api.pool.add_account(
                        username=username,
                        password="dummy_password",
                        email=f"dummy{account_index}@example.com",
                        email_password="dummy_email_password",
                        cookies=cookies
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
            
            # ログイン後、実際に利用可能なアカウントがあるか確認
            try:
                pool_stats = await self.api.pool.stats()
                self.logger.info(f"Initial pool stats: {pool_stats}")
                
                # 各アカウントの状態を確認（簡易的なテスト）
                accounts_info = await self.api.pool.accounts_info()
                active_count = sum(1 for acc in accounts_info if acc.get('active', False))
                available_count = sum(1 for acc in accounts_info 
                                     if acc.get('active', False) 
                                     and acc.get('locks', {}).get('UserTweets', 0) < time.time())
                self.logger.info(f"Active accounts: {active_count}/{len(accounts_info)}, Available for UserTweets: {available_count}")
                
            except Exception as e:
                self.logger.warning(f"Could not verify account status: {e}")
            
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
        """gallery-dlが全メディアを処理するため、この関数は不要"""
        return []
    
    async def download_tweet_videos(self, tweet_data: Dict[str, Any]) -> List[str]:
        """gallery-dlが全メディアを処理するため、この関数は不要"""
        return []
    
    async def get_user_tweets_with_gallery_dl_first(self, username: str, days_lookback: int = 365, force_full_fetch: bool = False, event_detection_enabled: bool = True) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        gallery-dl優先でツイートを取得
        
        Args:
            username: Twitter username
            days_lookback: 過去何日分を取得するか
            force_full_fetch: 強制的に全件取得するか
            event_detection_enabled: このアカウントでイベント検知を行うか
        
        Returns:
            (全ツイート, イベント関連ツイート)のタプル
        """
        all_tweets = []
        all_event_tweets = []
        
        # Gallery-dlの有効性をチェック
        gallery_dl_enabled = self.config.get('tweet_settings', {}).get('gallery_dl', {}).get('enabled', True)
        twscrape_enabled = self.config.get('tweet_settings', {}).get('twscrape', {}).get('enabled', True)
        
        # DB から今回のクロール実行前の最新ツイート日時を記録（twscrape用）
        pre_crawl_latest_date = None
        pre_crawl_latest_id = None
        if self.db_manager:
            pre_crawl_latest_date = self.db_manager.get_latest_tweet_date(username)
            pre_crawl_latest_id = self.db_manager.get_latest_tweet_id(username)
        
        # 1. Gallery-dlでメディア付きツイートを優先取得
        if gallery_dl_enabled:
            self.logger.info(f"Step 1: Fetching media tweets with gallery-dl for @{username}")
            try:
                gallery_tweets, gallery_event_tweets = await self.gallery_dl_extractor.fetch_and_analyze_tweets(username, event_detection_enabled=event_detection_enabled)
                
                if gallery_tweets:
                    all_tweets.extend(gallery_tweets)
                    self.logger.info(f"Gallery-dl retrieved {len(gallery_tweets)} media tweets for @{username}")
                    
                if gallery_event_tweets:
                    all_event_tweets.extend(gallery_event_tweets)
                    self.logger.info(f"Gallery-dl found {len(gallery_event_tweets)} event tweets for @{username}")
                
            except Exception as e:
                self.logger.error(f"Gallery-dl failed for @{username}: {e}")
        else:
            self.logger.info("Gallery-dl is disabled, skipping media tweet fetching")
        
        # 2. twscrapeでテキストのみツイートを補完取得
        if twscrape_enabled:
            self.logger.info(f"Step 2: Fetching remaining tweets with twscrape for @{username}")
            try:
                # twscrapeは事前に記録した最新日時を基準に効率化
                twscrape_tweets = await self._get_user_tweets_twscrape_only(
                    username, 
                    days_lookback, 
                    force_full_fetch, 
                    latest_date_override=pre_crawl_latest_date,
                    latest_id_override=pre_crawl_latest_id
                )
                
                if twscrape_tweets:
                    # gallery-dlで取得済みのツイートIDを除外
                    gallery_tweet_ids = {tweet['id'] for tweet in all_tweets}
                    new_twscrape_tweets = [
                        tweet for tweet in twscrape_tweets 
                        if tweet['id'] not in gallery_tweet_ids
                    ]
                    
                    all_tweets.extend(new_twscrape_tweets)
                    self.logger.info(f"twscrape added {len(new_twscrape_tweets)} additional tweets for @{username} (filtered {len(twscrape_tweets) - len(new_twscrape_tweets)} duplicates)")
                
            except Exception as e:
                self.logger.error(f"twscrape failed for @{username}: {e}")
        else:
            self.logger.info("twscrape is disabled, skipping text-only tweet fetching")
        
        # 日付でソート（新しい順）
        all_tweets.sort(key=lambda x: x['date'], reverse=True)
        all_event_tweets.sort(key=lambda x: x['date'], reverse=True)
        
        self.logger.info(f"Total tweets retrieved for @{username}: {len(all_tweets)} (including {len(all_event_tweets)} event tweets)")
        
        return all_tweets, all_event_tweets
    
    async def _get_user_tweets_twscrape_only(self, username: str, days_lookback: int = 365, force_full_fetch: bool = False, latest_date_override=None, latest_id_override=None) -> List[Dict[str, Any]]:
        """
        twscrapeのみでツイートを取得（gallery-dl優先処理用）
        
        Args:
            latest_date_override: 効率化のため外部から指定された最新日時
            latest_id_override: 効率化のため外部から指定された最新ID
        """
        # リトライ処理（最大3回）
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                return await self._get_user_tweets_twscrape_internal(
                    username, days_lookback, force_full_fetch, 
                    latest_date_override, latest_id_override
                )
            except TimeoutError as e:
                retry_count += 1
                if retry_count < max_retries:
                    self.logger.warning(f"twscrape: Timeout for @{username}, retry {retry_count}/{max_retries}")
                    
                    # アカウントローテーションを試行
                    try:
                        await self._rotate_account()
                        self.logger.info(f"twscrape: Rotated to next account for retry {retry_count}")
                    except Exception as rotate_error:
                        self.logger.warning(f"twscrape: Failed to rotate account: {rotate_error}")
                    
                    await asyncio.sleep(10 * retry_count)  # 10秒, 20秒, 30秒
                else:
                    self.logger.error(f"twscrape: Max retries reached for @{username}")
                    raise e
            except Exception as e:
                self.logger.error(f"twscrape: Error for @{username}: {e}")
                return []
                
        return []
    
    async def _get_user_tweets_twscrape_internal(self, username: str, days_lookback: int = 365, force_full_fetch: bool = False, latest_date_override=None, latest_id_override=None) -> List[Dict[str, Any]]:
        """
        twscrapeの内部実装（タイムアウトエラーを投げる）
        """
        # 既存のget_user_tweetsロジックを流用し、DB取得部分のみoverride値を使用
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
        
        # override値があればそれを使用、なければDBから取得
        latest_tweet_date = latest_date_override
        latest_tweet_id = latest_id_override
        
        if latest_tweet_date is None and not force_full_fetch:
            latest_tweet_date = db_manager.get_latest_tweet_date(username)
            latest_tweet_id = db_manager.get_latest_tweet_id(username)
        
        # force_full_fetchがfalseで既存データがある場合、最初の1件だけチェック
        check_for_new_tweets_only = not force_full_fetch and latest_tweet_id is not None
        
        if not force_full_fetch and latest_tweet_date:
            # 最新ツイート日付以降のみ取得（効率化）
            since_date = latest_tweet_date
            self.logger.info(f"twscrape: Found existing tweets for @{username}, fetching since {since_date.strftime('%Y-%m-%d %H:%M:%S')}")
            if latest_tweet_id:
                self.logger.debug(f"twscrape: Latest tweet ID: {latest_tweet_id}, will check for new tweets only")
        elif not force_full_fetch:
            self.logger.info(f"twscrape: No existing tweets for @{username}, fetching since {since_date.strftime('%Y-%m-%d')}")
        else:
            self.logger.info(f"twscrape: Force full fetch enabled for @{username}, fetching ALL tweets since {since_date.strftime('%Y-%m-%d')} with duplicate checking")
        
        # collected_tweets処理を削除（簡素化）
        
        try:
            # アカウント利用可能性をチェック（簡易版）
            try:
                pool_stats = await self.api.pool.stats()
                self.logger.debug(f"twscrape: Account pool stats: {pool_stats}")
                
                # アクティブなアカウントが1つもない場合のみ早期リターン
                accounts_info = await self.api.pool.accounts_info()
                active_accounts = [acc for acc in accounts_info if acc.get('active', False)]
                if not active_accounts:
                    self.logger.error(f"twscrape: No active accounts found. All {len(accounts_info)} accounts are invalid.")
                    return []
                
                # 利用可能なアカウント数を確認（情報のみ）
                available_now = [acc for acc in active_accounts if acc.get('locks', {}).get('UserTweets', 0) < time.time()]
                if not available_now:
                    self.logger.info(f"twscrape: All {len(active_accounts)} active accounts are rate-limited. Will wait for next available slot.")
                else:
                    self.logger.debug(f"twscrape: {len(available_now)}/{len(active_accounts)} accounts available now")
                    
            except Exception as e:
                self.logger.warning(f"twscrape: Could not check pool stats: {e}")
            
            # First, resolve username to user ID
            user = await self.api.user_by_login(username)
            if not user:
                self.logger.error(f"twscrape: User @{username} not found or suspended")
                return []
            
            display_name = user.displayname
            self.logger.info(f"twscrape: Resolved @{username} to user ID: {user.id} (Name: {display_name})")
            
            tweet_count = 0
            total_fetched = 0
            old_tweets_count = 0
            consecutive_old_tweets = 0
            max_consecutive_old = 20
            
            # kvパラメータで日付フィルタリング
            kv = None
            if latest_tweet_date and not force_full_fetch:
                kv = {"since_time": latest_tweet_date.isoformat()}
                self.logger.debug(f"twscrape: Using kv parameter: {kv}")
            
            # 新着チェックモードの場合
            if check_for_new_tweets_only:
                self.logger.info(f"twscrape: Checking for new tweets only (quick check mode)")
                has_new_tweets = False
                
                # 最初の数件だけチェック
                check_limit = 5
                start_time = time.time()
                async for tweet in self.api.user_tweets(user.id):
                    # タイムアウトチェック
                    if time.time() - start_time > self._timeout_seconds:
                        self.logger.error(f"twscrape: Timeout after {self._timeout_seconds}s while checking new tweets for @{username}")
                        raise TimeoutError(f"Timeout while checking new tweets for @{username}")
                    total_fetched += 1
                    self.logger.debug(f"twscrape: Quick check tweet {total_fetched}: ID={tweet.id}, Date={tweet.date}")
                    
                    # 既知のツイートに到達したら新着なし
                    if int(tweet.id) <= int(latest_tweet_id):
                        self.logger.info(f"twscrape: No new tweets found for @{username} (reached known tweet {tweet.id})")
                        return []
                    
                    # 新着ツイートがあることを確認
                    has_new_tweets = True
                    
                    # チェック上限に達したら通常モードで再取得
                    if total_fetched >= check_limit:
                        self.logger.info(f"twscrape: New tweets detected for @{username}, switching to normal fetch mode")
                        break
                
                # 新着がない場合は早期終了
                if not has_new_tweets:
                    self.logger.info(f"twscrape: No new tweets found for @{username}")
                    return []
                
                # 新着がある場合は最初から取得し直す
                total_fetched = 0
            
            start_time = time.time()
            
            # イテレータ自体にタイムアウトを設定
            tweet_iterator = self.api.user_tweets(user.id).__aiter__()
            
            while True:
                try:
                    # 各ツイート取得に30秒のタイムアウトを設定
                    tweet = await asyncio.wait_for(tweet_iterator.__anext__(), timeout=30.0)
                    
                    # 全体のタイムアウトチェック
                    if time.time() - start_time > self._timeout_seconds:
                        self.logger.error(f"twscrape: Overall timeout after {self._timeout_seconds}s while fetching tweets for @{username}")
                        raise TimeoutError(f"Overall timeout while fetching tweets for @{username}")
                    
                    total_fetched += 1
                    self.logger.debug(f"twscrape: Tweet {total_fetched}: ID={tweet.id}, Date={tweet.date}")
                    
                    # Tweet IDベースの早期終了
                    if not force_full_fetch and latest_tweet_id and int(tweet.id) <= int(latest_tweet_id):
                        self.logger.debug(f"twscrape: Reached known tweet {tweet.id}, stopping")
                        break
                    
                    # 日付チェック
                    if tweet.date < since_date:
                        old_tweets_count += 1
                        consecutive_old_tweets += 1
                        self.logger.debug(f"twscrape: Skipping old tweet: {tweet.id}")
                        
                        if not force_full_fetch and consecutive_old_tweets >= max_consecutive_old:
                            self.logger.debug(f"twscrape: Reached {max_consecutive_old} consecutive old tweets, stopping")
                            break
                        continue
                    else:
                        consecutive_old_tweets = 0
                    
                    # リツイートをスキップ
                    if self._is_retweet(tweet, username):
                        self.logger.debug(f"twscrape: Skipping retweet: {tweet.id}")
                        continue
                    
                    # 既存ツイートとの重複チェック
                    tweet_id_str = str(tweet.id)
                    if tweet_id_str in existing_tweet_ids:
                        self.logger.debug(f"twscrape: Skipping duplicate tweet: {tweet.id}")
                        continue
                    
                    # ツイートデータを抽出
                    tweet_data = {
                        'id': str(tweet.id),
                        'text': tweet.rawContent,
                        'date': tweet.date.isoformat(),
                        'url': f"https://twitter.com/{username}/status/{tweet.id}",
                        'username': username,
                        'media': [],
                        'videos': []
                    }
                    
                    # メディア（画像）URLを抽出
                    if hasattr(tweet, 'media') and hasattr(tweet.media, 'photos'):
                        tweet_data['media'] = [photo.url for photo in tweet.media.photos]
                    
                    # 動画URLを抽出
                    if hasattr(tweet, 'media') and hasattr(tweet.media, 'videos'):
                        for video in tweet.media.videos:
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
                    
                    tweets.append(tweet_data)
                    tweet_count += 1
                    
                    # ツイート取得成功時にリトライカウンタをリセット
                    self._tweet_retry_count = 0
                    self._http_retry_count = 0
                    
                    if tweet_count % 100 == 0:
                        self.logger.debug(f"twscrape: Fetched {tweet_count} tweets so far...")
                        
                except StopAsyncIteration:
                    # イテレータ終了
                    self.logger.debug(f"twscrape: Reached end of tweets for @{username}")
                    break
                    
                except asyncio.TimeoutError:
                    # 個別ツイート取得のタイムアウト - ローテーションしてリトライ
                    self.logger.warning(f"twscrape: Tweet fetch timeout after 30s")
                    
                    # 個別ツイートレベルでのリトライ（最大2回）
                    tweet_retry_count = getattr(self, '_tweet_retry_count', 0)
                    if tweet_retry_count < 2:
                        self._tweet_retry_count = tweet_retry_count + 1
                        self.logger.info(f"twscrape: Individual tweet retry {self._tweet_retry_count}/2")
                        
                        # アカウントローテーション
                        try:
                            await self._rotate_account()
                            self.logger.info(f"twscrape: Rotated account for individual tweet retry")
                        except Exception as rotate_error:
                            self.logger.warning(f"twscrape: Failed to rotate account for tweet retry: {rotate_error}")
                        
                        # 短時間待機後にリトライ（同じツイートを再試行）
                        await asyncio.sleep(5)
                        continue
                    else:
                        # 最大リトライ数に達したら次のツイートに進む
                        self.logger.warning(f"twscrape: Max individual tweet retries reached, skipping to next tweet")
                        self._tweet_retry_count = 0  # カウンタリセット
                        continue
                    
                except (httpx.ReadTimeout, httpx.ConnectTimeout) as e:
                    # HTTPタイムアウト - ローテーションしてリトライ
                    self.logger.warning(f"twscrape: HTTP timeout: {e}")
                    
                    # HTTPレベルでのリトライ（最大2回）
                    http_retry_count = getattr(self, '_http_retry_count', 0)
                    if http_retry_count < 2:
                        self._http_retry_count = http_retry_count + 1
                        self.logger.info(f"twscrape: HTTP retry {self._http_retry_count}/2")
                        
                        # アカウントローテーション
                        try:
                            await self._rotate_account()
                            self.logger.info(f"twscrape: Rotated account for HTTP retry")
                        except Exception as rotate_error:
                            self.logger.warning(f"twscrape: Failed to rotate account for HTTP retry: {rotate_error}")
                        
                        # 指数バックオフで待機
                        await asyncio.sleep(2 ** self._http_retry_count)  # 2秒, 4秒
                        continue
                    else:
                        # 最大リトライ数に達したら次のツイートに進む
                        self.logger.warning(f"twscrape: Max HTTP retries reached, skipping to next tweet") 
                        self._http_retry_count = 0  # カウンタリセット
                        continue
                        
                except Exception as e:
                    # その他のエラー
                    self.logger.warning(f"twscrape: Error processing tweet: {e}")
                    continue
            
            # 重複を除去
            unique_tweets = []
            seen_ids = set()
            for tweet in tweets:
                if tweet['id'] not in seen_ids:
                    unique_tweets.append(tweet)
                    seen_ids.add(tweet['id'])
            
            duplicate_count = len(tweets) - len(unique_tweets)
            if duplicate_count > 0:
                self.logger.warning(f"twscrape: Removed {duplicate_count} duplicate tweets")
            
            self.logger.info(f"twscrape: Fetched {len(unique_tweets)} unique tweets for @{username} (examined: {total_fetched}, old skipped: {old_tweets_count})")
            return unique_tweets
            
        except Exception as e:
            if "No account available" in str(e):
                self.logger.warning(f"twscrape: Rate limit reached for @{username}: {e}")
                # 次の利用可能時間を抽出してログ出力
                next_available_match = re.search(r'Next available at (\d{2}:\d{2}:\d{2})', str(e))
                if next_available_match:
                    next_time = next_available_match.group(1)
                    self.logger.info(f"twscrape: Next available time: {next_time}")
            else:
                self.logger.error(f"twscrape: Error fetching tweets for @{username}: {e}")
            # タイムアウトエラーは再スロー
            if isinstance(e, TimeoutError):
                raise e
            return []
    
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
            # アカウント利用可能性をチェック（簡易版）
            try:
                pool_stats = await self.api.pool.stats()
                self.logger.debug(f"Account pool stats: {pool_stats}")
                
                # アクティブなアカウントが1つもない場合のみ早期リターン
                accounts_info = await self.api.pool.accounts_info()
                active_accounts = [acc for acc in accounts_info if acc.get('active', False)]
                if not active_accounts:
                    self.logger.error(f"No active accounts found. All {len(accounts_info)} accounts are invalid.")
                    return []
                
                # 利用可能なアカウント数を確認（情報のみ）
                available_now = [acc for acc in active_accounts if acc.get('locks', {}).get('UserTweets', 0) < time.time()]
                if not available_now:
                    self.logger.info(f"All {len(active_accounts)} active accounts are rate-limited. Will wait for next available slot.")
                else:
                    self.logger.debug(f"{len(available_now)}/{len(active_accounts)} accounts available now")
                    
            except Exception as e:
                self.logger.warning(f"Could not check pool stats: {e}")
            
            # First, resolve username to user ID
            user = await self.api.user_by_login(username)
            if not user:
                self.logger.error(f"User @{username} not found or suspended")
                return []
            
            display_name = user.displayname  # display_nameを設定
            self.logger.info(f"Resolved @{username} to user ID: {user.id} (Name: {display_name})")
            self.logger.info(f"@{username} の総ツイート数: {user.statusesCount}")
            
            tweet_count = 0
            total_fetched = 0
            old_tweets_count = 0
            consecutive_old_tweets = 0
            max_consecutive_old = 20  # 連続して古いツイートが20個続いたら終了
            
            # APIからは必要な分だけ取得（日付でフィルタリング）
            self.logger.debug(f"Starting to fetch tweets")
            
            # twscrapeのkvパラメータで日付フィルタリングを試験的に実装
            kv = None
            if latest_tweet_date and not force_full_fetch:
                # ISO形式の日時文字列でフィルタリング（試験的）
                kv = {"since_time": latest_tweet_date.isoformat()}
                self.logger.debug(f"Trying to filter tweets with kv parameter: {kv}")
            
            # limitの設定
            limit = -1  # デフォルトは-1（無制限）
            if not force_full_fetch:
                if latest_tweet_date and (datetime.now(timezone.utc) - latest_tweet_date).days < 7:
                    limit = 50  # 1週間以内に更新があった場合は50件まで
                    self.logger.debug(f"Recent update detected, limiting fetch to {limit} tweets")
                elif latest_tweet_date and (datetime.now(timezone.utc) - latest_tweet_date).days < 30:
                    limit = 100  # 1ヶ月以内の場合は100件まで
                    self.logger.debug(f"Recent month update detected, limiting fetch to {limit} tweets")
                else:
                    # 初回取得や古いデータの場合は無制限
                    limit = -1
                    self.logger.info(f"No recent updates, fetching all available tweets (limit=-1)")
            else:
                # force_full_fetchの場合は明示的に-1（無制限）を設定
                limit = -1
                self.logger.info(f"Force full fetch enabled, no limit set (fetching all available tweets)")
                
                # force_full_fetchの場合、kvパラメータをクリア（日付フィルタを無効化）
                kv = None
                self.logger.info(f"Clearing date filters for complete fetch")
            
            # force_full_fetchの場合、アカウントプールの状態を定期的に確認
            check_interval = 500  # 500ツイートごとにチェック
            
            start_time = time.time()
            async for tweet in self.api.user_tweets(user.id):
                # タイムアウトチェック
                if time.time() - start_time > self._timeout_seconds:
                    self.logger.error(f"Timeout after {self._timeout_seconds}s while fetching tweets for @{username}")
                    break
                    
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
            
            # gallery-dl統合（設定で有効な場合）
            gallery_dl_config = self.config.get('tweet_settings', {}).get('gallery_dl', {})
            if gallery_dl_config.get('enabled', False):
                self.logger.info(f"gallery-dl integration enabled for @{username}")
                try:
                    from .gallery_dl_extractor import GalleryDLExtractor
                    gallery_extractor = GalleryDLExtractor(self.config)
                    
                    # gallery-dlでメディア付きツイートを取得（制限なし）
                    self.logger.info(f"Fetching all media tweets with gallery-dl")
                    gallery_tweets = gallery_extractor.fetch_media_tweets(username, limit=None)
                    
                    if gallery_tweets:
                        # 既存のツイートIDセット（重複排除用）
                        existing_ids = {tweet['id'] for tweet in unique_tweets}
                        existing_ids.update(existing_tweet_ids)  # データベースの既存IDも含める
                        
                        # gallery-dlのツイートを追加（重複を除く）
                        new_from_gallery = []
                        new_tweet_ids = []  # 新規ツイートIDのリスト（ダウンロード用）
                        for g_tweet in gallery_tweets:
                            if g_tweet['id'] not in existing_ids:
                                # display_nameを追加
                                g_tweet['display_name'] = display_name
                                new_from_gallery.append(g_tweet)
                                new_tweet_ids.append(g_tweet['id'])  # 新規ツイートIDを記録
                                existing_ids.add(g_tweet['id'])
                        
                        if new_from_gallery:
                            self.logger.info(f"Added {len(new_from_gallery)} new tweets from gallery-dl")
                            
                            # 新規ツイートのメディアのみをダウンロード
                            if new_tweet_ids:
                                self.logger.info(f"Downloading media files for {len(new_tweet_ids)} new tweets")
                                tweet_media_paths = gallery_extractor.download_media_for_tweets(username, new_tweet_ids)
                                
                                # 各ツイートにlocal_mediaを設定
                                for g_tweet in new_from_gallery:
                                    if g_tweet['id'] in tweet_media_paths:
                                        g_tweet['local_media'] = tweet_media_paths[g_tweet['id']]
                                        self.logger.debug(f"Set local_media for tweet {g_tweet['id']}: {len(g_tweet['local_media'])} files")
                                    else:
                                        g_tweet['local_media'] = []
                            
                            unique_tweets.extend(new_from_gallery)
                            
                            # 日付でソート（新しい順）
                            unique_tweets.sort(key=lambda x: x['date'], reverse=True)
                        else:
                            self.logger.info(f"No new tweets from gallery-dl (all {len(gallery_tweets)} were duplicates)")
                    else:
                        self.logger.info("No media tweets found by gallery-dl")
                        
                except Exception as e:
                    self.logger.error(f"gallery-dl integration failed: {e}")
                    # エラーが発生してもtwscrapeの結果は返す
            
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
    
    async def _rotate_account(self):
        """次のアカウントにローテーション"""
        try:
            # 現在のアカウント情報を取得
            current_accounts = await self.api.pool.accounts_info()
            active_accounts = [acc for acc in current_accounts if acc.get('active', True)]
            
            if len(active_accounts) <= 1:
                self.logger.warning("twscrape: Only one active account available, cannot rotate")
                return
            
            # アカウントプールの統計を取得
            pool_stats = await self.api.pool.stats()
            self.logger.debug(f"twscrape: Current pool stats: {pool_stats}")
            
            # 失敗したアカウントを明示的にマークして次のアカウントを使用させる
            try:
                # 現在使用中のアカウントを一時的に無効化
                current_account = getattr(self.api.pool, '_current_account', None)
                if current_account:
                    # 短時間のクールダウンを設定
                    current_account.unlock_at = time.time() + 60  # 1分間のクールダウン
                    self.logger.info(f"twscrape: Set 1-minute cooldown for current account")
            except Exception as cooldown_error:
                self.logger.debug(f"twscrape: Could not set account cooldown: {cooldown_error}")
            
            # 利用可能なアカウントの再確認を強制
            await self.api.pool.refresh()
            
            self.logger.info(f"twscrape: Account rotation completed, {len(active_accounts)} accounts available")
            
        except Exception as e:
            self.logger.error(f"twscrape: Error during account rotation: {e}")
            # 代替手段：失敗したアカウントの再ログインを試行
            try:
                await self.api.pool.relogin_failed()
                self.logger.info("twscrape: Attempted relogin for failed accounts")
            except Exception as e2:
                self.logger.warning(f"twscrape: Could not relogin failed accounts: {e2}")
                # エラーを再発生させずに続行

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