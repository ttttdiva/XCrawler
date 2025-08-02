#!/usr/bin/env python3
"""
Playwrightを使用してTwitterから全ツイートを取得するモジュール

半年ずつの期間に区切って検索することで、効率的に全ツイートを取得する。
3年間連続でツイートが存在しない期間が続いた場合、1970年まで遡って検索する。
"""
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import json
from pathlib import Path
import os
from dotenv import load_dotenv
import time
import re

try:
    from playwright.async_api import async_playwright, Page
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

load_dotenv()

class BrowserFetcher:
    """Playwrightを使用してツイートを取得するクラス"""
    
    def __init__(self, config: Dict[str, Any], db_manager, twitter_monitor=None):
        """
        Args:
            config: 設定辞書
            db_manager: データベースマネージャー
            twitter_monitor: TwitterMonitorインスタンス（認証情報取得用）
        """
        self.config = config
        self.db_manager = db_manager
        self.twitter_monitor = twitter_monitor
        self.logger = logging.getLogger(__name__)
        
        if not PLAYWRIGHT_AVAILABLE:
            raise ImportError("Playwright is not installed. Please run: pip install playwright && playwright install chromium")
    
    async def fetch_all_tweet_ids(self, username: str) -> List[str]:
        """
        指定されたユーザーの全ツイートIDを半年ずつの期間に分けて収集
        
        Args:
            username: Twitterユーザー名
            
        Returns:
            ツイートIDのリスト（詳細データはtwscrapeで取得するため）
        """
        self.logger.info(f"Starting Playwright ID collection for @{username}")
        self.logger.info("fetch_all_tweet_ids method called!")
        
        async with async_playwright() as p:
            self.logger.info("Playwright started")
            # ブラウザを起動（ステルス設定）
            browser = await p.chromium.launch(
                headless=self.config.get('tweet_settings', {}).get('playwright', {}).get('headless', True),
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-extensions',
                    '--disable-plugins',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--no-first-run',
                    '--no-default-browser-check'
                ]
            )
            
            try:
                # コンテキストを作成（ステルス設定）
                context = await browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                    extra_http_headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'Accept-Language': 'en-US,en;q=0.9,ja;q=0.8',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'DNT': '1',
                        'Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1',
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': 'none',
                        'Sec-Fetch-User': '?1',
                        'Cache-Control': 'max-age=0'
                    },
                    java_script_enabled=True  # JavaScriptを明示的に有効化
                )
                
                # ボット検出回避のためのJavaScript実行
                await context.add_init_script("""
                    // webdriver プロパティを削除
                    delete navigator.__proto__.webdriver;
                    
                    // Chrome ObjectやPermissions APIの調整
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined,
                    });
                    
                    // プラグイン配列の調整
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5],
                    });
                    
                    // 言語設定
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['en-US', 'en', 'ja'],
                    });
                """)
                
                # Cookieを設定（認証用）
                cookies_set = False
                
                # 環境変数から認証情報を取得（TWITTER_ACCOUNT_X_TOKEN形式）
                self.logger.info("Looking for authentication cookies...")
                for i in range(1, 15):  # 1-14アカウント
                    auth_token = os.getenv(f'TWITTER_ACCOUNT_{i}_TOKEN')
                    ct0 = os.getenv(f'TWITTER_ACCOUNT_{i}_CT0')
                    if i == 1:
                        self.logger.info(f"Account 1 check: token={auth_token is not None}, ct0={ct0 is not None}")
                    
                    if auth_token and ct0:
                        self.logger.info(f"Using account {i} for Playwright authentication")
                        
                        # ドメイン初期化用ページ
                        init_page = await context.new_page()
                        await init_page.goto("https://x.com", wait_until='domcontentloaded')
                        
                        # Cookieを設定
                        await context.add_cookies([
                            {
                                'name': 'auth_token',
                                'value': auth_token,
                                'domain': '.x.com',
                                'path': '/',
                                'httpOnly': True,
                                'secure': True,
                                'sameSite': 'Lax'
                            },
                            {
                                'name': 'ct0',
                                'value': ct0,
                                'domain': '.x.com',
                                'path': '/',
                                'httpOnly': False,
                                'secure': True,
                                'sameSite': 'Lax'
                            }
                        ])
                        
                        await init_page.close()
                        cookies_set = True
                        break
                
                if not cookies_set:
                    self.logger.warning("No authentication cookies found, proceeding without login")
                
                # 認証状態を確立するため、まずホームページにアクセス
                if cookies_set:
                    auth_page = await context.new_page()
                    try:
                        self.logger.info("Establishing authentication by visiting home page...")
                        await auth_page.goto("https://x.com/home", wait_until='domcontentloaded', timeout=30000)
                        await asyncio.sleep(3)
                        
                        # 認証状態を確認
                        page_content = await auth_page.content()
                        if "Sign in" in page_content or "Log in" in page_content:
                            self.logger.warning("Authentication may have failed - login prompt detected")
                        else:
                            self.logger.info("✅ Authentication appears successful")
                            
                    except Exception as e:
                        self.logger.warning(f"Could not verify authentication: {e}")
                    finally:
                        await auth_page.close()
                
                # 全ツイートIDを収集
                all_tweet_ids = await self._fetch_tweet_ids_by_periods(context, username)
                
                return all_tweet_ids
                
            finally:
                await browser.close()
    
    
    async def _fetch_tweet_ids_by_periods(self, context, username: str) -> List[str]:
        """期間分割検索でツイートIDを収集"""
        all_tweet_ids = []
        current_date = datetime.now()
        seen_tweet_ids = set()
        consecutive_empty_periods = 0
        
        self.logger.info(f"@{username} の期間分割検索による全件取得を開始")
        
        page = await context.new_page()
        
        try:
            # 期間を短くして検索精度を向上（3ヶ月単位に変更）
            while consecutive_empty_periods < 12:  # 12回連続空白で終了
                # 3ヶ月前の期間を設定（より細かく検索）
                end_date = current_date
                start_date = end_date - timedelta(days=90)  # 約3ヶ月
                
                start_str = start_date.strftime("%Y-%m-%d")
                end_str = end_date.strftime("%Y-%m-%d")
                
                self.logger.info(f"期間検索: {start_str} 〜 {end_str}")
                
                # 検索クエリを構築
                search_query = f"from:{username} since:{start_str} until:{end_str}"
                search_url = f"https://x.com/search?q={search_query}&src=typed_query&f=live"
                
                self.logger.info(f"検索URL: {search_url}")
                
                # 検索ページにアクセス
                self.logger.info(f"Accessing search page: {search_url}")
                max_retries = 3
                for retry in range(max_retries):
                    try:
                        # タイムアウトを120秒に延長、domcontentloadedで早めに続行
                        await page.goto(search_url, wait_until='domcontentloaded', timeout=120000)
                        await asyncio.sleep(5)  # 検索結果の読み込み待機
                        break  # 成功したらループを抜ける
                    except Exception as e:
                        if "Timeout" in str(e) and retry < max_retries - 1:
                            self.logger.warning(f"Timeout occurred, retrying... ({retry + 1}/{max_retries})")
                            await asyncio.sleep(10)  # リトライ前に待機
                            continue
                        else:
                            raise  # 最後のリトライでも失敗したらエラーを投げる
                
                # ページ読み込み完了を短時間待つ
                await asyncio.sleep(2)
                
                # 「検索結果はありません」を即座にチェック
                try:
                    page_text = await page.text_content('body')
                    if "検索結果はありません" in page_text or "No results for" in page_text:
                        self.logger.info("✅ No results for this period - skipping")
                        period_tweet_ids = []
                    else:
                        # ツイートがあるか確認（待機なし）
                        tweet_count = await page.locator('article[data-testid="tweet"]').count()
                        if tweet_count > 0:
                            self.logger.info(f"✅ Found {tweet_count} tweets in this period")
                            # この期間のツイートIDを収集
                            period_tweet_ids = await self._fetch_tweet_ids_from_search_page(page, username, seen_tweet_ids)
                        else:
                            self.logger.info("✅ No tweets visible yet")
                            period_tweet_ids = []
                        
                except Exception as e:
                    self.logger.error(f"Failed to process search page: {e}")
                    period_tweet_ids = []
                
                if period_tweet_ids:
                    all_tweet_ids.extend(period_tweet_ids)
                    seen_tweet_ids.update(period_tweet_ids)
                    consecutive_empty_periods = 0
                    self.logger.info(f"期間 {start_str}〜{end_str}: {len(period_tweet_ids)}件のID収集 (累計: {len(all_tweet_ids)}件)")
                else:
                    consecutive_empty_periods += 1
                    self.logger.info(f"期間 {start_str}〜{end_str}: 0件 (連続空白: {consecutive_empty_periods}/12)")
                
                # 次の期間に移動
                current_date = start_date
            
            # 12回連続空白が続いた場合、1970年まで一気に検索
            if consecutive_empty_periods >= 12:
                self.logger.info("12回連続空白期間を検出。1970年まで一気に検索します")
                
                end_date = current_date
                start_date = datetime(1970, 1, 1)
                
                start_str = start_date.strftime("%Y-%m-%d")
                end_str = end_date.strftime("%Y-%m-%d")
                
                search_query = f"from:{username} since:{start_str} until:{end_str}"
                search_url = f"https://x.com/search?q={search_query}&src=typed_query&f=live"
                
                self.logger.info(f"最終検索: {start_str} 〜 {end_str}")
                
                await page.goto(search_url, wait_until='domcontentloaded', timeout=120000)
                await asyncio.sleep(5)
                
                final_tweet_ids = await self._fetch_tweet_ids_from_search_page(page, username, seen_tweet_ids)
                
                if final_tweet_ids:
                    all_tweet_ids.extend(final_tweet_ids)
                    self.logger.info(f"最終検索: {len(final_tweet_ids)}件のID収集")
            
        except Exception as e:
            self.logger.error(f"期間分割検索中にエラー: {e}")
        
        finally:
            await page.close()
        
        # 重複除去（IDなのでsetを使用）
        unique_tweet_ids = list(set(all_tweet_ids))
        self.logger.info(f"期間分割検索完了: 総収集数 {len(all_tweet_ids)}件、重複除去後 {len(unique_tweet_ids)}件")
        
        return unique_tweet_ids
    
    async def _fetch_tweet_ids_from_search_page(self, page: Page, username: str, seen_tweet_ids: set) -> List[str]:
        """検索ページからスクロールしてツイートIDを収集"""
        tweet_ids = []
        consecutive_no_new = 0
        max_consecutive_no_new = 20  # 10→20に増加：より多くスクロール
        scroll_attempts = 0
        
        self.logger.info("検索ページからツイートID収集開始")
        
        while consecutive_no_new < max_consecutive_no_new:
            # 現在のページからツイートIDを収集
            new_tweet_ids = await self._extract_tweet_ids_from_page(page, username, seen_tweet_ids)
            
            if new_tweet_ids:
                tweet_ids.extend(new_tweet_ids)
                seen_tweet_ids.update(new_tweet_ids)
                consecutive_no_new = 0
                self.logger.debug(f"検索ページで新しいツイートID {len(new_tweet_ids)} 件を発見")
            else:
                consecutive_no_new += 1
                self.logger.debug(f"検索ページで新しいツイートIDなし ({consecutive_no_new}/{max_consecutive_no_new})")
            
            # ページをスクロール（より滑らかなスクロール）
            await page.evaluate("""
                window.scrollBy({
                    top: window.innerHeight * 0.8,
                    behavior: 'smooth'
                });
            """)
            await asyncio.sleep(2)  # スクロール後の読み込み待機を短縮
            scroll_attempts += 1
            
            # 進捗ログ
            if scroll_attempts % 5 == 0:
                self.logger.debug(f"検索ページスクロール: {scroll_attempts}回, 収集済み: {len(tweet_ids)}件")
            
            # 安全装置を緩和（この期間で3000件まで取得可能に）
            if len(tweet_ids) >= 3000:
                self.logger.warning(f"この期間で3000件に達したため終了")
                break
        
        self.logger.info(f"検索ページから {len(tweet_ids)} 件のツイートIDを収集")
        return tweet_ids
    
    async def _extract_tweet_ids_from_page(self, page: Page, username: str, seen_tweet_ids: set) -> List[str]:
        """ページからツイートIDを抽出（シンプル化）"""
        tweet_ids = []
        
        try:
            # 複数のセレクタを試行してツイート要素を取得
            tweet_selectors = [
                'article[data-testid="tweet"]',
                '[data-testid="tweet"]',
                'article[role="article"]',
                '[role="article"]'
            ]
            
            tweet_elements = []
            for selector in tweet_selectors:
                elements = await page.query_selector_all(selector)
                self.logger.info(f"セレクタ '{selector}': {len(elements)}個の要素")
                if elements:
                    tweet_elements = elements
                    self.logger.info(f"使用するセレクタ: {selector} ({len(elements)}個)")
                    break
            
            if not tweet_elements:
                # ツイートがない期間は正常（エラーではない）
                self.logger.info("この期間にはツイートがありません")
                return tweet_ids
            
            for element in tweet_elements:
                try:
                    # ツイートIDを複数の方法で取得を試行
                    tweet_id = None
                    
                    # 方法1: timeタグのparent linkから取得
                    time_element = await element.query_selector('time')
                    if time_element:
                        parent_link = await time_element.query_selector('xpath=ancestor::a')
                        if parent_link:
                            href = await parent_link.get_attribute('href')
                            if href and '/status/' in href:
                                match = re.search(r'/status/(\d+)', href)
                                if match:
                                    tweet_id = match.group(1)
                    
                    # 方法2: 直接リンクから取得
                    if not tweet_id:
                        link_elements = await element.query_selector_all('a[href*="/status/"]')
                        for link in link_elements:
                            href = await link.get_attribute('href')
                            if href and '/status/' in href:
                                match = re.search(r'/status/(\d+)', href)
                                if match:
                                    tweet_id = match.group(1)
                                    break
                    
                    # 方法3: data-tweet-id属性から取得
                    if not tweet_id:
                        tweet_id = await element.get_attribute('data-tweet-id')
                    
                    if not tweet_id or tweet_id in seen_tweet_ids:
                        continue
                    
                    # ユーザー名を確認（自分のツイートかチェック）
                    user_element = await element.query_selector('a[href*="/' + username + '"]')
                    if not user_element:
                        # リツイートの可能性があるのでスキップ
                        continue
                    
                    # IDのみを収集（詳細はtwscrapeで取得）
                    tweet_ids.append(tweet_id)
                    self.logger.debug(f"Collected tweet ID: {tweet_id}")
                    
                except Exception as e:
                    self.logger.debug(f"Error extracting individual tweet: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Error extracting tweets from page: {e}")
        
        return tweet_ids
    
    def _remove_duplicates(self, tweets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """ツイートIDに基づいて重複を除去"""
        seen_ids = set()
        unique_tweets = []
        
        for tweet in tweets:
            if tweet['id'] not in seen_ids:
                unique_tweets.append(tweet)
                seen_ids.add(tweet['id'])
        
        return unique_tweets