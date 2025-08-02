#!/usr/bin/env python3
"""
get_username_list.txtのユーザーIDに対応するハンドルネームを取得するスクリプト
twscrapeを使用してTwitterユーザー情報を取得
"""

import os
import asyncio
import logging
import csv
from typing import List, Dict, Optional

# SQLite3エラー回避のためpysqlite3を優先使用
try:
    import pysqlite3.dbapi2 as sqlite3
    import sys
    sys.modules['sqlite3'] = sqlite3
except ImportError:
    import sqlite3

from twscrape import API
from dotenv import load_dotenv

# .envファイルを読み込み
load_dotenv()

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class UsernameExtractor:
    def __init__(self):
        self.api = API()
        self._accounts_initialized = False
    
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
                    logger.info("Added main Twitter account")
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
                    cookie_string = f"auth_token={token}; ct0={ct0}"
                    
                    await self.api.pool.add_account(
                        username=username,
                        password="dummy_password",
                        email=f"dummy{account_index}@example.com",
                        email_password="dummy_email_password",
                        cookies=cookie_string
                    )
                    logger.info(f"Added Twitter account {account_index}")
                    
                total_accounts += 1
                account_index += 1
            
            if total_accounts == 0:
                raise ValueError("No Twitter accounts configured. Please set at least one account.")
            
            # すべてのアカウントでログイン
            await self.api.pool.login_all()
            self._accounts_initialized = True
            logger.info(f"Initialized {total_accounts} Twitter account(s)")
            
        except Exception as e:
            logger.error(f"Failed to initialize Twitter accounts: {e}")
            raise
    
    def _parse_user_ids(self, file_path: str) -> List[str]:
        """get_username_list.txtからユーザーIDを抽出"""
        user_ids = []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line.startswith('@'):
                    # @で始まる行からユーザー名を取得
                    username = line[1:]  # @を除去
                    if username:
                        user_ids.append(username)
        
        logger.info(f"Extracted {len(user_ids)} user IDs from {file_path}")
        return user_ids
    
    async def _get_user_info(self, username: str) -> Optional[Dict[str, str]]:
        """ユーザー名からユーザー情報を取得"""
        try:
            user = await self.api.user_by_login(username)
            if user:
                return {
                    'username': username,
                    'display_name': user.displayname,
                    'user_id': str(user.id),
                    'followers_count': user.followersCount,
                    'following_count': user.friendsCount,
                    'created_at': user.created.isoformat() if user.created else None,
                    'description': getattr(user, 'description', '') or '',
                    'verified': user.verified,
                    'protected': user.protected
                }
            else:
                logger.warning(f"User not found: {username}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting user info for {username}: {e}")
            return None
    
    async def extract_usernames(self, input_file: str, output_file: str):
        """メイン処理：ユーザーIDリストからユーザー情報を取得"""
        await self._initialize_accounts()
        
        # ユーザーIDを読み込み
        user_ids = self._parse_user_ids(input_file)
        
        # 結果を格納するリスト
        results = []
        
        # 各ユーザーの情報を取得
        for i, username in enumerate(user_ids, 1):
            logger.info(f"Processing {i}/{len(user_ids)}: @{username}")
            
            user_info = await self._get_user_info(username)
            if user_info:
                results.append(user_info)
                logger.info(f"✓ {username} -> {user_info['display_name']}")
            else:
                # 情報が取得できなかった場合も記録
                results.append({
                    'username': username,
                    'display_name': 'N/A',
                    'user_id': 'N/A',
                    'followers_count': 'N/A',
                    'following_count': 'N/A',
                    'created_at': 'N/A',
                    'description': 'N/A',
                    'verified': 'N/A',
                    'protected': 'N/A'
                })
                logger.warning(f"✗ {username} -> Failed to get info")
            
            # レート制限対策：少し待機
            if i % 10 == 0:
                logger.info("Rate limit protection: waiting 2 seconds...")
                await asyncio.sleep(2)
        
        # CSVファイルに保存
        self._save_to_csv(results, output_file)
        logger.info(f"Results saved to {output_file}")
    
    def _save_to_csv(self, data: List[Dict[str, str]], output_file: str):
        """結果をCSVファイルに保存"""
        fieldnames = [
            'username', 'display_name', 'user_id', 'followers_count', 
            'following_count', 'created_at', 'description', 'verified', 'protected'
        ]
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

async def main():
    """メイン関数"""
    input_file = "get_username_list.txt"
    output_file = "hiero2_usernames.csv"
    
    if not os.path.exists(input_file):
        logger.error(f"Input file not found: {input_file}")
        return
    
    extractor = UsernameExtractor()
    await extractor.extract_usernames(input_file, output_file)
    logger.info("Username extraction completed!")

if __name__ == "__main__":
    asyncio.run(main())