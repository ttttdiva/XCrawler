#!/usr/bin/env python3
"""
gallery-dl用のCookieローテーション機能
複数のCookieファイルを順番に使用してレート制限を回避
"""

import os
import random
from pathlib import Path
from typing import List, Optional
import logging


class GalleryDLCookieRotator:
    """gallery-dl用のCookieローテーター"""
    
    def __init__(self, cookie_dir: str = "cookies"):
        self.logger = logging.getLogger("EventMonitor.CookieRotator")
        self.cookie_dir = Path(cookie_dir)
        self.cookie_files = self._find_cookie_files()
        self.current_index = 0
        
        if self.cookie_files:
            self.logger.info(f"Found {len(self.cookie_files)} cookie files for rotation")
        else:
            self.logger.warning("No cookie files found for rotation")
    
    def _find_cookie_files(self) -> List[Path]:
        """Cookieファイルを検索"""
        cookie_files = []
        
        if not self.cookie_dir.exists():
            return cookie_files
        
        # x.com_cookies*.txt パターンのファイルを検索
        patterns = [
            "x.com_cookies.txt",
            "x.com_cookies_*.txt"
        ]
        
        for pattern in patterns:
            cookie_files.extend(self.cookie_dir.glob(pattern))
        
        # 重複を除去
        cookie_files = list(set(cookie_files))
        
        return sorted(cookie_files)
    
    def get_next_cookie(self) -> Optional[Path]:
        """次のCookieファイルを取得（ラウンドロビン）"""
        if not self.cookie_files:
            # デフォルトのCookieファイルを返す
            default_cookie = self.cookie_dir / "x.com_cookies.txt"
            if default_cookie.exists():
                return default_cookie
            return None
        
        cookie_file = self.cookie_files[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.cookie_files)
        
        self.logger.debug(f"Using cookie file: {cookie_file.name}")
        return cookie_file
    
    def get_random_cookie(self) -> Optional[Path]:
        """ランダムにCookieファイルを選択"""
        if not self.cookie_files:
            default_cookie = self.cookie_dir / "x.com_cookies.txt"
            if default_cookie.exists():
                return default_cookie
            return None
        
        cookie_file = random.choice(self.cookie_files)
        self.logger.debug(f"Using random cookie file: {cookie_file.name}")
        return cookie_file
    
    def validate_cookies(self) -> List[Path]:
        """有効なCookieファイルのみを返す"""
        valid_cookies = []
        
        for cookie_file in self.cookie_files:
            if self._is_valid_cookie(cookie_file):
                valid_cookies.append(cookie_file)
            else:
                self.logger.warning(f"Invalid cookie file: {cookie_file.name}")
        
        return valid_cookies
    
    def _is_valid_cookie(self, cookie_file: Path) -> bool:
        """Cookieファイルの妥当性チェック"""
        if not cookie_file.exists():
            return False
        
        # ファイルサイズチェック（最低100バイト）
        if cookie_file.stat().st_size < 100:
            return False
        
        # auth_tokenとct0が含まれているかチェック
        try:
            with open(cookie_file, 'r') as f:
                content = f.read()
                return 'auth_token' in content and 'ct0' in content
        except Exception:
            return False
    
    def setup_multiple_cookies(self, base_cookie: Path, count: int = 3) -> List[Path]:
        """
        ベースのCookieファイルから複数のコピーを作成（テスト用）
        
        実際の運用では、異なるアカウントのCookieを用意する必要があります
        """
        if not base_cookie.exists():
            self.logger.error(f"Base cookie file not found: {base_cookie}")
            return []
        
        created_files = []
        
        for i in range(1, count + 1):
            new_cookie = self.cookie_dir / f"x.com_cookies_{i}.txt"
            
            # 既に存在する場合はスキップ
            if new_cookie.exists():
                created_files.append(new_cookie)
                continue
            
            # コピー作成（実際は異なるアカウントのCookieを用意すべき）
            try:
                new_cookie.write_text(base_cookie.read_text())
                created_files.append(new_cookie)
                self.logger.info(f"Created cookie copy: {new_cookie.name}")
            except Exception as e:
                self.logger.error(f"Failed to create cookie copy: {e}")
        
        return created_files