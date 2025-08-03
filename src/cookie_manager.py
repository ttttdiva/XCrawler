#!/usr/bin/env python3
"""
Twitter Cookie管理ユーティリティ
"""

import os
import logging
from pathlib import Path


class CookieManager:
    """Twitter Cookie管理クラス"""
    
    def __init__(self):
        self.logger = logging.getLogger("EventMonitor.CookieManager")

    def get_cookie_files(self) -> list[Path]:
        """利用可能なcookieファイルのリストを取得"""
        cookies_dir = Path('cookies')
        if not cookies_dir.exists():
            self.logger.warning("cookies directory does not exist")
            return []
        
        # x.com_cookies.txt及び連番ファイルを取得
        cookie_files = []
        
        # メインファイル
        main_file = cookies_dir / 'x.com_cookies.txt'
        if main_file.exists():
            cookie_files.append(main_file)
        
        # 連番ファイル
        i = 2
        while True:
            numbered_file = cookies_dir / f'x.com_cookies_{i}.txt'
            if numbered_file.exists():
                cookie_files.append(numbered_file)
                i += 1
            else:
                break
        
        return cookie_files