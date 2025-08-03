#!/usr/bin/env python3
"""
gallery-dl wrapper for pysqlite3 environment
Usage: python src/gallery_dl_wrapper.py [gallery-dl options] URL
"""

import sys
import os

# pysqlite3を標準のsqlite3として登録
__import__('pysqlite3')
sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')

# 標準エラー出力を抑制（プログレス表示など）
os.environ['PYTHONWARNINGS'] = 'ignore'

import gallery_dl

if __name__ == "__main__":
    # gallery-dlのメイン関数を実行
    try:
        sys.exit(gallery_dl.main())
    except SystemExit as e:
        # 正常終了の場合はエラーコード0を返す
        if e.code in (0, None):
            sys.exit(0)
        else:
            sys.exit(e.code)