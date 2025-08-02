import logging
import sys
from pathlib import Path
from datetime import datetime
import colorama
from colorama import Fore, Style


def setup_logging(log_level: str = "INFO", log_dir: Path = None) -> logging.Logger:
    """ロギングの設定"""
    colorama.init()
    
    # ログディレクトリが指定されていない場合はデフォルト
    if log_dir is None:
        log_dir = Path("data")
        log_dir.mkdir(exist_ok=True)
    
    # ログファイルは常にapp.log
    log_file = log_dir / "app.log"
    
    # ロガーの設定
    logger = logging.getLogger("EventMonitor")
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # 既存のハンドラーをクリア（重複防止）
    logger.handlers.clear()
    
    # フォーマッター
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_formatter = logging.Formatter(
        f'{Fore.CYAN}%(asctime)s{Style.RESET_ALL} - '
        f'{Fore.GREEN}%(name)s{Style.RESET_ALL} - '
        f'%(levelname)s - %(message)s'
    )
    
    # ファイルハンドラー
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    
    # コンソールハンドラー
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    console_handler.setFormatter(console_formatter)
    
    # カスタムフィルタでレベルごとの色分け
    class ColoredFilter(logging.Filter):
        def filter(self, record):
            if record.levelno == logging.DEBUG:
                record.levelname = f"{Fore.BLUE}{record.levelname}{Style.RESET_ALL}"
            elif record.levelno == logging.INFO:
                record.levelname = f"{Fore.GREEN}{record.levelname}{Style.RESET_ALL}"
            elif record.levelno == logging.WARNING:
                record.levelname = f"{Fore.YELLOW}{record.levelname}{Style.RESET_ALL}"
            elif record.levelno == logging.ERROR:
                record.levelname = f"{Fore.RED}{record.levelname}{Style.RESET_ALL}"
            elif record.levelno == logging.CRITICAL:
                record.levelname = f"{Fore.MAGENTA}{record.levelname}{Style.RESET_ALL}"
            return True
    
    console_handler.addFilter(ColoredFilter())
    
    # ハンドラーを追加
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def format_tweet_url(username: str, tweet_id: str) -> str:
    """ツイートURLをフォーマット"""
    return f"https://twitter.com/{username}/status/{tweet_id}"


def parse_date_string(date_str: str) -> datetime:
    """日付文字列をdatetimeオブジェクトに変換"""
    # 複数のフォーマットを試す
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S.%fZ"
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    # すべて失敗した場合
    raise ValueError(f"Unable to parse date string: {date_str}")