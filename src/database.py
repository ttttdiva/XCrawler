import sys
# pysqlite3を標準のsqlite3より先にインポート
__import__('pysqlite3')
sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')

import os
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import json

from sqlalchemy import create_engine, Column, String, DateTime, Text, Boolean, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv


Base = declarative_base()


class AllTweets(Base):
    """全ツイートログを保存するテーブル"""
    __tablename__ = 'all_tweets'
    
    id = Column(String(64), primary_key=True)  # Tweet ID
    username = Column(String(100), nullable=False, index=True)
    display_name = Column(String(200))
    tweet_text = Column(Text, nullable=False)
    tweet_date = Column(DateTime, nullable=False, index=True)
    tweet_url = Column(String(500), nullable=False)
    
    # メディア情報
    media_urls = Column(Text)  # JSON配列として保存
    local_media = Column(Text)  # ローカルメディアパス（画像・動画、JSON配列として保存）
    huggingface_urls = Column(Text)  # アップロード後のURL（JSON配列）
    
    # メタデータ
    created_at = Column(DateTime, default=datetime.now)
    checked_for_event = Column(Boolean, default=False)  # イベント検査済みフラグ


class EventTweet(Base):
    """イベント関連ツイートのモデル"""
    __tablename__ = 'event_tweets'
    
    id = Column(String(64), primary_key=True)  # Tweet ID
    username = Column(String(100), nullable=False, index=True)
    display_name = Column(String(200))
    tweet_text = Column(Text, nullable=False)
    tweet_date = Column(DateTime, nullable=False, index=True)
    tweet_url = Column(String(500), nullable=False)
    
    # イベント情報
    is_event_related = Column(Boolean, default=True)
    event_type = Column(String(100))
    event_date = Column(String(50))  # 推定されるイベント日付
    participation_type = Column(String(50))  # サークル参加/一般参加/委託
    space_number = Column(String(50))
    circle_name = Column(String(200))
    confidence_score = Column(String(10))  # 判定の信頼度
    
    # メディア情報
    media_urls = Column(Text)  # JSON配列として保存
    local_media = Column(Text)  # ローカルメディアパス（画像・動画、JSON配列として保存）
    
    # 分析結果
    analysis_result = Column(Text)  # JSON形式で保存
    
    # メタデータ
    created_at = Column(DateTime, default=datetime.now)
    notified = Column(Boolean, default=False)  # Discord通知済みフラグ


class LogOnlyTweet(Base):
    """ログ専用アカウントのツイートを保存するテーブル"""
    __tablename__ = 'log_only_tweets'
    
    id = Column(String(64), primary_key=True)  # Tweet ID
    username = Column(String(100), nullable=False, index=True)
    display_name = Column(String(200))
    tweet_text = Column(Text, nullable=False)
    tweet_date = Column(DateTime, nullable=False, index=True)
    tweet_url = Column(String(500), nullable=False)
    media_urls = Column(Text)  # JSON配列として保存
    huggingface_urls = Column(Text)  # アップロード後のURL（JSON配列）
    created_at = Column(DateTime, default=datetime.now)
    uploaded_to_hf = Column(Boolean, default=False)


class DatabaseManager:
    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger("EventMonitor.Database")
        self.engine = None
        self.Session = None
        self._initialize_database()
        
    def _initialize_database(self):
        """データベース接続を初期化"""
        try:
            db_config = self.config['database']
            
            # SQLiteかMySQLかを判定
            if db_config.get('type') == 'sqlite':
                # SQLiteの場合
                db_path = db_config['path']
                # ディレクトリを作成
                os.makedirs(os.path.dirname(db_path), exist_ok=True)
                db_url = f"sqlite:///{db_path}"
                
                self.engine = create_engine(
                    db_url,
                    echo=False
                )
            else:
                # MySQLの場合（従来の処理）
                host = db_config['host']
                port = db_config['port']
                user = db_config['user']
                password = os.getenv('DB_PASSWORD', db_config['password'])
                database = db_config['database']
                
                db_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"
                
                self.engine = create_engine(
                    db_url,
                    pool_pre_ping=True,
                    pool_size=5,
                    max_overflow=10,
                    echo=False
                )
            
            # テーブルを作成
            Base.metadata.create_all(self.engine)
            
            # セッションファクトリーを作成
            self.Session = sessionmaker(bind=self.engine)
            
            self.logger.info("Database initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
            raise
    
    def _get_session(self) -> Session:
        """データベースセッションを取得"""
        return self.Session()
    
    def filter_new_tweets(self, tweets: List[Dict[str, Any]], username: str) -> List[Dict[str, Any]]:
        """新規ツイートのみをフィルタリング（all_tweetsテーブルを参照）"""
        session = self._get_session()
        new_tweets = []
        
        try:
            # 既存のツイートIDを取得（all_tweetsテーブルから、ユーザー名に関係なく）
            existing_ids = set()
            existing_tweets = session.query(AllTweets.id).all()
            existing_ids = {tweet.id for tweet in existing_tweets}
            
            # 新規ツイートのみを抽出
            for tweet in tweets:
                if tweet['id'] not in existing_ids:
                    new_tweets.append(tweet)
                else:
                    # 既存のツイートの場合、どのユーザーから既に取得済みかを確認
                    existing_record = session.query(AllTweets).filter(
                        AllTweets.id == tweet['id']
                    ).first()
                    if existing_record:
                        self.logger.debug(
                            f"Tweet {tweet['id']} already exists in database "
                            f"(originally from @{existing_record.username})"
                        )
            
            self.logger.info(f"Filtered {len(new_tweets)} new tweets out of {len(tweets)} total for @{username}")
            return new_tweets
            
        except SQLAlchemyError as e:
            self.logger.error(f"Database error in filter_new_tweets: {e}")
            return tweets  # エラー時は全ツイートを返す（安全側に倒す）
        finally:
            session.close()
    
    def save_all_tweets(self, tweets: List[Dict[str, Any]], username: str) -> int:
        """全ツイートをall_tweetsテーブルに保存"""
        session = self._get_session()
        saved_count = 0
        
        try:
            for tweet_data in tweets:
                # 既存チェック
                existing = session.query(AllTweets).filter(
                    AllTweets.id == tweet_data['id']
                ).first()
                
                if existing:
                    continue
                
                # 新規レコードを作成
                tweet_record = AllTweets(
                    id=tweet_data['id'],
                    username=username,
                    display_name=tweet_data.get('display_name', username),
                    tweet_text=tweet_data['text'],
                    tweet_date=datetime.fromisoformat(tweet_data['date'].replace('Z', '+00:00')),
                    tweet_url=tweet_data['url'],
                    media_urls=json.dumps(tweet_data.get('media', [])),
                    local_media=json.dumps(tweet_data.get('local_media', [])),
                    huggingface_urls=json.dumps(tweet_data.get('huggingface_urls', [])),  # HF URLsを初期化
                    checked_for_event=False  # まだイベント検査していない
                )
                
                session.add(tweet_record)
                saved_count += 1
            
            session.commit()
            return saved_count
            
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Database error in save_all_tweets: {e}")
            return 0
        finally:
            session.close()
    
    def save_event_tweets(self, event_tweets: List[Dict[str, Any]], username: str):
        """イベント関連ツイートをevent_tweetsテーブルに保存"""
        session = self._get_session()
        saved_count = 0
        
        try:
            for tweet_data in event_tweets:
                # 既にevent_tweetsに存在するかチェック
                existing = session.query(EventTweet).filter(
                    EventTweet.id == tweet_data['id']
                ).first()
                
                if existing:
                    self.logger.debug(f"Tweet {tweet_data['id']} already exists in event_tweets")
                    continue
                
                # イベント情報を抽出
                event_info = tweet_data.get('event_analysis', {})
                
                # 新規レコードを作成
                tweet_record = EventTweet(
                    id=tweet_data['id'],
                    username=username,
                    display_name=tweet_data.get('display_name', username),
                    tweet_text=tweet_data['text'],
                    tweet_date=datetime.fromisoformat(tweet_data['date'].replace('Z', '+00:00')),
                    tweet_url=tweet_data['url'],
                    is_event_related=True,
                    event_type=event_info.get('event_type'),
                    event_date=event_info.get('event_date'),
                    participation_type=event_info.get('participation_type'),
                    confidence_score=str(event_info.get('confidence', 1.0)),
                    media_urls=json.dumps(tweet_data.get('media', [])),
                    local_media=json.dumps(tweet_data.get('local_media', [])),
                    analysis_result=json.dumps(event_info)
                )
                
                # スペース番号やサークル名を抽出
                from .event_detector import EventDetector
                detector = EventDetector(self.config)
                extracted_info = detector.extract_event_info(tweet_data)
                tweet_record.space_number = extracted_info.get('space_number')
                tweet_record.circle_name = extracted_info.get('circle_name')
                
                session.add(tweet_record)
                saved_count += 1
                
                # all_tweetsのchecked_for_eventフラグをTrueに更新
                all_tweet = session.query(AllTweets).filter(
                    AllTweets.id == tweet_data['id']
                ).first()
                if all_tweet:
                    all_tweet.checked_for_event = True
            
            session.commit()
            self.logger.info(f"Saved {saved_count} event tweets to database")
            
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Database error in save_event_tweets: {e}")
        finally:
            session.close()
    
    def save_tweets(self, tweets: List[Dict[str, Any]], username: str):
        """互換性のために残す（save_event_tweetsを呼び出す）"""
        self.save_event_tweets(tweets, username)
    
    def get_unnotified_tweets(self) -> List[EventTweet]:
        """未通知のツイートを取得"""
        session = self._get_session()
        
        try:
            tweets = session.query(EventTweet).filter(
                EventTweet.notified == False
            ).order_by(EventTweet.tweet_date.desc()).all()
            
            return tweets
            
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to get unnotified tweets: {e}")
            return []
        finally:
            session.close()
    
    def get_latest_tweet_date(self, username: str) -> Optional[datetime]:
        """指定ユーザーの最新ツイート日付を取得"""
        session = self._get_session()
        try:
            latest_tweet = session.query(AllTweets).filter(
                AllTweets.username == username
            ).order_by(AllTweets.tweet_date.desc()).first()
            
            if latest_tweet:
                # データベースの日時はタイムゾーンなしなので、UTCとして扱う
                if latest_tweet.tweet_date.tzinfo is None:
                    return latest_tweet.tweet_date.replace(tzinfo=timezone.utc)
                return latest_tweet.tweet_date
            return None
            
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to get latest tweet date for {username}: {e}")
            return None
        finally:
            session.close()
    
    def get_latest_tweet_id(self, username: str) -> Optional[str]:
        """指定ユーザーの最新ツイートIDを取得"""
        session = self._get_session()
        try:
            latest_tweet = session.query(AllTweets).filter(
                AllTweets.username == username
            ).order_by(AllTweets.tweet_date.desc()).first()
            
            if latest_tweet:
                return latest_tweet.id
            return None
            
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to get latest tweet ID for {username}: {e}")
            return None
        finally:
            session.close()
    
    def get_existing_tweet_ids(self, username: str) -> set:
        """指定ユーザーの既存ツイートIDセットを取得（重複チェック用）"""
        session = self._get_session()
        try:
            # all_tweetsテーブルから該当ユーザーの全ツイートIDを取得
            tweet_ids = session.query(AllTweets.id).filter(
                AllTweets.username == username
            ).all()
            
            # セットに変換して返す
            return {tweet_id[0] for tweet_id in tweet_ids}
            
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to get existing tweet IDs for {username}: {e}")
            return set()
        finally:
            session.close()
    
    def update_all_tweet_hf_urls(self, tweet_id: str, huggingface_urls: List[str]):
        """all_tweetsテーブルのHugging Face URLsを更新"""
        session = self._get_session()
        
        try:
            tweet = session.query(AllTweets).filter(
                AllTweets.id == tweet_id
            ).first()
            
            if tweet:
                tweet.huggingface_urls = json.dumps(huggingface_urls)
                session.commit()
                self.logger.debug(f"Updated HF URLs for tweet {tweet_id}")
            else:
                self.logger.warning(f"Tweet {tweet_id} not found in all_tweets")
                
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Failed to update HF URLs for tweet {tweet_id}: {e}")
        finally:
            session.close()
    
    def mark_as_notified(self, tweet_id: str):
        """ツイートを通知済みとしてマーク"""
        session = self._get_session()
        
        try:
            tweet = session.query(EventTweet).filter(
                EventTweet.id == tweet_id
            ).first()
            
            if tweet:
                tweet.notified = True
                session.commit()
                self.logger.debug(f"Marked tweet {tweet_id} as notified")
            else:
                self.logger.warning(f"Tweet {tweet_id} not found in database")
                
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Failed to mark tweet as notified: {e}")
        finally:
            session.close()
    
    def get_recent_events(self, days: int = 30) -> List[Dict[str, Any]]:
        """最近のイベント情報を取得（統計用）"""
        session = self._get_session()
        
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            
            tweets = session.query(EventTweet).filter(
                EventTweet.tweet_date >= cutoff_date
            ).order_by(EventTweet.tweet_date.desc()).all()
            
            # 辞書形式に変換
            results = []
            for tweet in tweets:
                results.append({
                    'id': tweet.id,
                    'username': tweet.username,
                    'display_name': tweet.display_name,
                    'text': tweet.tweet_text,
                    'date': tweet.tweet_date.isoformat(),
                    'url': tweet.tweet_url,
                    'event_type': tweet.event_type,
                    'participation_type': tweet.participation_type,
                    'space_number': tweet.space_number,
                    'circle_name': tweet.circle_name
                })
            
            return results
            
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to get recent events: {e}")
            return []
        finally:
            session.close()
    
    def filter_log_only_tweets(self, tweets: List[Dict[str, Any]], username: str) -> List[Dict[str, Any]]:
        """ログ専用アカウントの新規ツイートのみをフィルタリング"""
        session = self._get_session()
        new_tweets = []
        
        try:
            # 既存のツイートIDを取得（log_only_tweetsテーブルから、ユーザー名に関係なく）
            existing_ids = set()
            existing_tweets = session.query(LogOnlyTweet.id).all()
            existing_ids = {tweet.id for tweet in existing_tweets}
            
            # 新規ツイートのみを抽出
            for tweet in tweets:
                if tweet['id'] not in existing_ids:
                    new_tweets.append(tweet)
                else:
                    # 既存のツイートの場合、どのユーザーから既に取得済みかを確認
                    existing_record = session.query(LogOnlyTweet).filter(
                        LogOnlyTweet.id == tweet['id']
                    ).first()
                    if existing_record:
                        self.logger.debug(
                            f"Log-only tweet {tweet['id']} already exists in database "
                            f"(originally from @{existing_record.username})"
                        )
            
            self.logger.info(f"Filtered {len(new_tweets)} new log-only tweets out of {len(tweets)} total for @{username}")
            return new_tweets
            
        except SQLAlchemyError as e:
            self.logger.error(f"Database error in filter_log_only_tweets: {e}")
            return tweets  # エラー時は全ツイートを返す（安全側に倒す）
        finally:
            session.close()
    
    def save_log_only_tweets(self, tweets: List[Dict[str, Any]], username: str) -> int:
        """ログ専用ツイートをlog_only_tweetsテーブルに保存"""
        session = self._get_session()
        saved_count = 0
        
        try:
            for tweet_data in tweets:
                # 既存チェック
                existing = session.query(LogOnlyTweet).filter(
                    LogOnlyTweet.id == tweet_data['id']
                ).first()
                
                if existing:
                    continue
                
                # 新規レコードを作成
                tweet_record = LogOnlyTweet(
                    id=tweet_data['id'],
                    username=username,
                    display_name=tweet_data.get('display_name', username),
                    tweet_text=tweet_data['text'],
                    tweet_date=datetime.fromisoformat(tweet_data['date'].replace('Z', '+00:00')),
                    tweet_url=tweet_data['url'],
                    media_urls=json.dumps(tweet_data.get('media', [])),
                    huggingface_urls=json.dumps(tweet_data.get('huggingface_urls', [])),
                    uploaded_to_hf=tweet_data.get('uploaded_to_hf', False)
                )
                
                session.add(tweet_record)
                saved_count += 1
            
            session.commit()
            self.logger.info(f"Saved {saved_count} log-only tweets to database")
            return saved_count
            
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Database error in save_log_only_tweets: {e}")
            return 0
        finally:
            session.close()
    
    def update_log_only_tweet_hf_urls(self, tweet_id: str, huggingface_urls: List[str]):
        """ログ専用ツイートのHugging Face URLを更新"""
        session = self._get_session()
        
        try:
            tweet = session.query(LogOnlyTweet).filter(
                LogOnlyTweet.id == tweet_id
            ).first()
            
            if tweet:
                tweet.huggingface_urls = json.dumps(huggingface_urls)
                tweet.uploaded_to_hf = True
                session.commit()
                self.logger.debug(f"Updated HF URLs for log-only tweet {tweet_id}")
            else:
                self.logger.warning(f"Log-only tweet {tweet_id} not found in database")
                
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Failed to update log-only tweet HF URLs: {e}")
        finally:
            session.close()
    
    def get_log_only_tweets(self, username: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """ログ専用ツイートを取得"""
        session = self._get_session()
        
        try:
            query = session.query(LogOnlyTweet)
            if username:
                query = query.filter(LogOnlyTweet.username == username)
            
            tweets = query.order_by(LogOnlyTweet.tweet_date.desc()).limit(limit).all()
            
            # 辞書形式に変換
            results = []
            for tweet in tweets:
                results.append({
                    'id': tweet.id,
                    'username': tweet.username,
                    'display_name': tweet.display_name,
                    'text': tweet.tweet_text,
                    'date': tweet.tweet_date.isoformat(),
                    'url': tweet.tweet_url,
                    'media_urls': json.loads(tweet.media_urls) if tweet.media_urls else [],
                    'huggingface_urls': json.loads(tweet.huggingface_urls) if tweet.huggingface_urls else [],
                    'uploaded_to_hf': tweet.uploaded_to_hf
                })
            
            return results
            
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to get log-only tweets: {e}")
            return []
        finally:
            session.close()