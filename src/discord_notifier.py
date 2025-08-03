import os
import logging
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, List
import json
import time

from discord_webhook import DiscordWebhook, DiscordEmbed
import requests
from dotenv import load_dotenv


class DiscordNotifier:
    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger("EventMonitor.Discord")
        self.webhook_url = os.getenv('DISCORD_WEBHOOK_URL')
        self.enabled = bool(self.webhook_url)
        
        if not self.enabled:
            self.logger.warning("Discord webhook URL is not configured. Discord notifications will be disabled.")
        
        self.notification_config = config.get('discord', {}).get('notification', {})
        
        # レート制限管理
        self.last_request_time = 0
        self.request_delay = 0.5  # 基本遅延（秒）
        self.rate_limit_delay = 0  # レート制限による追加遅延
        
    def _create_embed(self, tweet: Dict[str, Any], username: str, display_name: str) -> DiscordEmbed:
        """Discord埋め込みメッセージを作成"""
        # イベント情報を取得
        event_info = tweet.get('event_analysis', {})
        
        # タイトル
        title = f"🎪 {display_name} (@{username}) のイベント参加情報"
        
        # 説明文（ツイート本文）
        description = tweet['text']
        if len(description) > 1024:  # Discordの制限
            description = description[:1021] + "..."
        
        # 埋め込みを作成
        embed = DiscordEmbed(
            title=title,
            description=description,
            color=self.notification_config.get('embed_color', 0x1da1f2),  # Twitter青
            url=tweet['url']
        )
        
        # アカウント情報
        embed.set_author(
            name=f"{display_name} (@{username})",
            url=f"https://twitter.com/{username}",
            icon_url=f"https://unavatar.io/twitter/{username}"  # プロフィール画像の代替
        )
        
        # フィールドを追加
        if event_info.get('event_type'):
            embed.add_embed_field(
                name="イベント種別",
                value=event_info['event_type'],
                inline=True
            )
        
        if event_info.get('participation_type'):
            embed.add_embed_field(
                name="参加形態",
                value=event_info['participation_type'],
                inline=True
            )
        
        if event_info.get('event_date'):
            embed.add_embed_field(
                name="推定イベント日",
                value=event_info['event_date'],
                inline=True
            )
        
        # スペース番号やサークル名
        space_number = tweet.get('space_number') or event_info.get('space_number')
        if space_number:
            embed.add_embed_field(
                name="スペース",
                value=space_number,
                inline=True
            )
        
        circle_name = tweet.get('circle_name') or event_info.get('circle_name')
        if circle_name:
            embed.add_embed_field(
                name="サークル名",
                value=circle_name,
                inline=True
            )
        
        # 信頼度スコア
        confidence = event_info.get('confidence', 1.0)
        embed.add_embed_field(
            name="判定信頼度",
            value=f"{confidence:.0%}",
            inline=True
        )
        
        # タイムスタンプ
        tweet_date = datetime.fromisoformat(tweet['date'].replace('Z', '+00:00'))
        embed.set_timestamp(tweet_date.timestamp())
        
        # フッター
        embed.set_footer(
            text="EventMonitor",
            icon_url="https://abs.twimg.com/icons/apple-touch-icon-192x192.png"
        )
        
        return embed
    
    async def send_notification(self, tweet: Dict[str, Any], username: str, display_name: str):
        """Discord通知を送信"""
        if not self.enabled:
            self.logger.debug(f"Discord notification skipped (disabled) for tweet {tweet['id']}")
            return
        
        try:
            # Webhookを作成
            webhook = DiscordWebhook(
                url=self.webhook_url,
                username="EventMonitor",
                avatar_url="https://abs.twimg.com/icons/apple-touch-icon-192x192.png"
            )
            
            # メンション設定
            content = ""
            mention_role = self.notification_config.get('mention_role')
            if mention_role:
                if mention_role in ['@everyone', '@here']:
                    content = mention_role
                else:
                    content = f"<@&{mention_role}>"  # ロールID
            
            if content:
                webhook.content = content
            
            # 埋め込みを作成
            embed = self._create_embed(tweet, username, display_name)
            webhook.add_embed(embed)
            
            # メディア（画像）がある場合
            media_urls = tweet.get('media', [])
            if media_urls and len(media_urls) > 0:
                # 最初の画像を埋め込みに追加
                embed.set_image(url=media_urls[0])
                
                # 複数画像がある場合は追加の埋め込みを作成
                for i, media_url in enumerate(media_urls[1:4], 1):  # 最大4枚まで
                    additional_embed = DiscordEmbed(
                        title=f"画像 {i+1}",
                        color=self.notification_config.get('embed_color', 0x1da1f2)
                    )
                    additional_embed.set_image(url=media_url)
                    webhook.add_embed(additional_embed)
            
            # レート制限を考慮した送信
            await self._execute_with_rate_limit(webhook, tweet['id'])
                
        except Exception as e:
            self.logger.error(f"Error sending Discord notification: {e}")
            raise
    
    async def send_batch_notification(self, tweets: List[Dict[str, Any]], username: str, display_name: str):
        """複数のツイートをまとめて通知"""
        if not tweets:
            return
        
        try:
            webhook = DiscordWebhook(
                url=self.webhook_url,
                username="EventMonitor",
                avatar_url="https://abs.twimg.com/icons/apple-touch-icon-192x192.png"
            )
            
            # メンション設定
            content = f"**{display_name} (@{username})** から {len(tweets)} 件の新しいイベント情報が見つかりました！\n"
            mention_role = self.notification_config.get('mention_role')
            if mention_role:
                if mention_role in ['@everyone', '@here']:
                    content = f"{mention_role} " + content
                else:
                    content = f"<@&{mention_role}> " + content
            
            webhook.content = content
            
            # 各ツイートの埋め込みを作成（最大10件）
            for tweet in tweets[:10]:
                embed = self._create_embed(tweet, username, display_name)
                webhook.add_embed(embed)
            
            if len(tweets) > 10:
                # 10件を超える場合は追加の埋め込みで通知
                remaining_embed = DiscordEmbed(
                    title="その他",
                    description=f"他に {len(tweets) - 10} 件のイベント情報があります",
                    color=self.notification_config.get('embed_color', 0x1da1f2)
                )
                webhook.add_embed(remaining_embed)
            
            # レート制限を考慮した送信
            await self._execute_with_rate_limit(webhook, f"batch_{len(tweets)}_tweets", is_batch=True)
                
        except Exception as e:
            self.logger.error(f"Error sending batch Discord notification: {e}")
            raise
    
    async def send_error_notification(self, error_message: str, error_type: str = "Error"):
        """エラー通知を送信"""
        try:
            webhook = DiscordWebhook(
                url=self.webhook_url,
                username="EventMonitor",
                avatar_url="https://abs.twimg.com/icons/apple-touch-icon-192x192.png"
            )
            
            embed = DiscordEmbed(
                title=f"⚠️ EventMonitor {error_type}",
                description=error_message,
                color=0xFF0000  # 赤
            )
            
            embed.set_timestamp()
            embed.set_footer(text="EventMonitor Error Report")
            
            webhook.add_embed(embed)
            response = webhook.execute()
            
            if response.status_code != 200:
                self.logger.error(f"Failed to send error notification: {response.status_code}")
                
        except Exception as e:
            self.logger.error(f"Error sending error notification: {e}")
    
    def test_webhook(self):
        """Webhook接続をテスト"""
        try:
            webhook = DiscordWebhook(
                url=self.webhook_url,
                content="EventMonitor webhook test - 接続テスト成功！ ✅"
            )
            
            response = webhook.execute()
            
            if response.status_code == 200:
                self.logger.info("Discord webhook test successful")
                return True
            else:
                self.logger.error(f"Discord webhook test failed: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"Discord webhook test error: {e}")
            return False
    
    async def _execute_with_rate_limit(self, webhook: DiscordWebhook, identifier: str, is_batch: bool = False):
        """レート制限を考慮してWebhookを実行"""
        # 前回のリクエストからの経過時間を確認
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        # 必要な遅延時間を計算
        required_delay = max(self.request_delay, self.rate_limit_delay)
        
        if elapsed < required_delay:
            wait_time = required_delay - elapsed
            self.logger.debug(f"Waiting {wait_time:.2f} seconds before sending notification")
            await asyncio.sleep(wait_time)
        
        # リクエストを送信
        try:
            response = webhook.execute()
            self.last_request_time = time.time()
            
            if response.status_code == 200:
                if is_batch:
                    self.logger.info(f"Batch Discord notification sent: {identifier}")
                else:
                    self.logger.info(f"Discord notification sent for tweet {identifier}")
                # 成功した場合、レート制限遅延をリセット
                self.rate_limit_delay = 0
            elif response.status_code == 429:
                # レート制限エラーの処理
                retry_after = 1.0  # デフォルト値
                try:
                    error_data = response.json()
                    retry_after = error_data.get('retry_after', 1.0)
                    self.logger.warning(f"Discord rate limit hit. Retry after: {retry_after} seconds")
                except:
                    pass
                
                # レート制限遅延を更新
                self.rate_limit_delay = retry_after + 0.1  # 少し余裕を持たせる
                
                # 再試行
                await asyncio.sleep(retry_after)
                response = webhook.execute()
                self.last_request_time = time.time()
                
                if response.status_code == 200:
                    if is_batch:
                        self.logger.info(f"Batch Discord notification sent after retry: {identifier}")
                    else:
                        self.logger.info(f"Discord notification sent after retry for tweet {identifier}")
                else:
                    raise Exception(f"Failed after retry: {response.status_code}")
            else:
                self.logger.error(f"Failed to send Discord notification: {response.status_code}")
                raise Exception(f"Discord webhook returned status code: {response.status_code}")
                
        except Exception as e:
            self.logger.error(f"Error executing Discord webhook: {e}")
            raise