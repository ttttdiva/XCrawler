import os
import logging
import asyncio
from typing import List, Dict, Any, Optional
import json
import re
import time
from datetime import datetime, timedelta

import openai
import google.generativeai as genai
from dotenv import load_dotenv


class EventDetector:
    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger("EventMonitor.EventDetector")
        self.enabled = config['event_detection'].get('enabled', True)
        self.keywords = config['event_detection']['keywords']
        self.exclude_keywords = config['event_detection']['exclude_keywords']
        
        # LLMクライアントの初期化（有効な場合のみ）
        if self.enabled:
            self._initialize_llm_clients()
        else:
            self.logger.info("Event detection is disabled. Running as crawler only.")
        
        # Gemini用のレート制限
        self.gemini_last_request_time = {}
        self.gemini_request_count = {}
        self.gemini_quota_reset_time = datetime.now()
        
    def _initialize_llm_clients(self):
        """LLMクライアントを初期化"""
        # OpenAI
        openai_key = os.getenv('OPENAI_API_KEY')
        if openai_key:
            openai.api_key = openai_key
            self.openai_client = openai.OpenAI(api_key=openai_key)
        else:
            self.openai_client = None
            
        # Google Gemini
        google_key = os.getenv('GOOGLE_API_KEY')
        if google_key:
            genai.configure(api_key=google_key)
            self.gemini_models = {}
        else:
            self.gemini_models = {}
    
    def _quick_keyword_check(self, tweet_text: str) -> tuple[bool, list]:
        """キーワードによる簡易チェック"""
        text_lower = tweet_text.lower()
        
        # 除外キーワードチェック
        for exclude_kw in self.exclude_keywords:
            if exclude_kw.lower() in text_lower:
                self.logger.debug(f"Tweet excluded due to keyword: {exclude_kw}")
                return False, []
        
        # 含むべきキーワードチェック
        matched_keywords = []
        for keyword in self.keywords:
            if keyword.lower() in text_lower:
                matched_keywords.append(keyword)
                
        return len(matched_keywords) > 0, matched_keywords
    
    def _check_gemini_rate_limit(self, model_name: str) -> bool:
        """Geminiのレート制限をチェック"""
        now = datetime.now()
        
        # クォータリセット時間の確認（1分ごと）
        if now - self.gemini_quota_reset_time >= timedelta(minutes=1):
            self.gemini_request_count = {}
            self.gemini_quota_reset_time = now
        
        # モデル別のリクエスト数を確認（Free Tierは15 requests/minute）
        request_count = self.gemini_request_count.get(model_name, 0)
        if request_count >= 15:
            wait_time = 60 - (now - self.gemini_quota_reset_time).total_seconds()
            if wait_time > 0:
                self.logger.warning(f"Gemini rate limit reached for {model_name}. Waiting {wait_time:.1f} seconds")
                return False
        
        return True
    
    def _update_gemini_request_count(self, model_name: str):
        """Geminiのリクエスト数を更新"""
        self.gemini_request_count[model_name] = self.gemini_request_count.get(model_name, 0) + 1
    
    async def _analyze_with_llm(self, tweet: Dict[str, Any], model_name: str) -> Optional[Dict[str, Any]]:
        """LLMを使ってツイートを分析"""
        prompt = f"""以下のツイートがイベント（コミケ、コミティア、例大祭、オンリーイベントなど）への参加告知や関連情報かどうか判定してください。

ツイート本文:
{tweet['text']}

判定基準（これらの要素が含まれる場合のみイベント関連と判定）:
1. イベントへの参加予告・告知（「参加します」「出展します」など未来形の表現）
2. スペース番号やブース情報の告知（例：東A-12a、西れ-01b）
3. 新刊・頒布物の告知（イベントでの頒布を明示している場合）
4. イベント当日の実況（設営完了、在庫情報、列形成など）
5. イベント関連の委託情報（特定のイベントへの委託）
6. 自分の作品を通販に出品した告知（「通販開始しました」「BOOTHに登録しました」など）

必ず除外する内容:
1. 他人の作品の購入報告（「買いました」「購入しました」「ポチった」など）
2. イベント終了後の感想・報告（「参加しました」「楽しかった」など過去形）
3. 商業作品（漫画、アニメ、ゲーム等）への単なる反応・感想
4. 「参加」という単語があっても、イベント以外への参加（企画、配信、祭りの感想など）
5. 他人の通販商品へのリンクや宣伝のRT
6. イベントと無関係な日常ツイート

重要な判定ポイント:
- 自分の作品の告知か、他人の作品への反応かを区別する
- 「通販開始」「販売開始」などは自分の作品なら検知対象
- 「購入しました」「買いました」は除外対象
- 時制に注意：未来のイベントへの参加表明を優先する
- 「参加」という単語だけで判定せず、文脈を正確に理解する

判定結果をJSON形式で返してください:
{{
    "is_event_related": true/false,
    "confidence": 0.0-1.0,
    "event_type": "イベントの種類（コミケ、コミティアなど）",
    "event_date": "推定されるイベント日付（わからない場合はnull）",
    "participation_type": "参加形態（サークル参加/一般参加/委託/不明）",
    "reason": "判定理由の簡潔な説明"
}}"""
        
        try:
            if model_name.startswith('gpt') and self.openai_client:
                # OpenAI API
                response = self.openai_client.chat.completions.create(
                    model=model_name,
                    messages=[
                        {"role": "system", "content": "あなたはイベント参加情報を正確に判定するアシスタントです。"},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.3,
                    response_format={"type": "json_object"}
                )
                result_text = response.choices[0].message.content
                
            elif model_name.startswith('gemini') and self.gemini_models is not None:
                # Gemini API
                
                # レート制限チェック
                if not self._check_gemini_rate_limit(model_name):
                    # レート制限に達している場合はスキップ
                    return None
                
                try:
                    model = genai.GenerativeModel(model_name)
                    response = model.generate_content(
                        prompt,
                        generation_config=genai.types.GenerationConfig(
                            temperature=0.3,
                            response_mime_type="application/json"
                        )
                    )
                    result_text = response.text
                    
                    # リクエスト数を更新
                    self._update_gemini_request_count(model_name)
                    
                except Exception as e:
                    self.logger.error(f"Failed to create or use Gemini model {model_name}: {e}")
                    
                    # 429エラー（レート制限）の場合はリクエスト数を最大値に設定
                    if "429" in str(e) or "quota" in str(e).lower():
                        self.gemini_request_count[model_name] = 15
                    
                    return None
                
            else:
                self.logger.warning(f"Model {model_name} not available")
                return None
            
            # JSONパース
            result = json.loads(result_text)
            
            # リストが返された場合は最初の要素を取得
            if isinstance(result, list):
                if len(result) > 0:
                    result = result[0]
                else:
                    self.logger.error(f"Empty list returned from {model_name}")
                    return None
            
            # 必須フィールドの確認
            if not isinstance(result, dict):
                self.logger.error(f"Invalid response format from {model_name}: {type(result)}")
                return None
            
            return result
            
        except Exception as e:
            self.logger.error(f"LLM analysis failed with {model_name}: {e}")
            return None
    
    async def detect_event_tweets(self, tweets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """イベント関連ツイートを検出"""
        # イベント検出が無効な場合は空リストを返す
        if not self.enabled:
            self.logger.info("Event detection is disabled. Returning empty list.")
            return []
        
        event_tweets = []
        
        for tweet in tweets:
            # まずキーワードチェック
            has_keywords, matched_keywords = self._quick_keyword_check(tweet['text'])
            if not has_keywords:
                continue
            
            # LLMで詳細分析（フォールバック付き）
            analysis_result = None
            for model in self.config['models']:
                self.logger.debug(f"Analyzing tweet {tweet['id']} with {model}")
                analysis_result = await self._analyze_with_llm(tweet, model)
                if analysis_result:
                    break
            
            if not analysis_result:
                self.logger.warning(f"All LLM models failed for tweet {tweet['id']}")
                # LLMが全て失敗した場合、キーワードマッチのみで判定
                analysis_result = {
                    'is_event_related': True,
                    'confidence': 0.5,
                    'reason': 'Keyword match only (LLM unavailable)'
                }
            
            # イベント関連と判定された場合
            if analysis_result.get('is_event_related', False) and analysis_result.get('confidence', 0) >= 0.6:
                # 分析結果をツイートデータに追加
                tweet['event_analysis'] = analysis_result
                
                # Hydrusタグ用の情報を追加
                event_info = {
                    'detected_keywords': matched_keywords,
                    'detected_events': [],
                    'event_type': analysis_result.get('event_type'),
                    'event_date': analysis_result.get('event_date'),
                    'participation_type': analysis_result.get('participation_type')
                }
                
                # イベント名を抽出
                if event_info['event_type']:
                    event_info['detected_events'].append(event_info['event_type'])
                
                # スペース番号やサークル名も抽出
                extracted_info = self.extract_event_info(tweet)
                event_info.update(extracted_info)
                
                tweet['event_info'] = event_info
                event_tweets.append(tweet)
                self.logger.info(f"Event tweet detected: {tweet['id']} - {analysis_result['reason']}")
        
        self.logger.info(f"Detected {len(event_tweets)} event-related tweets out of {len(tweets)} total")
        return event_tweets
    
    def extract_event_info(self, tweet: Dict[str, Any]) -> Dict[str, Any]:
        """ツイートからイベント情報を抽出"""
        text = tweet['text']
        
        info = {
            'space_number': None,
            'circle_name': None
        }
        
        # スペース番号の抽出（例: "東A-12a", "西れ-01b"）
        space_pattern = r'[東西南北][A-Zあ-ん\d]+-?\d+[ab]?'
        space_match = re.search(space_pattern, text)
        if space_match:
            info['space_number'] = space_match.group()
        
        # サークル名の抽出（「」内の文字列）
        circle_pattern = r'「([^」]+)」'
        circle_matches = re.findall(circle_pattern, text)
        if circle_matches:
            # 最初に見つかったものをサークル名とする
            info['circle_name'] = circle_matches[0]
        
        return info