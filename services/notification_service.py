# -*- coding: utf-8 -*-
from models.lp_exception import LPException
from services.singleton_service import SingletonService
import requests
import json
import os
from typing import Dict, List, Optional


class NotificationService(SingletonService):
    def __init__(self, logger, slack_webhook_url: str = None):
        self.logger = logger
        self.slack_webhook_url = slack_webhook_url
    
    def send_slack_message(self, message: str, max_retries: int = 3, initial_delay: float = 1.0) -> bool:
        """
        发送 Slack 消息
        
        参数:
            message: 要发送的消息内容
            max_retries: 最大重试次数（默认3次）
            initial_delay: 初始延迟秒数（默认1秒）
        
        返回:
            bool: 是否发送成功
        """
        if not self.slack_webhook_url:
            self.logger.error("Slack webhook URL is not set")
            return False
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        payload = {
            "text": message
        }
        
        delay = initial_delay
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    self.logger.info(f"Retrying Slack message send (attempt {attempt + 1}/{max_retries + 1}) after {delay:.1f}s delay...")
                    import time
                    time.sleep(delay)
                    delay *= 2
                
                response = requests.post(
                    self.slack_webhook_url,
                    headers=headers,
                    json=payload,
                    timeout=10
                )
                
                if response.status_code == 429:
                    last_exception = requests.exceptions.HTTPError(f"Rate limit (429) on attempt {attempt + 1}")
                    if attempt < max_retries:
                        self.logger.warning(f"Rate limit hit (429) on attempt {attempt + 1}/{max_retries + 1}. Will retry...")
                        continue
                    else:
                        self.logger.error(f"Rate limit (429) after {max_retries + 1} attempts")
                        return False
                
                response.raise_for_status()
                
                if response.status_code == 200:
                    self.logger.info("Slack message sent successfully")
                    return True
                else:
                    self.logger.error(f"Slack API returned unexpected status code: {response.status_code}")
                    return False
                    
            except requests.exceptions.Timeout as e:
                last_exception = e
                if attempt < max_retries:
                    self.logger.warning(f"Request timeout on attempt {attempt + 1}/{max_retries + 1}. Will retry...")
                    continue
                else:
                    self.logger.error(f"Request timeout after {max_retries + 1} attempts: {e}")
                    return False
            except requests.exceptions.ConnectionError as e:
                last_exception = e
                if attempt < max_retries:
                    self.logger.warning(f"Connection error on attempt {attempt + 1}/{max_retries + 1}. Will retry...")
                    continue
                else:
                    self.logger.error(f"Connection error after {max_retries + 1} attempts: {e}")
                    return False
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Slack API request failed: {e}")
                return False
            except Exception as e:
                self.logger.error(f"Unexpected error while sending Slack message: {e}")
                return False
        
        if last_exception:
            self.logger.error(f"Failed to send Slack message after {max_retries + 1} attempts: {last_exception}")
        return False
    
    def send_slack(self, message: str) -> None:
        """
        发送 Slack 消息（简化版本，用于向后兼容）
        
        参数:
            message: 要发送的消息内容
        """
        if not self.slack_webhook_url:
            self.logger.warning("Slack notification skipped: missing webhook URL.")
            return

        headers = {
            "Content-Type": "application/json",
        }
        payload = {"text": message}

        try:
            response = requests.post(self.slack_webhook_url, headers=headers, json=payload, timeout=10)
            response.raise_for_status()
            self.logger.info(f"Sent Slack notification successfully. status={response.status_code}")
        except requests.exceptions.Timeout:
            self.logger.error("Slack notification timed out.")
        except requests.exceptions.HTTPError as exc:
            self.logger.error(f"Slack notification failed with status {exc.response.status_code}: {exc.response.text}")
        except Exception as exc:
            self.logger.error(f"Slack notification encountered an error: {exc}")
    
    def format_risk_notification(
        self, 
        user_name: str,
        login_id: str, 
        score: int, 
        risk_level: str, 
        hacking_event: str, 
        detail_list: List[str], 
        risk_detail: List[Dict]
    ) -> str:
        """
        格式化风险评估通知消息
        
        参数:
            user_name: 用户名称
            login_id: 用户登录ID
            score: 风险评分
            risk_level: 风险级别
            hacking_event: 安全事件
            detail_list: 风险描述列表
            risk_detail: 详细风险信息列表
        
        返回:
            str: 格式化后的消息
        """
        message_lines = [
            f"{user_name}（{login_id}）的个人钱包存在风险。",
            f"",
            f"风险评分：{score}",
            f"风险级别：{risk_level}",
            f"hacking_event：{hacking_event if hacking_event else '无'}",
            f"detail_list：{', '.join(detail_list) if detail_list else '无'}",
            f"risk_detail：{json.dumps(risk_detail, ensure_ascii=False) if risk_detail else '无'}"
        ]
        
        return "\n".join(message_lines)
    
    def format_deposit_risk_notification(
        self,
        user_name: str,
        login_id: str,
        from_address: str,
        score: int,
        risk_level: str,
        hacking_event: str,
        detail_list: List[str],
        risk_detail: List[Dict]
    ) -> str:
        """
        格式化存款风险评估通知消息
        
        参数:
            user_name: 用户名称
            login_id: 用户登录ID
            from_address: 钱包地址
            score: 风险评分
            risk_level: 风险级别
            hacking_event: 安全事件
            detail_list: 风险描述列表
            risk_detail: 详细风险信息列表
        
        返回:
            str: 格式化后的消息
        """
        message_lines = [
            f"{user_name}（{login_id}）存款使用的钱包有风险。",
            f"",
            f"钱包：{from_address}",
            f"分数：{score}",
            f"风险级别：{risk_level}",
            f"hacking_event：{hacking_event if hacking_event else '无'}",
            f"detail_list：{', '.join(detail_list) if detail_list else '无'}",
            f"risk_detail：{json.dumps(risk_detail, ensure_ascii=False) if risk_detail else '无'}"
        ]
        
        return "\n".join(message_lines)
