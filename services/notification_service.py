# -*- coding: utf-8 -*-
from models.lp_exception import LPException
from services.singleton_service import SingletonService
import requests
import json
import os
from typing import Dict, List, Optional
from decimal import Decimal, ROUND_DOWN
import pendulum


class NotificationService(SingletonService):
    def __init__(self, logger, slack_webhook_url: str = None):
        self.logger = logger
        self.slack_webhook_url = slack_webhook_url
    
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
    
    def format_notification(self, row: Dict[str, Optional[str]]) -> str:
        """
        格式化提现失败通知消息
        
        参数:
            row: 包含提现记录信息的字典
        
        返回:
            str: 格式化后的消息
        """
        created_at = row.get("created_at")
        created_at_dt = pendulum.instance(created_at) if created_at else pendulum.now("UTC")
        if created_at_dt.tzinfo is None:
            created_at_dt = created_at_dt.replace(tz="UTC")
        created_at_tokyo = created_at_dt.in_timezone("Asia/Tokyo")
        created_at_str = created_at_tokyo.format("YYYY/MM/DD HH:mm:ss")

        amount_raw = row.get("amount") or 0
        amount_decimal = Decimal(str(amount_raw))
        amount_formatted = amount_decimal.quantize(Decimal("0.000001"), rounding=ROUND_DOWN)

        name = row.get("name") or ""
        user_id = row.get("user_id") or ""
        login_id = row.get("login_id") or ""
        to_address = row.get("to_address") or ""

        return (
            f"{name}(ID:{user_id},登录号:{login_id})提现失败。"
            f"提现时间:{created_at_str}，金额:{amount_formatted}，目标钱包:{to_address}。"
        )
    
    def format_large_withdrawal_notification(self, row: Dict[str, Optional[str]]) -> str:
        """
        格式化大额提现通知消息
        
        参数:
            row: 包含提现记录信息的字典
        
        返回:
            str: 格式化后的消息
        """
        created_at = row.get("created_at")
        created_at_dt = pendulum.instance(created_at) if created_at else pendulum.now("UTC")
        if created_at_dt.tzinfo is None:
            created_at_dt = created_at_dt.replace(tz="UTC")
        created_at_tokyo = created_at_dt.in_timezone("Asia/Tokyo")
        created_at_str = created_at_tokyo.format("YYYY/MM/DD HH:mm:ss")

        amount_raw = row.get("amount") or 0
        amount_decimal = Decimal(str(amount_raw))
        amount_formatted = amount_decimal.quantize(Decimal("0.000001"), rounding=ROUND_DOWN)

        name = row.get("name") or ""
        user_id = row.get("user_id") or ""
        login_id = row.get("login_id") or ""
        to_address = row.get("to_address") or ""

        return (
            f"{name}(ID:{user_id},登录号:{login_id})大额提现。"
            f"提现时间:{created_at_str}，金额:{amount_formatted}，目标钱包:{to_address}。"
        )
