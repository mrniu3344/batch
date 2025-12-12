# -*- coding: utf-8 -*-
from models.lp_exception import LPException
from services.singleton_service import SingletonService
import requests
import json
import os
from typing import Dict, List, Optional, Any
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
        risk_detail: List[Dict],
        tx_id: Optional[str] = None,
        tx_score: Optional[int] = None,
        tx_risk_level: Optional[str] = None,
        tx_hacking_event: Optional[str] = None,
        tx_detail_list: Optional[List[str]] = None,
        tx_risk_detail: Optional[List[Dict]] = None,
        amount: Optional[Decimal] = None,
        created_at: Optional[Any] = None
    ) -> str:
        """
        格式化存款风险评估通知消息
        支持三种情况：1.钱包风险高+交易风险高，2.钱包风险高，3.交易风险高
        
        参数:
            user_name: 用户名称
            login_id: 用户登录ID
            from_address: 钱包地址
            score: 钱包风险评分
            risk_level: 钱包风险级别
            hacking_event: 钱包安全事件
            detail_list: 钱包风险描述列表
            risk_detail: 钱包详细风险信息列表
            tx_id: 交易ID（可选）
            tx_score: 交易风险评分（可选）
            tx_risk_level: 交易风险级别（可选）
            tx_hacking_event: 交易安全事件（可选）
            tx_detail_list: 交易风险描述列表（可选）
            tx_risk_detail: 交易详细风险信息列表（可选）
            amount: 存款金额（可选，需要除以1000000显示）
            created_at: 存款时间（可选，timestamptz类型）
        
        返回:
            str: 格式化后的消息
        """
        has_wallet_risk = risk_level in ['High', 'Severe']
        has_tx_risk = tx_risk_level and tx_risk_level in ['High', 'Severe']
        
        # 格式化存款金额
        amount_str = "未知"
        if amount is not None:
            try:
                amount_decimal = Decimal(str(amount)) / Decimal("1000000")
                amount_str = str(amount_decimal.quantize(Decimal("0.000001"), rounding=ROUND_DOWN))
            except (TypeError, ValueError):
                amount_str = "未知"
        
        # 格式化存款时间
        created_at_str = "未知"
        if created_at:
            try:
                created_at_dt = pendulum.instance(created_at) if created_at else None
                if created_at_dt:
                    if created_at_dt.tzinfo is None:
                        created_at_dt = created_at_dt.replace(tz="UTC")
                    created_at_tokyo = created_at_dt.in_timezone("Asia/Tokyo")
                    created_at_str = created_at_tokyo.format("YYYY/MM/DD HH:mm:ss")
            except Exception:
                created_at_str = "未知"
        
        message_lines = []
        
        if has_wallet_risk and has_tx_risk:
            # 情况1: 钱包风险高 + 交易风险高
            message_lines.append(f"{user_name}（{login_id}）存款使用的钱包和交易都有风险。")
            message_lines.append("")
            message_lines.append(f"存款金额：{amount_str} USDT")
            message_lines.append(f"存款时间：{created_at_str}")
            message_lines.append("")
            message_lines.append("【钱包风险】")
            message_lines.append(f"钱包：{from_address}")
            message_lines.append(f"分数：{score}")
            message_lines.append(f"风险级别：{risk_level}")
            message_lines.append(f"hacking_event：{hacking_event if hacking_event else '无'}")
            message_lines.append(f"detail_list：{', '.join(detail_list) if detail_list else '无'}")
            message_lines.append(f"risk_detail：{json.dumps(risk_detail, ensure_ascii=False) if risk_detail else '无'}")
            message_lines.append("")
            message_lines.append("【交易风险】")
            message_lines.append(f"交易ID：{tx_id}")
            message_lines.append(f"分数：{tx_score}")
            message_lines.append(f"风险级别：{tx_risk_level}")
            message_lines.append(f"hacking_event：{tx_hacking_event if tx_hacking_event else '无'}")
            message_lines.append(f"detail_list：{', '.join(tx_detail_list) if tx_detail_list else '无'}")
            message_lines.append(f"risk_detail：{json.dumps(tx_risk_detail, ensure_ascii=False) if tx_risk_detail else '无'}")
        elif has_wallet_risk:
            # 情况2: 钱包风险高
            message_lines.append(f"{user_name}（{login_id}）存款使用的钱包有风险。")
            message_lines.append("")
            message_lines.append(f"存款金额：{amount_str} USDT")
            message_lines.append(f"存款时间：{created_at_str}")
            message_lines.append("")
            message_lines.append(f"钱包：{from_address}")
            message_lines.append(f"分数：{score}")
            message_lines.append(f"风险级别：{risk_level}")
            message_lines.append(f"hacking_event：{hacking_event if hacking_event else '无'}")
            message_lines.append(f"detail_list：{', '.join(detail_list) if detail_list else '无'}")
            message_lines.append(f"risk_detail：{json.dumps(risk_detail, ensure_ascii=False) if risk_detail else '无'}")
        elif has_tx_risk:
            # 情况3: 交易风险高
            message_lines.append(f"{user_name}（{login_id}）存款使用的交易有风险。")
            message_lines.append("")
            message_lines.append(f"存款金额：{amount_str} USDT")
            message_lines.append(f"存款时间：{created_at_str}")
            message_lines.append("")
            message_lines.append(f"交易ID：{tx_id}")
            message_lines.append(f"分数：{tx_score}")
            message_lines.append(f"风险级别：{tx_risk_level}")
            message_lines.append(f"hacking_event：{tx_hacking_event if tx_hacking_event else '无'}")
            message_lines.append(f"detail_list：{', '.join(tx_detail_list) if tx_detail_list else '无'}")
            message_lines.append(f"risk_detail：{json.dumps(tx_risk_detail, ensure_ascii=False) if tx_risk_detail else '无'}")
        
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
