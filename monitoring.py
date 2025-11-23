import argparse
import logging
import time
import os
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Optional

import pendulum
import requests
import schedule

import base
import constants
from models.lp_exception import LPException
from services.db_service import DBService
from services.wallet_service import WalletService


class NotificationService:
    def __init__(self, logger: logging.Logger, slack_webhook_url: Optional[str] = None):
        self.logger = logger
        self.slack_webhook_url = slack_webhook_url or os.getenv("SLACK_WEBHOOK_URL") or "os.getenv("SLACK_WEBHOOK_URL")"

    def send_slack(self, message: str) -> None:
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


def fetch_last_monitoring_timestamp(conn) -> int:
    sql = """
        SELECT config_value
        FROM system_configs
        WHERE config_key = %s
        LIMIT 1
    """
    rows = conn.select(sql, ("last_monitoring",))
    if not rows:
        return 0

    value = rows[0].get("config_value")
    if value is None:
        return 0

    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def fetch_failed_deposits(conn, last_monitoring: int) -> List[Dict[str, Optional[str]]]:
    sql = """
        SELECT
            dr.user_id,
            dr.amount,
            dr.created_at,
            dr.to_address,
            u.name,
            u.login_id
        FROM withdraw_records AS dr
        INNER JOIN users AS u ON u.id = dr.user_id
        WHERE dr.status = %s
          AND dr.created_at > to_timestamp(%s / 1000.0)
        ORDER BY dr.created_at ASC
    """
    rows = conn.select(sql, ("failed", last_monitoring))
    return rows


def format_notification(row: Dict[str, Optional[str]]) -> str:
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


def update_last_monitoring(conn, logger: logging.Logger) -> None:
    now_ms = int(pendulum.now("UTC").timestamp() * 1000)
    conn.update(
        "system_configs",
        {"config_key": "last_monitoring"},
        {"config_value": str(now_ms)},
        0,
        "monitoring.update_last_monitoring",
        is_master=True,
    )
    logger.info(f"Updated last_monitoring to {now_ms}")


def fetch_pre_wallet_balance(conn) -> Optional[Decimal]:
    sql = """
        SELECT config_value
        FROM system_configs
        WHERE config_key = %s
        LIMIT 1
    """
    rows = conn.select(sql, ("pre_depth",))
    if not rows:
        return None

    value = rows[0].get("config_value")
    if value is None:
        return None

    try:
        return Decimal(str(value))
    except (TypeError, ValueError):
        return None


def update_pre_wallet_balance(conn, logger: logging.Logger, balance: Decimal) -> None:
    conn.update(
        "system_configs",
        {"config_key": "pre_depth"},
        {"config_value": str(balance)},
        0,
        "monitoring.update_pre_depth",
        is_master=True,
    )
    logger.info(f"Updated pre_depth to {balance}")


def fetch_large_amount_threshold(conn) -> Optional[Decimal]:
    sql = """
        SELECT config_value
        FROM system_configs
        WHERE config_key = %s
        LIMIT 1
    """
    rows = conn.select(sql, ("withdraw_large_amount_threshold",))
    if not rows:
        return None

    value = rows[0].get("config_value")
    if value is None:
        return None

    try:
        return Decimal(str(value))
    except (TypeError, ValueError):
        return None


def fetch_large_withdrawals(conn, last_monitoring: int, threshold: Decimal) -> List[Dict[str, Optional[str]]]:
    sql = """
        SELECT
            dr.user_id,
            dr.amount,
            dr.created_at,
            dr.to_address,
            u.name,
            u.login_id
        FROM withdraw_records AS dr
        INNER JOIN users AS u ON u.id = dr.user_id
        WHERE dr.created_at > to_timestamp(%s / 1000.0)
          AND dr.amount >= %s
        ORDER BY dr.created_at ASC
    """
    rows = conn.select(sql, (last_monitoring, str(threshold)))
    return rows


def format_large_withdrawal_notification(row: Dict[str, Optional[str]]) -> str:
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


def run_monitoring(logger: logging.Logger, mode: str) -> None:
    logger.info("===== monitoring iteration start =====")
    conn = None

    try:
        db_service = DBService(logger, mode)
        conn = db_service.get_connection()

        last_monitoring = fetch_last_monitoring_timestamp(conn)
        logger.info(f"last_monitoring={last_monitoring}")

        rows = fetch_failed_deposits(conn, last_monitoring)
        logger.info(f"Found {len(rows)} failed deposit records since last monitoring.")

        notifier = NotificationService(logger)
        for row in rows:
            message = format_notification(row)
            logger.info(f"Prepared notification: {message}")
            notifier.send_slack(message)

        # Large withdrawal monitoring
        threshold = fetch_large_amount_threshold(conn)
        if threshold is not None:
            large_withdrawal_rows = fetch_large_withdrawals(conn, last_monitoring, threshold)
            logger.info(f"Found {len(large_withdrawal_rows)} large withdrawal records since last monitoring.")
            
            large_withdrawal_webhook = "os.getenv("SLACK_LARGE_WITHDRAWAL_WEBHOOK_URL")"
            large_withdrawal_notifier = NotificationService(logger, slack_webhook_url=large_withdrawal_webhook)
            for row in large_withdrawal_rows:
                message = format_large_withdrawal_notification(row)
                logger.info(f"Prepared large withdrawal notification: {message}")
                large_withdrawal_notifier.send_slack(message)
        else:
            logger.warning("Large withdrawal threshold not found in system_configs, skipping large withdrawal monitoring.")

        update_last_monitoring(conn, logger)
        conn.commit()
    except LPException as exc:
        logger.error("LPException occurred during monitoring.")
        exc.print()
        if conn:
            conn.rollback()
    except Exception as exc:
        logger.error(f"Unexpected error during monitoring: {exc}")
        if conn:
            conn.rollback()
    finally:
        logger.info("===== monitoring iteration end =====")


def run_hourly_monitoring(logger: logging.Logger, mode: str) -> None:
    logger.info("===== hourly monitoring iteration start =====")
    conn = None

    try:
        db_service = DBService(logger, mode)
        conn = db_service.get_connection()

        # 1. 从system_configs表取得上次执行时取得的钱包金额
        pre_wallet_balance = fetch_pre_wallet_balance(conn)
        logger.info(f"Previous wallet balance: {pre_wallet_balance}")

        # 2. 使用wallet_service.audit_wallet查询钱包余额
        wallet_address = "TCQKEmxNJuYoagbDkX4W5UZX9o3y5zocSS"
        wallet_service = WalletService(logger)
        balance_info = wallet_service.audit_wallet(wallet_address)
        
        if balance_info is None:
            logger.error("Failed to get wallet balance info")
            conn.rollback()
            return

        current_usdt_balance = balance_info.get("usdt_balance", Decimal("0"))
        logger.info(f"Current USDT balance: {current_usdt_balance}")

        # 3. 如果usdt_balance减少1万以上，就推送slack消息
        # 注意：usdt_balance单位是最小单位，1万USDT = 10,000 * 1,000,000 = 10,000,000,000
        threshold = Decimal("10000000000")  # 1万USDT的最小单位
        
        if pre_wallet_balance is not None:
            decrease = pre_wallet_balance - current_usdt_balance
            logger.info(f"Balance decrease: {decrease}")

            alert_webhook = "os.getenv("SLACK_WALLET_ALERT_WEBHOOK_URL")"
            notifier = NotificationService(logger, slack_webhook_url=alert_webhook)
            notifier.send_slack("钱包警察巡逻中")
            
            d = decrease / Decimal("1000000")
            if decrease >= threshold:
                # 格式化金额显示（除以1,000,000转换为USDT）
                pre_balance_usdt = pre_wallet_balance / Decimal("1000000")
                current_balance_usdt = current_usdt_balance / Decimal("1000000")
                
                message = (
                    f"主钱包提现预警，1小时前余额为{pre_balance_usdt}，"
                    f"现在余额为{current_balance_usdt}，"
                    f"总提现额{d}。"
                )
                
                notifier.send_slack(message)
                logger.info(f"Sent wallet withdrawal alert: {message}")
        else:
            logger.info("No previous wallet balance found, skipping alert check")

        # 4. 将当前余额update进system_configs表
        update_pre_wallet_balance(conn, logger, current_usdt_balance)

        conn.commit()
    except LPException as exc:
        logger.error("LPException occurred during hourly monitoring.")
        exc.print()
        if conn:
            conn.rollback()
    except Exception as exc:
        logger.error(f"Unexpected error during hourly monitoring: {exc}")
        if conn:
            conn.rollback()
    finally:
        logger.info("===== hourly monitoring iteration end =====")


def schedule_monitoring(logger: logging.Logger, mode: str) -> None:
    schedule.every(1).minutes.do(run_monitoring, logger, mode)
    logger.info("Scheduled monitoring job every 1 minute.")
    
    schedule.every(1).hours.do(run_hourly_monitoring, logger, mode)
    logger.info("Scheduled hourly monitoring job every 1 hour.")

    while True:
        schedule.run_pending()
        time.sleep(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-m",
        required=True,
        type=str,
        choices=[
            constants.env["development"],
            constants.env["staging"],
            constants.env["staging-aws"],
            constants.env["production"],
            constants.env["production-aws"],
        ],
    )
    parser.add_argument("-n", "--appName", dest="appName", help="app name")
    args = parser.parse_args()

    mode = args.m
    app_name = args.appName

    logger = base.getLogger(mode, app_name)
    logger.info("=================== monitoring start ===================")

    schedule_monitoring(logger, mode)

    logger.info("=================== monitoring end ===================")


if __name__ == "__main__":
    main()

