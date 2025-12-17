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
from services.notification_service import NotificationService
from services.user_service import UserService
from services.riskService import RiskService


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

        notifier = NotificationService(logger, slack_webhook_url=constants.slack_webhook_url["tixian"])
        for row in rows:
            message = notifier.format_notification(row)
            logger.info(f"Prepared notification: {message}")
            notifier.send_slack(message)

        # Large withdrawal monitoring
        threshold = fetch_large_amount_threshold(conn)
        if threshold is not None:
            large_withdrawal_rows = fetch_large_withdrawals(conn, last_monitoring, threshold)
            logger.info(f"Found {len(large_withdrawal_rows)} large withdrawal records since last monitoring.")
            
            large_withdrawal_notifier = NotificationService(logger, slack_webhook_url=constants.slack_webhook_url["tixian"])
            for row in large_withdrawal_rows:
                message = large_withdrawal_notifier.format_large_withdrawal_notification(row)
                logger.info(f"Prepared large withdrawal notification: {message}")
                large_withdrawal_notifier.send_slack(message)
        else:
            logger.warning("Large withdrawal threshold not found in system_configs, skipping large withdrawal monitoring.")

        # 检查存款记录的风险
        check_deposit_records_risk(logger, conn)

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


def check_deposit_records_risk(logger: logging.Logger, conn) -> None:
    """
    检查存款记录的风险
    检索deposit_records表，检查from_address的风险，如果是高风险则更新users表并发送通知
    """
    logger.info("===== checking deposit records risk =====")
    
    try:
        # 检索deposit_records表
        sql = """
            SELECT id, user_id, tx_id, amount, from_address, to_address, created_at
            FROM deposit_records
            WHERE status = 'completed' AND reviewed = false
        """
        deposit_records = conn.select(sql)
        
        if not deposit_records:
            logger.info("No unreviewed completed deposit records found")
            return
        
        logger.info(f"Found {len(deposit_records)} unreviewed completed deposit records")
        
        user_service = UserService(logger)
        risk_service = RiskService(logger)
        notification_service = NotificationService(logger, slack_webhook_url=constants.slack_webhook_url["fengxian"])
        
        for record in deposit_records:
            record_id = record.get("id")
            user_id = record.get("user_id")
            from_address = record.get("from_address")
            tx_id = record.get("tx_id")
            amount = record.get("amount")
            created_at = record.get("created_at")
            
            if not from_address:
                logger.warning(f"Deposit record {record_id} has no from_address, skipping")
                continue
            
            try:
                # 获取用户信息
                user = user_service.get_user(conn, user_id)
                if not user:
                    logger.warning(f"User {user_id} not found for deposit record {record_id}, skipping")
                    continue
                
                # 检查from_address的风险
                logger.info(f"Checking wallet risk for deposit record {record_id}, from_address: {from_address}")
                wallet_risk_result = None
                wallet_score = 0
                wallet_risk_level = 'Unknown'
                wallet_hacking_event = ''
                wallet_detail_list = []
                wallet_risk_detail = []
                
                try:
                    wallet_risk_result = risk_service.assess_wallet_risk(from_address)
                    if wallet_risk_result:
                        wallet_score = wallet_risk_result.get('score', 0)
                        wallet_risk_level = wallet_risk_result.get('risk_level', 'Unknown')
                        wallet_hacking_event = wallet_risk_result.get('hacking_event', '')
                        wallet_detail_list = wallet_risk_result.get('detail_list', [])
                        wallet_risk_detail = wallet_risk_result.get('risk_detail', [])
                        logger.info(f"Deposit record {record_id} wallet risk assessment - Score: {wallet_score}, Risk Level: {wallet_risk_level}")
                    else:
                        logger.warning(f"Failed to get wallet risk assessment for deposit record {record_id}")
                except Exception as e:
                    logger.error(f"Error assessing wallet risk for deposit record {record_id}: {e}")
                
                # 检查交易的风险（如果存在tx_id）
                tx_risk_result = None
                tx_score = None
                tx_risk_level = None
                tx_hacking_event = None
                tx_detail_list = None
                tx_risk_detail = None
                
                if tx_id:
                    logger.info(f"Checking transaction risk for deposit record {record_id}, tx_id: {tx_id}")
                    try:
                        tx_risk_result = risk_service.assess_transaction_risk(tx_id)
                        if tx_risk_result:
                            tx_score = tx_risk_result.get('score', 0)
                            tx_risk_level = tx_risk_result.get('risk_level', 'Unknown')
                            tx_hacking_event = tx_risk_result.get('hacking_event', '')
                            tx_detail_list = tx_risk_result.get('detail_list', [])
                            tx_risk_detail = tx_risk_result.get('risk_detail', [])
                            logger.info(f"Deposit record {record_id} transaction risk assessment - Score: {tx_score}, Risk Level: {tx_risk_level}")
                        else:
                            logger.warning(f"Failed to get transaction risk assessment for deposit record {record_id}")
                    except Exception as e:
                        logger.error(f"Error assessing transaction risk for deposit record {record_id}: {e}")
                
                # 判断是否需要发送通知
                # has_wallet_risk = wallet_risk_level in ['High', 'Severe']
                # has_tx_risk = tx_risk_level and tx_risk_level in ['High', 'Severe']
                
                # if has_wallet_risk or has_tx_risk:
                #     # 发送Slack通知
                #     login_id = user.login_id or f"ID:{user_id}"
                #     message = notification_service.format_deposit_risk_notification(
                #         user_name=user.name,
                #         login_id=login_id,
                #         from_address=from_address,
                #         score=wallet_score,
                #         risk_level=wallet_risk_level,
                #         hacking_event=wallet_hacking_event,
                #         detail_list=wallet_detail_list,
                #         risk_detail=wallet_risk_detail,
                #         tx_id=tx_id,
                #         tx_score=tx_score,
                #         tx_risk_level=tx_risk_level,
                #         tx_hacking_event=tx_hacking_event,
                #         tx_detail_list=tx_detail_list,
                #         tx_risk_detail=tx_risk_detail,
                #         amount=amount,
                #         created_at=created_at
                #     )
                    
                #     notification_service.send_slack(message)
                #     logger.info(f"Sent risk notification for deposit record {record_id}")
                
                # 更新deposit_records表的风险评估字段和reviewed状态
                import json as json_module
                
                # 准备更新数据
                # 处理列表和字典字段，转换为JSON字符串
                wallet_detail_list_json = None
                try:
                    if wallet_risk_result and wallet_detail_list:
                        wallet_detail_list_json = json_module.dumps(wallet_detail_list, ensure_ascii=False)
                except Exception as e:
                    logger.warning(f"Failed to serialize wallet detail_list for record {record_id}: {e}")
                
                wallet_risk_detail_json = None
                try:
                    if wallet_risk_result and wallet_risk_detail:
                        wallet_risk_detail_json = json_module.dumps(wallet_risk_detail, ensure_ascii=False)
                except Exception as e:
                    logger.warning(f"Failed to serialize wallet risk_detail for record {record_id}: {e}")
                
                tx_detail_list_json = None
                try:
                    if tx_risk_result and tx_detail_list:
                        tx_detail_list_json = json_module.dumps(tx_detail_list, ensure_ascii=False)
                except Exception as e:
                    logger.warning(f"Failed to serialize transaction detail_list for record {record_id}: {e}")
                
                tx_risk_detail_json = None
                try:
                    if tx_risk_result and tx_risk_detail:
                        tx_risk_detail_json = json_module.dumps(tx_risk_detail, ensure_ascii=False)
                except Exception as e:
                    logger.warning(f"Failed to serialize transaction risk_detail for record {record_id}: {e}")
                
                # 使用cur.execute执行UPDATE语句
                sql = """
                    UPDATE deposit_records 
                    SET reviewed = %s,
                        score = %s,
                        risk_level = %s,
                        hacking_event = %s,
                        detail_list = %s,
                        risk_detail = %s,
                        t_score = %s,
                        t_risk_level = %s,
                        t_hacking_event = %s,
                        t_detail_list = %s,
                        t_risk_detail = %s
                    WHERE id = %s
                """
                params = (
                    True,  # reviewed
                    wallet_score if wallet_risk_result else None,  # score
                    wallet_risk_level if wallet_risk_result and wallet_risk_level != 'Unknown' else None,  # risk_level
                    wallet_hacking_event if wallet_risk_result and wallet_hacking_event else None,  # hacking_event
                    wallet_detail_list_json,  # detail_list
                    wallet_risk_detail_json,  # risk_detail
                    tx_score if tx_risk_result else None,  # t_score
                    tx_risk_level if tx_risk_result and tx_risk_level and tx_risk_level != 'Unknown' else None,  # t_risk_level
                    tx_hacking_event if tx_risk_result and tx_hacking_event else None,  # t_hacking_event
                    tx_detail_list_json,  # t_detail_list
                    tx_risk_detail_json,  # t_risk_detail
                    record_id  # WHERE id
                )
                
                cur = conn.conn.cursor()
                cur.execute(sql, params)
                cur.close()
                logger.info(f"Updated deposit record {record_id} with risk assessment data and marked as reviewed")
                
                # 对钱包和交易都进行一次风险分析然后合并风险分析结果
                try:
                    # 分析钱包风险
                    wallet_level, wallet_binary_score = risk_service.analyseRisk(
                        wallet_score if wallet_risk_result else 0,
                        wallet_risk_level if wallet_risk_result else 'Unknown',
                        wallet_hacking_event if wallet_risk_result else '',
                        wallet_detail_list if wallet_risk_result else [],
                        wallet_risk_detail if wallet_risk_result else []
                    )
                    logger.info(f"Deposit record {record_id} wallet risk analysis - Level: {wallet_level}, Binary Score: {wallet_binary_score}")
                    
                    # 分析交易风险（如果存在）
                    tx_level = None
                    tx_binary_score = None
                    if tx_risk_result:
                        tx_level, tx_binary_score = risk_service.analyseRisk(
                            tx_score if tx_risk_result else 0,
                            tx_risk_level if tx_risk_result else 'Unknown',
                            tx_hacking_event if tx_risk_result else '',
                            tx_detail_list if tx_risk_result else [],
                            tx_risk_detail if tx_risk_result else []
                        )
                        logger.info(f"Deposit record {record_id} transaction risk analysis - Level: {tx_level}, Binary Score: {tx_binary_score}")
                        
                        # 合并钱包和交易风险
                        merged_level, merged_binary_score = risk_service.mergeRisk(
                            wallet_level, wallet_binary_score,
                            tx_level, tx_binary_score
                        )
                    else:
                        # 如果没有交易风险，直接使用钱包风险
                        merged_level = wallet_level
                        merged_binary_score = wallet_binary_score
                    
                    logger.info(f"Deposit record {record_id} merged risk - Level: {merged_level}, Binary Score: {merged_binary_score}")
                    
                    # 根据合并后的风险级别调用相应的API
                    # api_endpoint = None
                    # if merged_level == 'Low':
                    #     api_endpoint = constants.risk_api_endpoints["low"]
                    # elif merged_level in ['Moderate', 'Unknown']:
                    #     api_endpoint = constants.risk_api_endpoints["moderate"]
                    # elif merged_level == 'High':
                    #     api_endpoint = constants.risk_api_endpoints["high"]
                    
                    # if api_endpoint:
                    #     try:
                    #         # 获取to_address
                    #         to_address = record.get("to_address")
                    #         # 调用API，只传递to_address字段
                    #         response = requests.post(
                    #             api_endpoint,
                    #             json={
                    #                 "to_address": to_address
                    #             },
                    #             timeout=30
                    #         )
                    #         logger.info(f"Deposit record {record_id} API call response - Status: {response.status_code}, Endpoint: {api_endpoint}")
                    #     except Exception as e:
                    #         logger.error(f"Error calling API for deposit record {record_id}: {e}")
                    # else:
                    #     logger.warning(f"Deposit record {record_id} merged level {merged_level} has no corresponding API endpoint")
                        
                except Exception as e:
                    logger.error(f"Error in risk analysis and API call for deposit record {record_id}: {e}")
                    import traceback
                    logger.error(f"Traceback: {traceback.format_exc()}")
                
            except LPException as e:
                e.print()
                logger.error(f"Error processing deposit record {record_id}: {e.error_function}, {e.error_detail}")
            except Exception as e:
                logger.error(f"Unexpected error processing deposit record {record_id}: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
        
        logger.info("===== deposit records risk check completed =====")
        
    except Exception as e:
        logger.error(f"Error in check_deposit_records_risk: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")


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
        wallet_address = constants.wallet_address
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

            notifier = NotificationService(logger, slack_webhook_url=constants.slack_webhook_url["qianbao"])
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

        # 5. 监控主钱包的风险
        risk_service = RiskService(logger)
        notifier = NotificationService(logger, slack_webhook_url=constants.slack_webhook_url["fengxian"])
        
        try:
            logger.info(f"Checking risk for main wallet: {wallet_address}")
            risk_result = risk_service.assess_wallet_risk(wallet_address)
            
            if risk_result:
                risk_level = risk_result.get('risk_level', 'Unknown')
                logger.info(f"Main wallet risk assessment - Risk Level: {risk_level}")
                
                # 如果风险级别是 High 或 Severe，发送 Slack 通知
                if risk_level in ['High', 'Severe']:
                    notifier.send_slack("注意！主钱包风险过高！")
                    logger.info("Sent main wallet high risk alert to Slack")
        except LPException as e:
            e.print()
            logger.error(f"Failed to check main wallet risk: {e.error_function}, {e.error_detail}")
        except Exception as e:
            logger.error(f"Unexpected error checking main wallet risk: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")

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

