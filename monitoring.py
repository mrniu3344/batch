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


def update_wallet_balance(conn, logger: logging.Logger, balance: Decimal, balance2: Decimal, balance3: Decimal, trx1: Decimal, trx2: Decimal, trx3: Decimal, trx4: Decimal, trx5: Decimal, trx6: Decimal) -> None:
    conn.update(
        "system_configs",
        {"config_key": "pre_depth"},
        {"config_value": str(balance)},
        0,
        "monitoring.update_pre_depth",
        is_master=True,
    )
    conn.update(
        "system_configs",
        {"config_key": "cur_depth"},
        {"config_value": str(balance2)},
        0,
        "monitoring.update_cur_depth",
        is_master=True,
    )
    conn.update(
        "system_configs",
        {"config_key": "nxt_depth"},
        {"config_value": str(balance3)},
        0,
        "monitoring.update_nxt_depth",
        is_master=True,
    )
    conn.update(
        "system_configs",
        {"config_key": "pre_depth_trx"},
        {"config_value": str(trx1)},
        0,
        "monitoring.update_pre_depth_trx",
        is_master=True,
    )
    conn.update(
        "system_configs",
        {"config_key": "cur_depth_trx"},
        {"config_value": str(trx2)},
        0,
        "monitoring.update_cur_depth_trx",
        is_master=True,
    )
    conn.update(
        "system_configs",
        {"config_key": "nxt_depth_trx"},
        {"config_value": str(trx3)},
        0,
        "monitoring.update_nxt_depth_trx",
        is_master=True,
    )
    conn.update(
        "system_configs",
        {"config_key": "pre_source_trx"},
        {"config_value": str(trx4)},
        0,
        "monitoring.update_pre_source_trx",
        is_master=True,
    )
    conn.update(
        "system_configs",
        {"config_key": "cur_source_trx"},
        {"config_value": str(trx5)},
        0,
        "monitoring.update_cur_source_trx",
        is_master=True,
    )
    conn.update(
        "system_configs",
        {"config_key": "nxt_source_trx"},
        {"config_value": str(trx6)},
        0,
        "monitoring.update_nxt_source_trx",
        is_master=True,
    )


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

        # rows = fetch_failed_deposits(conn, last_monitoring)
        # logger.info(f"Found {len(rows)} failed deposit records since last monitoring.")

        # notifier = NotificationService(logger, slack_webhook_url=constants.slack_webhook_url["tixian"])
        # for row in rows:
        #     message = notifier.format_notification(row)
        #     logger.info(f"Prepared notification: {message}")
        #     notifier.send_slack(message)

        # Large withdrawal monitoring
        # threshold = fetch_large_amount_threshold(conn)
        # if threshold is not None:
        #     large_withdrawal_rows = fetch_large_withdrawals(conn, last_monitoring, threshold)
        #     logger.info(f"Found {len(large_withdrawal_rows)} large withdrawal records since last monitoring.")
            
        #     large_withdrawal_notifier = NotificationService(logger, slack_webhook_url=constants.slack_webhook_url["tixian"])
        #     for row in large_withdrawal_rows:
        #         message = large_withdrawal_notifier.format_large_withdrawal_notification(row)
        #         logger.info(f"Prepared large withdrawal notification: {message}")
        #         large_withdrawal_notifier.send_slack(message)
        # else:
        #     logger.warning("Large withdrawal threshold not found in system_configs, skipping large withdrawal monitoring.")

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
        wallet_service = WalletService(logger)
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
                    
                    # 检查to_address的钱包余额，如果余额大于500U才处理后续call api
                    to_address = record.get("to_address")
                    should_call_api = False
                    if to_address:
                        try:
                            balance_info = wallet_service.audit_wallet(to_address)
                            if balance_info:
                                usdt_balance_min_unit = balance_info.get('usdt_balance', Decimal('0'))
                                # 500 USDT = 500 * 1,000,000 = 500,000,000（最小单位）
                                if merged_level == 'None':
                                    threshold = Decimal('10000000')
                                elif merged_level in ['Low', 'Unknown']:
                                    threshold = Decimal('100000000')
                                else:
                                    threshold = Decimal('20000000000')
                                
                                if usdt_balance_min_unit > threshold:
                                    should_call_api = True
                                    logger.info(f"Deposit record {record_id} to_address {to_address} balance {usdt_balance_min_unit} > {threshold}, will call API")
                                else:
                                    logger.info(f"Deposit record {record_id} to_address {to_address} balance {usdt_balance_min_unit} <= {threshold}, skip API call")
                            else:
                                logger.warning(f"Deposit record {record_id} failed to get balance info for to_address {to_address}, skip API call")
                        except Exception as e:
                            logger.error(f"Deposit record {record_id} error checking to_address {to_address} balance: {e}")
                            import traceback
                            logger.error(f"Traceback: {traceback.format_exc()}")
                    else:
                        logger.warning(f"Deposit record {record_id} has no to_address, skip API call")
                    
                    # 根据合并后的风险级别调用相应的API（仅在余额大于500U时）
                    api_endpoint = None
                    if merged_level == 'None':
                        api_endpoint = constants.risk_api_endpoints["low"]
                    elif merged_level in ['Low', 'Unknown']:
                        api_endpoint = constants.risk_api_endpoints["moderate"]
                    elif merged_level in ['Moderate', 'High']:
                        api_endpoint = constants.risk_api_endpoints["high"]
                    
                    if api_endpoint and should_call_api and to_address:
                        try:
                            # 调用API，将to_address作为URL路径的一部分
                            api_url = f"{api_endpoint}/{to_address}"
                            response = requests.post(
                                api_url,
                                timeout=300
                            )
                            logger.info(f"Deposit record {record_id} API call response - Status: {response.status_code}, URL: {api_url}")
                        except Exception as e:
                            logger.error(f"Error calling API for deposit record {record_id}: {e}")
                    else:
                        logger.warning(f"Deposit record {record_id} merged level {merged_level} has no corresponding API endpoint")
                        
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

        # audit函数现在自己管理连接和事务，按用户单位拆分
        audit(logger, mode)
    
        # 1. 从system_configs表取得上次执行时取得的钱包金额
        # pre_wallet_balance = fetch_pre_wallet_balance(conn)
        # logger.info(f"Previous wallet balance: {pre_wallet_balance}")

        # 2. 使用wallet_service.audit_wallet查询钱包余额
        wallet_address = constants.wallet_address
        wallet_address2 = constants.wallet_address2
        wallet_address3 = constants.wallet_address3
        wallet_address4 = constants.wallet_address4
        wallet_address5 = constants.wallet_address5
        wallet_address6 = constants.wallet_address6
        wallet_service = WalletService(logger)
        balance_info = wallet_service.audit_wallet(wallet_address)
        balance_info2 = wallet_service.audit_wallet(wallet_address2)
        balance_info3 = wallet_service.audit_wallet(wallet_address3)
        balance_info4 = wallet_service.audit_wallet(wallet_address4)
        balance_info5 = wallet_service.audit_wallet(wallet_address5)
        balance_info6 = wallet_service.audit_wallet(wallet_address6)
        
        # if balance_info is None:
        #     logger.error("Failed to get wallet balance info")
        #     conn.rollback()
        #     return

        current_usdt_balance = balance_info.get("usdt_balance", Decimal("0"))
        current_usdt_balance2 = balance_info2.get("usdt_balance", Decimal("0"))
        current_usdt_balance3 = balance_info3.get("usdt_balance", Decimal("0"))
        current_trx_balance = balance_info.get("trx_balance", Decimal("0"))
        current_trx_balance2 = balance_info2.get("trx_balance", Decimal("0"))
        current_trx_balance3 = balance_info3.get("trx_balance", Decimal("0"))
        current_trx_balance4 = balance_info4.get("trx_balance", Decimal("0"))
        current_trx_balance5 = balance_info5.get("trx_balance", Decimal("0"))
        current_trx_balance6 = balance_info6.get("trx_balance", Decimal("0"))
        logger.info(f"Current USDT balance: {current_usdt_balance}, {current_usdt_balance2}, {current_usdt_balance3}, {current_trx_balance}, {current_trx_balance2}, {current_trx_balance3}, {current_trx_balance4}, {current_trx_balance5}, {current_trx_balance6}")

        # 3. 如果usdt_balance减少1万以上，就推送slack消息
        # 注意：usdt_balance单位是最小单位，1万USDT = 10,000 * 1,000,000 = 10,000,000,000
        # threshold = Decimal("10000000000")  # 1万USDT的最小单位
        
        # if pre_wallet_balance is not None:
        #     decrease = pre_wallet_balance - current_usdt_balance
        #     logger.info(f"Balance decrease: {decrease}")

        #     notifier = NotificationService(logger, slack_webhook_url=constants.slack_webhook_url["qianbao"])
        #     notifier.send_slack("钱包警察巡逻中")
            
        #     d = decrease / Decimal("1000000")
        #     if decrease >= threshold:
        #         # 格式化金额显示（除以1,000,000转换为USDT）
        #         pre_balance_usdt = pre_wallet_balance / Decimal("1000000")
        #         current_balance_usdt = current_usdt_balance / Decimal("1000000")
                
        #         message = (
        #             f"主钱包提现预警，1小时前余额为{pre_balance_usdt}，"
        #             f"现在余额为{current_balance_usdt}，"
        #             f"总提现额{d}。"
        #         )
                
        #         notifier.send_slack(message)
        #         logger.info(f"Sent wallet withdrawal alert: {message}")
        # else:
        #     logger.info("No previous wallet balance found, skipping alert check")

        # 4. 将当前余额update进system_configs表
        update_wallet_balance(conn, logger, current_usdt_balance, current_usdt_balance2, current_usdt_balance3, current_trx_balance, current_trx_balance2, current_trx_balance3, current_trx_balance4, current_trx_balance5, current_trx_balance6)

        # 5. 监控主钱包的风险
        risk_service = RiskService(logger)
        notifier = NotificationService(logger, slack_webhook_url=constants.slack_webhook_url["fengxian"])
        
        # 监控所有6个钱包
        main_wallets = [
            ("high", wallet_address),
            ("moderate", wallet_address2),
            ("low", wallet_address3),
            ("high_trx", wallet_address4),
            ("moderate_trx", wallet_address5),
            ("low_trx", wallet_address6)
        ]
        
        for wallet_name, wallet_addr in main_wallets:
            try:
                logger.info(f"Checking risk for main wallet {wallet_name}: {wallet_addr}")
                risk_result = risk_service.assess_wallet_risk(wallet_addr)
                
                if risk_result:
                    risk_level = risk_result.get('risk_level', 'Unknown')
                    logger.info(f"Main wallet {wallet_name} risk assessment - Risk Level: {risk_level}")
                    
                    # 如果风险级别是 High 或 Severe，发送 Slack 通知
                    if risk_level in ['High', 'Severe']:
                        notifier.send_slack(f"注意！主钱包 {wallet_name} ({wallet_addr}) 风险过高！")
                        logger.info(f"Sent main wallet {wallet_name} high risk alert to Slack")
            except LPException as e:
                e.print()
                logger.error(f"Failed to check main wallet {wallet_name} risk: {e.error_function}, {e.error_detail}")
            except Exception as e:
                logger.error(f"Unexpected error checking main wallet {wallet_name} risk: {e}")
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

def audit(logger, mode):
    """
    审计用户钱包，按用户单位拆分事务，每处理完一个用户就提交一次
    这样可以避免长时间持有users表的锁
    
    Args:
        logger: 日志记录器
        mode: 运行模式 (dev/stg/prd)
    """
    logger.info("===== audit start =====")
    wallet_service = WalletService(logger)
    risk_service = RiskService(logger)
    notification_service = NotificationService(logger, slack_webhook_url=constants.slack_webhook_url["fengxian"])
    
    # 先获取需要审计的用户列表（使用独立连接，快速查询）
    db_service = DBService(logger, mode)
    list_conn = db_service.get_connection()
    try:
        user_service = UserService(logger)
        users = user_service.get_audit_users(list_conn)
        
        if not users:
            logger.info("没有需要审计的用户")
            return
        
        logger.info(f"开始审计 {len(users)} 个钱包，按用户单位拆分事务")
    finally:
        list_conn.commit()
    
    # 为每个用户创建独立的事务
    success_count = 0
    error_count = 0
    
    for user in users:
        user_conn = None
        try:
            # 为每个用户创建独立的连接和事务
            user_conn = db_service.get_connection()
            user_service = UserService(logger)
            
            # 处理单个用户的审计
            _audit_single_user(logger, user, user_conn, wallet_service, user_service, risk_service, notification_service)
            
            # 立即提交当前用户的事务
            user_conn.commit()
            success_count += 1
            logger.info(f"用户 {user.id} 审计完成并已提交")
            
        except LPException as e:
            # LPException 有详细的错误信息，使用 print() 方法记录
            e.print()
            logger.error(f"用户 {user.id} 钱包 {user.wallet} 审计失败 - 错误函数: {e.error_function}, 错误详情: {e.error_detail}")
            if user_conn:
                user_conn.rollback()
            error_count += 1
        except Exception as e:
            logger.error(f"用户 {user.id} 钱包 {user.wallet} 审计失败: {type(e).__name__}: {str(e)}")
            import traceback
            logger.error(f"详细错误信息: {traceback.format_exc()}")
            if user_conn:
                user_conn.rollback()
            error_count += 1
        finally:
            # 确保连接关闭
            if user_conn:
                try:
                    user_conn.commit(holdConnection=False)
                except:
                    pass

    try:
        conn = db_service.get_connection()
        sql = "SELECT sum(audited_usdt)/1000000 as total_usdt FROM users where id != 345"
        rows = conn.select(sql)
        if rows and rows[0].get("total_usdt") is not None:
            total_usdt = Decimal(str(rows[0]["total_usdt"]))
            result_usdt = Decimal("40000") - total_usdt
            result_min_unit = (result_usdt * Decimal("1000000")).quantize(Decimal("1"), rounding=ROUND_DOWN)
            conn.update(
                "users",
                {"id": 345},
                {"audited_usdt": str(result_min_unit)},
                0,
                "monitoring.update_user_345_audited_usdt",
                is_master=True,
            )
        else:
            logger.warning("Failed to get sum of audited_usdt from users table")
        conn.commit()
    except Exception as e:
        logger.error(f"Error updating user 345 audited_usdt: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        if conn:
            conn.rollback()

    logger.info(f"===== audit completed =====")
    logger.info(f"成功: {success_count} 个用户, 失败: {error_count} 个用户")


def _audit_single_user(logger, user, conn, wallet_service, user_service, risk_service, notification_service):
    """
    处理单个用户的审计操作
    
    Args:
        logger: 日志记录器
        user: 用户对象
        conn: 数据库连接（用于当前用户的事务）
        wallet_service: 钱包服务
        user_service: 用户服务
        risk_service: 风险服务
        notification_service: 通知服务
    """
    balance_info = wallet_service.audit_wallet(user.wallet)
    
    if balance_info:
        audited_usdt = balance_info['usdt_balance']
        audited_trx = balance_info['trx_balance']
        logger.info(f"用户 {user.id} 审计结果 - USDT: {audited_usdt}, TRX: {audited_trx}")
        
        user_service.update_audited_info(conn, user.id, audited_usdt, audited_trx, 0, "batch.daily_wallet_audit")
    else:
        logger.warning(f"用户 {user.id} 的钱包 {user.wallet} 审计失败，跳过更新")
    
    # 风险评估
    try:
        risk_result = risk_service.assess_wallet_risk(user.wallet)
        if risk_result:
            score = risk_result.get('score', 0)
            risk_level = risk_result.get('risk_level', 'Unknown')
            hacking_event = risk_result.get('hacking_event', '')
            detail_list = risk_result.get('detail_list', [])
            risk_detail = risk_result.get('risk_detail', [])
            
            logger.info(f"用户 {user.id} 风险评估结果 - Score: {score}, Risk Level: {risk_level}")
            
            user_service.update_risk_info(
                conn,
                user.id,
                score,
                risk_level,
                0,
                "batch.daily_wallet_audit",
                hacking_event=hacking_event,
                detail_list=detail_list,
                risk_detail=risk_detail
            )
            
            # 钱包风险评估处理：合并用户钱包风险和deposit_records的风险
            try:
                # 1. 分析用户钱包风险
                user_wallet_level, user_wallet_binary_score = risk_service.analyseRisk(
                    score,
                    risk_level,
                    hacking_event,
                    detail_list,
                    risk_detail
                )
                logger.info(f"用户 {user.id} 钱包风险分析 - Level: {user_wallet_level}, Binary Score: {user_wallet_binary_score}")
                
                # 2. 查询deposit_records表，获取10个字段
                sql = """
                    SELECT id, score, risk_level, hacking_event, detail_list, risk_detail,
                           t_score, t_risk_level, t_hacking_event, t_detail_list, t_risk_detail
                    FROM deposit_records
                    WHERE user_id = %s AND status = 'completed'
                """
                deposit_records = conn.select(sql, (user.id,))
                logger.info(f"用户 {user.id} 找到 {len(deposit_records)} 条deposit_records记录")
                
                # 3. 合并所有风险
                merged_level = user_wallet_level
                merged_binary_score = user_wallet_binary_score
                
                import json as json_module
                for deposit_record in deposit_records:
                    # 分析deposit_records中的钱包风险
                    dr_score = deposit_record.get('score')
                    dr_risk_level = deposit_record.get('risk_level')
                    dr_hacking_event = deposit_record.get('hacking_event')
                    dr_detail_list_str = deposit_record.get('detail_list')
                    dr_risk_detail_str = deposit_record.get('risk_detail')
                    
                    # 解析JSON字符串
                    dr_detail_list = []
                    dr_risk_detail = []
                    try:
                        if dr_detail_list_str:
                            dr_detail_list = json_module.loads(dr_detail_list_str) if isinstance(dr_detail_list_str, str) else dr_detail_list_str
                        if dr_risk_detail_str:
                            dr_risk_detail = json_module.loads(dr_risk_detail_str) if isinstance(dr_risk_detail_str, str) else dr_risk_detail_str
                    except Exception as e:
                        logger.warning(f"解析deposit_record {deposit_record.get('id')} 的JSON字段失败: {e}")
                    
                    if dr_score is not None and dr_risk_level:
                        dr_wallet_level, dr_wallet_binary_score = risk_service.analyseRisk(
                            dr_score,
                            dr_risk_level,
                            dr_hacking_event or '',
                            dr_detail_list if isinstance(dr_detail_list, list) else [],
                            dr_risk_detail if isinstance(dr_risk_detail, list) else []
                        )
                        merged_level, merged_binary_score = risk_service.mergeRisk(
                            merged_level, merged_binary_score,
                            dr_wallet_level, dr_wallet_binary_score
                        )
                        logger.info(f"合并deposit_record钱包风险 - Level: {merged_level}, Binary Score: {merged_binary_score}")
                    
                    # 分析deposit_records中的交易风险
                    dr_t_score = deposit_record.get('t_score')
                    dr_t_risk_level = deposit_record.get('t_risk_level')
                    dr_t_hacking_event = deposit_record.get('t_hacking_event')
                    dr_t_detail_list_str = deposit_record.get('t_detail_list')
                    dr_t_risk_detail_str = deposit_record.get('t_risk_detail')
                    
                    # 解析JSON字符串
                    dr_t_detail_list = []
                    dr_t_risk_detail = []
                    try:
                        if dr_t_detail_list_str:
                            dr_t_detail_list = json_module.loads(dr_t_detail_list_str) if isinstance(dr_t_detail_list_str, str) else dr_t_detail_list_str
                        if dr_t_risk_detail_str:
                            dr_t_risk_detail = json_module.loads(dr_t_risk_detail_str) if isinstance(dr_t_risk_detail_str, str) else dr_t_risk_detail_str
                    except Exception as e:
                        logger.warning(f"解析deposit_record {deposit_record.get('id')} 的交易JSON字段失败: {e}")
                    
                    if dr_t_score is not None and dr_t_risk_level:
                        dr_tx_level, dr_tx_binary_score = risk_service.analyseRisk(
                            dr_t_score,
                            dr_t_risk_level,
                            dr_t_hacking_event or '',
                            dr_t_detail_list if isinstance(dr_t_detail_list, list) else [],
                            dr_t_risk_detail if isinstance(dr_t_risk_detail, list) else []
                        )
                        merged_level, merged_binary_score = risk_service.mergeRisk(
                            merged_level, merged_binary_score,
                            dr_tx_level, dr_tx_binary_score
                        )
                        logger.info(f"合并deposit_record交易风险 - Level: {merged_level}, Binary Score: {merged_binary_score}")
                
                logger.info(f"用户 {user.id} 最终合并风险 - Level: {merged_level}, Binary Score: {merged_binary_score}")
                
                # 4. 检查hw_risk_level是否是强制指定的
                existing_user = user_service.get_user(conn, user.id)
                if existing_user:
                    hw_risk_level = getattr(existing_user, 'hw_risk_level', None)
                    if hw_risk_level in ['to_Low', 'to_Moderate', 'toHigh']:
                        # 强制指定的情况下，不能变更风险级别，但需要更新score
                        # 保持强制指定的级别不变
                        final_level = hw_risk_level
                        logger.info(f"用户 {user.id} hw_risk_level是强制指定的 ({hw_risk_level})，保持级别不变，但更新score为 {merged_binary_score}")
                    else:
                        # 非强制指定，使用合并后的级别
                        final_level = merged_level
                        logger.info(f"用户 {user.id} hw_risk_level不是强制指定的，使用合并后的级别 {final_level}")
                else:
                    # 如果获取不到用户信息，使用合并后的级别
                    final_level = merged_level
                    logger.warning(f"无法获取用户 {user.id} 的信息，使用合并后的级别 {final_level}")
                
                # 5. 更新hw_score和hw_risk_level
                user_service.update_hw_risk_info(
                    conn,
                    user.id,
                    merged_binary_score,
                    final_level,
                    0,
                    "batch.daily_wallet_audit_hw_risk"
                )
                logger.info(f"用户 {user.id} hw_risk_info已更新 - hw_score: {merged_binary_score}, hw_risk_level: {final_level}")
                
            except Exception as e:
                logger.error(f"用户 {user.id} 钱包风险评估合并处理失败: {e}")
                import traceback
                logger.error(f"详细错误信息: {traceback.format_exc()}")
            
            # 如果风险级别是 High 或 Severe，发送 Slack 通知
            # if risk_level in ['High', 'Severe']:
            #     login_id = user.login_id or f"ID:{user.id}"
            #     message = notification_service.format_risk_notification(
            #         user_name=user.name,
            #         login_id=login_id,
            #         score=score,
            #         risk_level=risk_level,
            #         hacking_event=hacking_event,
            #         detail_list=detail_list,
            #         risk_detail=risk_detail
            #     )
            #     
            #     notification_service.send_slack(message)
            #     logger.info(f"用户 {user.id} 的高风险通知已发送到 Slack")
            # else:
            #     logger.warning(f"用户 {user.id} 的钱包 {user.wallet} 风险评估失败，跳过更新")
    except LPException as e:
        e.print()
        logger.error(f"钱包 {user.wallet} 风险评估失败 - 错误函数: {e.error_function}, 错误详情: {e.error_detail}")
        logger.warning(f"用户 {user.id} 的钱包 {user.wallet} 风险评估失败，跳过更新")
        raise  # 重新抛出异常，让外层处理
    except Exception as e:
        logger.error(f"钱包 {user.wallet} 风险评估失败: {type(e).__name__}: {str(e)}")
        import traceback
        logger.error(f"详细错误信息: {traceback.format_exc()}")
        logger.warning(f"用户 {user.id} 的钱包 {user.wallet} 风险评估失败，跳过更新")
        raise  # 重新抛出异常，让外层处理

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

