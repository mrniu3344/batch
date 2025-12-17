# -*- coding: utf-8 -*-
import logging.config
import argparse
import uuid
import traceback
import time
import schedule
import constants as constants
import base as base
import pendulum
from decimal import Decimal
from models.lp_exception import LPException
from services.db_service import DBService
from services.user_service import UserService
from services.deposit_service import DepositService
from services.demand_service import DemandService
from services.wallet_service import WalletService
from services.borrowing_service import BorrowingService
from services.user_fund_flow_service import UserFundFlowService
from services.riskService import RiskService
from services.notification_service import NotificationService
from utils.utils import Utils

def monthly(logger, mode, base_date, conn):
    logger.info(f"monthly: {base_date}")
    deposit_service = DepositService(logger)
    user_service = UserService(logger)
    user_fund_flow_service = UserFundFlowService(logger)
    
    # ユーザー情報を取得
    users = user_service.get_users(conn)
    users_dict = {user.id: user for user in users}
    
    deposits = deposit_service.get_deposits(conn)
    
    user_interest_totals = {}
    all_interests = []
    deposit_interest_flows = []
    
    for deposit in deposits:
        deposit_service.get_deposit_details(conn, deposit)
        interests, is_deposit_end = deposit.make_interests(base_date, logger)
        logger.info(f"deposit {deposit.id} 利息: {interests}")
        
        all_interests.extend(interests)
        
        # deposit単位でinterestsのamountを合計
        deposit_total_interest = Decimal('0')
        for interest in interests:
            uid = interest["uid"]
            amount = interest["amount"]
            if uid not in user_interest_totals:
                user_interest_totals[uid] = Decimal('0')
            user_interest_totals[uid] = user_interest_totals[uid] + Decimal(amount)
            deposit_total_interest += Decimal(amount)
        
        # deposit単位でuser_fund_flowを作成
        if deposit_total_interest > 0:
            user = users_dict.get(deposit.uid)
            if user:
                balance_after = user.point + deposit_total_interest
                flow = {
                    "user_id": deposit.uid,
                    "fund_type": "POINT",
                    "action": "brothers_deposit_interest",
                    "amount": deposit_total_interest,
                    "balance_after": balance_after,
                    "related_fund_type": None,
                    "related_amount": None,
                    "remark": "存款利息",
                    "related_flow_id": None,
                    "counter_side": None
                }
                deposit_interest_flows.append(flow)
                
                # ユーザーのpointを更新（次のdepositの計算で使用）
                user.point = balance_after
        
        # 預金が終了した場合、ステータスを更新
        if is_deposit_end:
            logger.info(f"deposit {deposit.uid}, {deposit.id} が終了しました。ステータスを更新します。")
            deposit_service.update_deposit_status(conn, deposit.uid, deposit.id, 0, "batch.monthly")

    deposit_service.save_deposit_interests(conn, all_interests, 0, "batch.save_deposit_interests")
    
    # user_fund_flowsを保存
    if deposit_interest_flows:
        user_fund_flow_service.save_deposit_interest_flows(conn, deposit_interest_flows, 0, "batch.monthly_deposit_interest_flows")
        logger.info(f"保存了 {len(deposit_interest_flows)} 条存款利息资金流水记录")
    
    for uid, total_interest in user_interest_totals.items():
        logger.info(f"用户 {uid} 获得利息总额: {total_interest}")
        user_service.update_point(conn, uid, total_interest, 0, "batch.monthly_interest_update")
    
def audit(logger, mode, base_date, conn):
    logger.info(f"audit: {base_date}")

    wallet_service = WalletService(logger)
    user_service = UserService(logger)
    risk_service = RiskService(logger)
    notification_service = NotificationService(logger, slack_webhook_url=constants.slack_webhook_url["fengxian"])
    
    users = user_service.get_audit_users(conn)
    
    if not users:
        logger.info("没有需要审计的用户")
        return
    
    logger.info(f"开始审计 {len(users)} 个钱包")
    
    for user in users:
        try:
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
                        
                    #     notification_service.send_slack(message)
                    #     logger.info(f"用户 {user.id} 的高风险通知已发送到 Slack")
                else:
                    logger.warning(f"用户 {user.id} 的钱包 {user.wallet} 风险评估失败，跳过更新")
            except LPException as e:
                e.print()
                logger.error(f"钱包 {user.wallet} 风险评估失败 - 错误函数: {e.error_function}, 错误详情: {e.error_detail}")
                logger.warning(f"用户 {user.id} 的钱包 {user.wallet} 风险评估失败，跳过更新")
            except Exception as e:
                logger.error(f"钱包 {user.wallet} 风险评估失败: {type(e).__name__}: {str(e)}")
                logger.error(f"详细错误信息: {traceback.format_exc()}")
                logger.warning(f"用户 {user.id} 的钱包 {user.wallet} 风险评估失败，跳过更新")
                
        except LPException as e:
            # LPException 有详细的错误信息，使用 print() 方法记录
            e.print()
            logger.error(f"钱包 {user.wallet} 审计失败 - 错误函数: {e.error_function}, 错误详情: {e.error_detail}")
            logger.warning(f"用户 {user.id} 的钱包 {user.wallet} 审计失败，跳过更新")
        except Exception as e:
            logger.error(f"钱包 {user.wallet} 审计失败: {type(e).__name__}: {str(e)}")
            logger.error(f"详细错误信息: {traceback.format_exc()}")
            logger.warning(f"用户 {user.id} 的钱包 {user.wallet} 审计失败，跳过更新")

def borrow(logger, mode, base_date, conn):
    logger.info(f"borrow: {base_date}")
    borrowing_service = BorrowingService(logger)
    borrowing_service.update_interest_status(conn, base_date)

    user_service = UserService(logger)
    user_fund_flow_service = UserFundFlowService(logger)
    users_dict = user_service.get_related_users(conn)
    
    all_incomes, all_flows = borrowing_service.distribute_incomes(conn, base_date, users_dict)
    
    if all_incomes:
        user_income_totals = {}
        for income in all_incomes:
            uid = income['uid']
            amount = Decimal(str(income['amount']))
            if uid not in user_income_totals:
                user_income_totals[uid] = Decimal('0')
            user_income_totals[uid] = user_income_totals[uid] + amount
        
        for uid, total_income in user_income_totals.items():
            logger.info(f"用户 {uid} 获得收入总额: {total_income}")
            user_service.update_point(conn, uid, total_income, 0, "batch.distribute_incomes")
        
        logger.info(f"成功更新 {len(user_income_totals)} 个用户的余额")
    
    # 处理flows的balance_after属性
    if all_flows:
        for flow in all_flows:
            user_id = flow['user_id']
            amount = Decimal(str(flow['amount']))
            
            if user_id in users_dict:
                user = users_dict[user_id]
                new_balance = user.point + amount
                flow['balance_after'] = new_balance
                user.point = new_balance
        
        # 保存flows到数据库
        user_fund_flow_service.save_deposit_interest_flows(conn, all_flows, 0, "batch.borrow_flows")
        logger.info(f"成功保存 {len(all_flows)} 条资金流水记录")
    
def check_deposit_details(logger, mode, base_date, conn):
    logger.info(f"check_deposit_details: {base_date}")
    
    deposit_service = DepositService(logger)
    utils = Utils()
    
    # NDYステータスの預金詳細を取得
    ndy_details = deposit_service.get_ndy_deposit_details(conn)
    
    if not ndy_details:
        logger.info("NDYステータスの預金詳細がありません")
        return
    
    logger.info(f"NDYステータスの預金詳細 {len(ndy_details)} 件をチェックします")
    
    # 使用传入的base_date而不是真实时间，以支持测试场景
    current_time = base_date
    overdue_count = 0
    
    for detail in ndy_details:
        if detail.deposit_limit is None:
            logger.warning(f"預金詳細 {detail.uid}-{detail.id}-{detail.installment} の期限日が設定されていません")
            continue
        
        # 現在時刻が期限日を過ぎているかチェック
        if current_time > detail.deposit_limit:
            logger.info(f"預金詳細 {detail.uid}-{detail.id}-{detail.installment} が期限切れです。期限日: {detail.deposit_limit}")
            
            try:
                deposit_service.update_deposit_detail_status(
                    conn, 
                    detail.uid, 
                    detail.id, 
                    detail.installment, 
                    "overdue", 
                    0, 
                    "batch.check_deposit_details"
                )
                overdue_count += 1
                logger.info(f"預金詳細 {detail.uid}-{detail.id}-{detail.installment} のステータスを overdue に更新しました")
            except Exception as e:
                logger.error(f"預金詳細 {detail.uid}-{detail.id}-{detail.installment} のステータス更新に失敗しました: {str(e)}")
    
    logger.info(f"期限切れの預金詳細 {overdue_count} 件のステータスを更新しました")


def process_demands(logger, mode, base_date, conn):
    """
    处理已到期的demand存款
    - 当base_date > demand_end时，处理status='begin'的记录：
      - 支付利息 + 退还存款 + 改status为done
    - status='end'的记录已通过画面处理，batch无需关心
    """
    logger.info(f"process_demands: {base_date}")
    
    demand_service = DemandService(logger)
    user_service = UserService(logger)
    user_fund_flow_service = UserFundFlowService(logger)
    
    # 获取所有已到期的demand记录
    expired_demands = demand_service.get_expired_demands(conn, base_date)
    
    if not expired_demands:
        logger.info("没有已到期的demand存款需要处理")
        return
    
    logger.info(f"找到 {len(expired_demands)} 条已到期的demand存款需要处理")
    
    # 获取所有用户信息
    users = user_service.get_users(conn)
    users_dict = {user.id: user for user in users}
    
    interest_flows = []
    deposit_end_flows = []
    
    for demand in expired_demands:
        logger.info(f"处理demand: uid={demand.uid}, id={demand.id}, status={demand.status.value}, amount={demand.amount}, interest={demand.interest}")
        
        user = users_dict.get(demand.uid)
        if not user:
            logger.warning(f"用户 {demand.uid} 不存在，跳过处理")
            continue
        
        if demand.amount is None or demand.amount == 0:
            logger.warning(f"demand {demand.uid}-{demand.id} 的amount为空或0，跳过处理")
            continue
        
        # 处理利息支付（所有查询到的demand都是status='begin'，需要支付利息）
        if demand.interest is not None and demand.interest > 0:
            # 更新用户point（加上利息）
            user_service.update_point(conn, demand.uid, demand.interest, 0, "batch.process_demands_interest")
            logger.info(f"用户 {demand.uid} 获得利息: {demand.interest}")
            
            # 更新内存中的用户对象，用于计算balance_after
            user.point = user.point + demand.interest
            
            # 创建资金流水记录
            balance_after = user.point
            interest_flow = {
                "user_id": demand.uid,
                "fund_type": "POINT",
                "action": "brothers_demand_interest",
                "amount": demand.interest,
                "balance_after": balance_after,
                "related_fund_type": None,
                "related_amount": None,
                "remark": "活期存款利息",
                "related_flow_id": None,
                "counter_side": None
            }
            interest_flows.append(interest_flow)
        
        # 处理存款退还
        # 更新用户point（加上存款金额）
        user_service.update_point(conn, demand.uid, demand.amount, 0, "batch.process_demands_deposit_end")
        logger.info(f"用户 {demand.uid} 退还存款: {demand.amount}")
        
        # 更新用户demand_balance（减去存款金额）
        user_service.update_demand_balance(conn, demand.uid, demand.amount, 0, "batch.process_demands_deposit_end")
        logger.info(f"用户 {demand.uid} demand_balance减少: {demand.amount}")
        
        # 更新内存中的用户对象，用于计算balance_after
        user.point = user.point + demand.amount
        user.demand_balance = user.demand_balance - demand.amount
        
        # 创建资金流水记录
        balance_after = user.point
        deposit_end_flow = {
            "user_id": demand.uid,
            "fund_type": "POINT",
            "action": "brothers_demand_deposit_end",
            "amount": demand.amount,
            "balance_after": balance_after,
            "related_fund_type": None,
            "related_amount": None,
            "remark": "活期存款结束退还",
            "related_flow_id": None,
            "counter_side": None
        }
        deposit_end_flows.append(deposit_end_flow)
        
        # 更新demand状态为done
        demand_service.update_demand_status(conn, demand.uid, demand.id, "done", 0, "batch.process_demands")
        logger.info(f"demand {demand.uid}-{demand.id} 状态已更新为done")
    
    # 保存资金流水记录
    if interest_flows:
        user_fund_flow_service.save_deposit_interest_flows(conn, interest_flows, 0, "batch.process_demands_interest_flows")
        logger.info(f"保存了 {len(interest_flows)} 条demand利息资金流水记录")
    
    if deposit_end_flows:
        user_fund_flow_service.save_deposit_interest_flows(conn, deposit_end_flows, 0, "batch.process_demands_deposit_end_flows")
        logger.info(f"保存了 {len(deposit_end_flows)} 条demand存款结束资金流水记录")
    
    logger.info(f"成功处理 {len(expired_demands)} 条已到期的demand存款")


def daily(logger, mode, base_date, conn):
    logger.info(f"daily: {base_date}")

    check_deposit_details(logger, mode, base_date, conn)
    
    borrow(logger, mode, base_date, conn)
    
    audit(logger, mode, base_date, conn)
    
    process_demands(logger, mode, base_date, conn)

def run(logger, mode, test_date: pendulum.DateTime = None):
    """
    执行midnight batch
    
    Args:
        logger: 日志记录器
        mode: 运行模式 (dev/stg/prd)
        test_date: 可选的测试日期（用于测试场景），如果提供则使用此日期而不是当前时间
    """
    logger.info("===================midnight_batch run begin===================")
    conn = None

    try:
        db_service = DBService(logger, mode)
        conn = db_service.get_connection()

        # 如果提供了测试日期，使用它；否则使用当前时间
        if test_date is not None:
            current_time = test_date
            logger.info(f"使用测试日期: {test_date.format('YYYY-MM-DD HH:mm:ss')}")
        else:
            current_time = pendulum.now()
        
        unix_milliseconds = int(current_time.timestamp() * 1000)

        utils = Utils()
        converted_date = utils.int_to_date(unix_milliseconds)
        date_only = converted_date.start_of('day')
        logger.info(f"当前时间Unix毫秒: {unix_milliseconds}, 转换后日期: {converted_date}, 仅日期: {date_only}")
        
        # 如果是每月第一天，执行monthly方法
        if date_only.day == 1:
            monthly(logger, mode, date_only, conn)

        daily(logger, mode, date_only, conn)

        conn.commit()
    except LPException as e:
        logger.info("===================midnight_batch error===================")
        e.print()
        if conn is not None:
            conn.rollback()

    except (KeyboardInterrupt, SystemExit) as e:
        logger.info("===================midnight_batch error===================")
        logger.error(e)
        if conn is not None:
            conn.rollback()

    except Exception as e:
        logger.info("===================midnight_batch error===================")
        logger.error(e)
        if conn is not None:
            conn.rollback()
    
    finally:
        logger.info("===================midnight_batch run completed===================")


if __name__ == "__main__":
    # 解析命令行参数（包括测试日期参数）
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
    parser.add_argument("-t", "--test-date", dest="test_date", default=None,
                       help="测试日期，格式: YYYY/MM/DD，用于测试场景（仅dev模式，生产环境无效）")
    parser.add_argument("-n", "--appName", dest="appName", help="app name")
    args = parser.parse_args()
    
    mode = args.m
    appName = args.appName
    
    logger = base.getLogger(mode, appName)
    logger.info("===================midnight_batch start===================")
    logger.debug(mode)

    # 如果提供了测试日期，解析它（仅dev模式有效）
    test_date = None
    if args.test_date:
        try:
            # 支持 YYYY/MM/DD 格式
            test_date = pendulum.parse(args.test_date, tz='Asia/Shanghai').start_of('day')
            logger.info(f"检测到测试日期参数: {test_date.format('YYYY/MM/DD')}")
        except Exception as e:
            logger.error(f"测试日期格式错误: {args.test_date}, 错误: {str(e)}")
            logger.error("期望格式: YYYY/MM/DD，将使用当前时间执行")
            test_date = None
    
    if mode == "dev":
        run(logger, mode, test_date)
        logger.info("===================midnight_batch end===================")
    else:
        # 19時に実行するようにスケジュールを設定
        schedule.every().day.at("19:00").do(run, logger, mode)
        logger.info("スケジュールを設定しました: 毎日19:00に実行")

        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("===================midnight_batch interrupted===================")
        except Exception as e:
            logger.error(f"スケジューラーでエラーが発生しました: {str(e)}")
            logger.info("===================midnight_batch error===================")
