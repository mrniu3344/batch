# -*- coding: utf-8 -*-
from services.singleton_service import SingletonService
import pendulum
from utils.utils import Utils
from models.user import User
from decimal import Decimal
from typing import Dict, List


class BorrowingService(SingletonService):
    def __init__(self, logger):
        self.logger = logger

    def update_interest_status(self, conn, base_date: pendulum.DateTime):
        try:
            utils = Utils()
            base_timestamp = utils.date_to_int(base_date)
            
            select_sql = """
                SELECT uid, id, interest_from, interest_to 
                FROM borrowing_interests 
                WHERE status = 'NDY' AND interest_to < %s
            """
            
            records = conn.select(select_sql, (base_timestamp,))
            
            if not records:
                self.logger.info("没有需要更新的利息记录")
                return
            
            update_count = 0
            for record in records:
                keys = {
                    "uid": record["uid"],
                    "id": record["id"], 
                    "interest_from": record["interest_from"],
                    "interest_to": record["interest_to"]
                }
                
                update_data = {
                    "status": "overdue"
                }
                
                conn.update("borrowing_interests", keys, update_data, 0, "batch.update_interest_status")
                update_count += 1
            
            self.logger.info(f"成功更新 {update_count} 条利息记录状态为 overdue")
            
        except Exception as e:
            self.logger.error(f"更新利息状态失败: {str(e)}")
            raise

    def distribute_incomes(self, conn, base_date: pendulum.DateTime, users_dict: dict[int, User]):
        try:
            utils = Utils()
            
            system_configs_sql = "SELECT config_value FROM system_configs where config_key = 'guarantor_divid'"
            system_configs = conn.select(system_configs_sql)
            guarantor_divid = Decimal('0.000000')
            if system_configs:
                guarantor_divid = Decimal(str(system_configs[0].get('config_value', Decimal('0.000000'))))
            
            select_sql = """
                SELECT bi.uid, bi.id, bi.interest_from, bi.interest_to, bi.interest_date, bi.amount,
                       b.guarantor1, b.guarantor2, b.guarantor3
                FROM borrowing_interests bi
                JOIN borrowings b ON bi.uid = b.uid AND bi.id = b.id
                WHERE bi.status = 'repaid'
            """
            
            interest_records = conn.select(select_sql)
            
            if not interest_records:
                self.logger.info("没有需要分配的利息记录")
                return [], []
            
            all_incomes = []
            all_flows = []
            
            for record in interest_records:
                amount = Decimal(str(record['amount'])) if record['amount'] else Decimal('0')
                if amount <= Decimal('0'):
                    continue
                
                self.logger.info(f"处理利息记录: 用户{record['uid']}, 贷款ID{record['id']}, 金额{amount}")
                
                guarantor_incomes, guarantor_flows = self._distribute_to_guarantors(
                    record, amount, guarantor_divid, users_dict
                )
                all_incomes.extend(guarantor_incomes)
                all_flows.extend(guarantor_flows)
                
                guarantor_total = sum(Decimal(str(income['amount'])) for income in guarantor_incomes)
                remaining_amount = amount - guarantor_total
                
                if remaining_amount > Decimal('0'):
                    hierarchy_incomes, hierarchy_flows = self._distribute_to_hierarchy(
                        record, remaining_amount, users_dict
                    )
                    all_incomes.extend(hierarchy_incomes)
                    all_flows.extend(hierarchy_flows)
                
                keys = {
                    "uid": record['uid'],
                    "id": record['id'],
                    "interest_from": record['interest_from'],
                    "interest_to": record['interest_to']
                }
                update_data = {
                    "status": "distributed"
                }
                conn.update("borrowing_interests", keys, update_data, 0, "batch.distribute_incomes")
                self.logger.debug(f"更新利息记录状态为 distributed: 用户{record['uid']}, 贷款ID{record['id']}")
            
            if all_incomes:
                conn.insertMany("incomes", all_incomes, 0, "batch.distribute_incomes")
                self.logger.info(f"成功分配 {len(all_incomes)} 条收入记录")
            
            return all_incomes, all_flows
            
        except Exception as e:
            self.logger.error(f"分配收入失败: {str(e)}")
            raise
    
    def _distribute_to_guarantors(self, record: dict, amount: Decimal, guarantor_divid: Decimal, users_dict: Dict[int, User]) -> tuple[List[dict], List[dict]]:
        incomes = []
        flow_records = []
        
        guarantors = [
            record['guarantor1'],
            record['guarantor2'], 
            record['guarantor3']
        ]
        
        for guarantor_id in guarantors:
            if guarantor_id and guarantor_id in users_dict:
                guarantor_amount = (amount * guarantor_divid).quantize(Decimal('1'), rounding='ROUND_DOWN')
                
                if guarantor_amount > Decimal('0'):
                    income = {
                        "uid": guarantor_id,
                        "borrowing_uid": record['uid'],
                        "bid": record['id'],
                        "interest_from": record['interest_from'],
                        "interest_to": record['interest_to'],
                        "amount": guarantor_amount,
                        "is_guarantee": True
                    }
                    incomes.append(income)
                    
                    # 创建资金流水记录
                    flow_record = {
                        "user_id": guarantor_id,
                        "fund_type": "POINT",
                        "action": "brothers_guarantee_interest",
                        "amount": guarantor_amount,
                        "balance_after": guarantor_amount,
                        "related_fund_type": None,
                        "related_amount": None,
                        "remark": "担保收入",
                        "related_flow_id": None,
                        "counter_side": record['uid']
                    }
                    flow_records.append(flow_record)
                    
                    self.logger.info(f"担保人 {guarantor_id} 获得收入: {guarantor_amount}")
        
        return incomes, flow_records
    
    def _distribute_to_hierarchy(self, record: dict, amount: Decimal, users_dict: Dict[int, User]) -> tuple[List[dict], List[dict]]:
        incomes = []
        flow_records = []
        
        borrower_id = record['uid']
        if borrower_id not in users_dict:
            self.logger.warning(f"借款人 {borrower_id} 不在用户字典中")
            return incomes, flow_records
        
        borrower = users_dict[borrower_id]
        current_amount = amount
        
        current_user = borrower
        while current_user.parent and current_user.parent in users_dict:
            parent = users_dict[current_user.parent]
            
            if parent.parent_divid:
                parent_amount = (current_amount * parent.parent_divid).quantize(Decimal('1'), rounding='ROUND_DOWN')
                
                if parent_amount > Decimal('0'):
                    income = {
                        "uid": parent.id,
                        "borrowing_uid": record['uid'],
                        "bid": record['id'],
                        "interest_from": record['interest_from'],
                        "interest_to": record['interest_to'],
                        "amount": parent_amount,
                        "is_guarantee": False
                    }
                    incomes.append(income)
                    
                    # 创建资金流水记录
                    flow_record = {
                        "user_id": parent.id,
                        "fund_type": "POINT",
                        "action": "brothers_distribute_interest",
                        "amount": parent_amount,
                        "balance_after": parent_amount,
                        "related_fund_type": None,
                        "related_amount": None,
                        "remark": "股东收入",
                        "related_flow_id": None,
                        "counter_side": record['uid']
                    }
                    flow_records.append(flow_record)
                    
                    self.logger.info(f"上家 {parent.id} 获得收入: {parent_amount}")
                
                current_amount = current_amount - parent_amount
                current_user = parent
            else:
                if current_amount > Decimal('0'):
                    income = {
                        "uid": parent.id,
                        "borrowing_uid": record['uid'],
                        "bid": record['id'],
                        "interest_from": record['interest_from'],
                        "interest_to": record['interest_to'],
                        "amount": current_amount,
                        "is_guarantee": False
                    }
                    incomes.append(income)
                    
                    # 创建资金流水记录
                    flow_record = {
                        "user_id": parent.id,
                        "fund_type": "POINT",
                        "action": "brothers_distribute_interest",
                        "amount": current_amount,
                        "balance_after": current_amount,
                        "related_fund_type": None,
                        "related_amount": None,
                        "remark": "股东收入",
                        "related_flow_id": None,
                        "counter_side": record['uid']
                    }
                    flow_records.append(flow_record)
                    
                    self.logger.info(f"上家 {parent.id} 获得全部剩余收入: {current_amount}")
                
                break
        
        return incomes, flow_records