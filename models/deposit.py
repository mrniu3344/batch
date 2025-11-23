from decimal import Decimal
from typing import Optional, List
from enum import Enum
import pendulum
from models.deposit_detail import DepositDetail
from utils.utils import Utils

class DepositStatus(Enum):
    BEGIN = 'begin'
    END = 'end'
    DEFAULT = 'default'

class Deposit:
    def __init__(self, data: dict):
        self.uid: int = data.get("uid") # type: ignore
        self.id: int = data.get("id") # type: ignore
        utils = Utils()
        self.deposit_begin = utils.int_to_date(data.get("deposit_begin"))
        self.deposit_end = utils.int_to_date(data.get("deposit_end")) if data.get("deposit_end") else None
        self.minimum_amount: Optional[Decimal] = Decimal(str(data.get("minimum_amount", 0))) if data.get("minimum_amount") else None
        self.status: DepositStatus = DepositStatus(data.get("status", "begin"))
        self.details: List[DepositDetail] = []

    def init_details(self, details: List[DepositDetail]):
        self.details = details

    @property
    def first_interest_date(self):
        next_month = self.deposit_begin.add(months=1).start_of('month')
        return next_month

    def make_interests(self, base_date: pendulum.DateTime, logger):
        interests = []
        is_deposit_end = False
        
        if base_date < self.first_interest_date:
            interests = []
        elif base_date == self.first_interest_date:
            interests = self.make_first_installment_interests(base_date, logger)
        else:
            interests, is_deposit_end = self.make_installment_interests(base_date, logger)
        
        return interests, is_deposit_end

    def make_first_installment_interests(self, base_date: pendulum.DateTime, logger):
        interests = []
        utils = Utils()
        
        # 计算按日比例：base_date到deposit_begin的天数 / deposit_begin当月的总天数
        days_from_begin_to_base = (base_date - self.deposit_begin.start_of('day')).days
        days_in_month = self.deposit_begin.days_in_month
        payment_ratio = Decimal(str(days_from_begin_to_base)) / Decimal(str(days_in_month))
        
        for detail in self.details:
            if detail.deposit_date is None or detail.amount is None or detail.amount == 0 or detail.interest_rate is None:
                continue

            # 计算基础利息金额
            base_interest_amount = detail.amount * detail.interest_rate
            
            # 计算实际利息金额（按日比例）并切り捨て到整数
            fifteenth_of_month = utils.string_to_date(f"{detail.installment}/15").end_of('day')
            if detail.deposit_date <= fifteenth_of_month:
                payment_ratio = Decimal('1')
            interest_amount = (base_interest_amount * payment_ratio).quantize(Decimal('1'), rounding='ROUND_DOWN')
            
            # 生成interest的json
            interest_json = {
                "uid": detail.uid,
                "id": detail.id,
                "installment": detail.installment,
                "interest_date": base_date,
                "amount": interest_amount
            }
            
            interests.append(interest_json)
        
        return interests

    def make_installment_interests(self, base_date: pendulum.DateTime, logger):
        interests = []

        utils = Utils()
        target_detail = None
        for detail in self.details:
            installment_date_str = f"{detail.installment}/01"
            installment_date = utils.string_to_date(installment_date_str)
            
            logger.debug(f"compare: {installment_date} {base_date.subtract(months=1)}")
            if installment_date == base_date.subtract(months=1):
                target_detail = detail
                break
        
        if target_detail is None or target_detail.deposit_date is None or target_detail.amount is None or target_detail.amount == 0:
            return interests, False
        
        # 各detailの独自のdeposit_limit（日付）を使用して期限日を計算
        if target_detail.deposit_limit is None:
            return interests, False
            
        # deposit_limitが日付型なので、そのまま使用
        deadline_date = target_detail.deposit_limit
        logger.debug(f"deadline_date: {deadline_date}, deposit_date: {target_detail.deposit_date}")
        
        if target_detail.deposit_date > deadline_date:
            return interests, False
        
        # target_detailが最後のdetailかどうかを判定
        # 只有当target_detail是最后一个installment时，才可能结束存款
        # 但还需要确保这是在正确的月份发放的（即base_date应该是max_installment的下一个月）
        max_installment = max(detail.installment for detail in self.details)
        
        # 检查：1. target_detail是最后一个installment
        #       2. base_date是最后一个installment的下一个月（即最后一个利息发放月份）
        if target_detail.installment == max_installment:
            max_installment_date_str = f"{max_installment}/01"
            max_installment_date = utils.string_to_date(max_installment_date_str)
            # 最后一个installment的利息应该在它的下一个月发放
            expected_end_date = max_installment_date.add(months=1).start_of('month')
            is_deposit_end = base_date == expected_end_date
        else:
            is_deposit_end = False
        
        for detail in self.details:
            if detail.deposit_date is None or detail.amount is None or detail.amount == 0 or detail.interest_rate is None:
                continue
                
            # 计算利息金额并切り捨て到整数
            interest_amount = (detail.amount * detail.interest_rate).quantize(Decimal('1'), rounding='ROUND_DOWN')
            
            interest_json = {
                "uid": detail.uid,
                "id": detail.id,
                "installment": detail.installment,
                "interest_date": base_date,
                "amount": interest_amount
            }
            
            interests.append(interest_json)
        
        return interests, is_deposit_end

    def print(self, logger):
        logger.info(f"Deposit ID: {self.id}, UID: {self.uid}, Begin: {self.deposit_begin}")
        for detail in self.details:
            detail.print(logger)
