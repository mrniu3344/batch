from decimal import Decimal
from typing import Optional, List
from utils.utils import Utils

class DepositDetail:
    def __init__(self, data: dict):
        self.uid: int = data.get("uid")  # type: ignore
        self.id: int = data.get("id")  # type: ignore
        self.installment: str = data.get("installment")  # type: ignore
        utils = Utils()
        self.deposit_date = utils.int_to_date(data.get("deposit_date")) if data.get("deposit_date") else None
        self.amount: Optional[Decimal] = Decimal(str(data.get("amount", 0))) if data.get("amount") else None
        self.interest_rate: Optional[Decimal] = Decimal(str(data.get("interest_rate", 0))) if data.get("interest_rate") else None
        self.deposit_limit = utils.int_to_date(data.get("deposit_limit")) if data.get("deposit_limit") else None

    def print(self, logger):
        logger.info(f"  DepositDetail ID: {self.id}, UID: {self.uid}, Installment: {self.installment}, Deposit Date: {self.deposit_date}, Amount: {self.amount}, Interest Rate: {self.interest_rate}, Deposit Limit: {self.deposit_limit}")