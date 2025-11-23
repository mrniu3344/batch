from decimal import Decimal
from typing import Optional, List
from utils.utils import Utils

class DepositInterest:
    def __init__(self, data: dict):
        self.uid: int = data.get("uid") # type: ignore
        self.id: int = data.get("id") # type: ignore
        self.installment: str = data.get("installment") # type: ignore
        utils = Utils()
        self.interest_date = utils.int_to_date(data.get("interest_date"))
        self.amount: Optional[Decimal] = Decimal(str(data.get("amount", 0))) if data.get("amount") else None
