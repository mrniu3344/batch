from decimal import Decimal
from typing import Optional
from utils.utils import Utils

class User:
    def __init__(self, data: dict):
        utils = Utils()
        self.id: int = data.get("id") # type: ignore
        self.name: str = data.get("name") # type: ignore
        self.email: str = data.get("email") # type: ignore
        self.phone: str = data.get("phone") # type: ignore
        self.invitation: str = data.get("invitation") # type: ignore
        self.point: Decimal = utils.safe_decimal(data.get("point")) or Decimal('0')
        self.balance: Decimal = utils.safe_decimal(data.get("balance")) or Decimal('0')
        self.loan: Decimal = utils.safe_decimal(data.get("loan")) or Decimal('0')
        self.demand_balance: Decimal = utils.safe_decimal(data.get("demand_balance")) or Decimal('0')
        self.can_lend: bool = data.get("can_lend", False)
        self.can_borrow: bool = data.get("can_borrow", False)
        self.parent: Optional[int] = data.get("parent")
        self.parent_divid: Optional[Decimal] = utils.safe_decimal(data.get("parent_divid"))
        self.wallet: Optional[str] = data.get("wallet")
        self.audited_usdt: Decimal = utils.safe_decimal(data.get("audited_usdt")) or Decimal('0')
        self.audited_trx: Decimal = utils.safe_decimal(data.get("audited_trx")) or Decimal('0')
        self.system_admin_id: Optional[int] = data.get("system_admin_id")
        self.children: list[User] = []

    @property
    def is_admin(self):
        return self.system_admin_id is not None

    def print(self, logger):
        logger.info(f"User ID: {self.id}, Name: {self.name}, Parent: {self.parent}, Point: {self.point}, Balance: {self.balance}")
        for child in self.children:
            child.print(logger)
