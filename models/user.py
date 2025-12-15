from decimal import Decimal
from typing import Optional, List, Dict, Any
import json
from utils.utils import Utils

class User:
    detail_list: Optional[List[str]]
    risk_detail: Optional[List[Dict[str, Any]]]
    
    def __init__(self, data: dict):
        utils = Utils()
        self.id: int = data.get("id") # type: ignore
        self.name: str = data.get("name") # type: ignore
        self.email: str = data.get("email") # type: ignore
        self.phone: str = data.get("phone") # type: ignore
        self.invitation: str = data.get("invitation") # type: ignore
        self.login_id: Optional[str] = data.get("login_id")
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
        self.score: Optional[int] = data.get("score")
        self.risk_level: Optional[str] = data.get("risk_level")
        self.hacking_event: Optional[str] = data.get("hacking_event")
        # detail_list 和 risk_detail 从数据库中以 JSON 字符串形式存储，需要解析
        detail_list_str = data.get("detail_list")
        if detail_list_str:
            try:
                self.detail_list = json.loads(detail_list_str) if isinstance(detail_list_str, str) else detail_list_str
            except (json.JSONDecodeError, TypeError):
                self.detail_list = None
        else:
            self.detail_list = None
        
        risk_detail_str = data.get("risk_detail")
        if risk_detail_str:
            try:
                self.risk_detail = json.loads(risk_detail_str) if isinstance(risk_detail_str, str) else risk_detail_str
            except (json.JSONDecodeError, TypeError):
                self.risk_detail = None
        else:
            self.risk_detail = None
        self.system_admin_id: Optional[int] = data.get("system_admin_id")
        self.children: list[User] = []

    @property
    def is_admin(self):
        return self.system_admin_id is not None

    def print(self, logger):
        logger.info(f"User ID: {self.id}, Name: {self.name}, Parent: {self.parent}, Point: {self.point}, Balance: {self.balance}")
        for child in self.children:
            child.print(logger)
