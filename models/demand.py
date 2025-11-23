# -*- coding: utf-8 -*-
from decimal import Decimal
from typing import Optional
from enum import Enum
import pendulum
from utils.utils import Utils


class DemandStatus(Enum):
    BEGIN = 'begin'
    END = 'end'
    DONE = 'done'


class Demand:
    def __init__(self, data: dict):
        self.uid: int = data.get("uid")  # type: ignore
        self.id: int = data.get("id")  # type: ignore
        utils = Utils()
        self.demand_begin = utils.int_to_date(data.get("demand_begin"))
        self.demand_end = utils.int_to_date(data.get("demand_end")) if data.get("demand_end") else None
        self.amount: Optional[Decimal] = utils.safe_decimal(data.get("amount"))
        self.status: DemandStatus = DemandStatus(data.get("status", "begin"))
        self.interest_rate: Optional[Decimal] = utils.safe_decimal(data.get("interest_rate"))
        self.interest: Optional[Decimal] = utils.safe_decimal(data.get("interest"))

    def print(self, logger):
        logger.info(f"Demand ID: {self.id}, UID: {self.uid}, Begin: {self.demand_begin}, End: {self.demand_end}, Status: {self.status.value}, Amount: {self.amount}, Interest: {self.interest}")

