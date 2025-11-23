# -*- coding: utf-8 -*-
from models.lp_exception import LPException
from services.singleton_service import SingletonService
from models.db_connection import DBConnection
import pendulum


class UserFundFlowService(SingletonService):
    def __init__(self, logger):
        self.logger = logger

    def save_deposit_interest_flows(self, conn: DBConnection, flows: list, user: int, process: str) -> None:
        if not flows or len(flows) == 0:
            return    
        conn.insertMany("user_fund_flows", flows, user, process)
