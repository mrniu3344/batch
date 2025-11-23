# -*- coding: utf-8 -*-
from models.lp_exception import LPException
from services.singleton_service import SingletonService
from models.db_connection import DBConnection
from models.deposit import Deposit
from models.deposit_detail import DepositDetail
import pendulum


class DepositService(SingletonService):
    def __init__(self, logger):
        self.logger = logger

    def get_deposits(self, conn: DBConnection) -> list[Deposit]:
        sql = "select * from deposits where status = 'begin' order by deposit_begin desc"
        datas = conn.select(sql)
        
        deposits = []
        for data in datas:
            deposit = Deposit(data)
            deposits.append(deposit)
        
        return deposits 

    def get_deposit_details(self, conn: DBConnection, deposit: Deposit):
        sql = "select * from deposit_details where uid = %s and id = %s order by installment"
        datas = conn.select(sql, (deposit.uid, deposit.id))
        
        details = []
        for data in datas:
            detail = DepositDetail(data)
            details.append(detail)
        
        deposit.init_details(details)
    
    def get_ndy_deposit_details(self, conn: DBConnection):
        sql = "select * from deposit_details where status='NDY'"
        datas = conn.select(sql)
        
        details = []
        for data in datas:
            detail = DepositDetail(data)
            details.append(detail)
        
        return details

    def save_deposit_interests(self, conn: DBConnection, interests: list, user: int, process: str) -> None:
        if not interests or len(interests) == 0:
            return    
        conn.insertMany("deposit_interests", interests, user, process)

    def save_deposit_details(self, conn: DBConnection, details: list, user: int, process: str) -> None:
        if not details or len(details) == 0:
            return    
        conn.insertMany("deposit_details", details, user, process)

    def update_deposit_status(self, conn: DBConnection, uid: int, id: int, user: int, process: str) -> None:
        keys = {"uid": uid, "id": id}
        now = int(pendulum.now().timestamp() * 1000)
        json_data = {"status": "end", "deposit_end": now}
        conn.update("deposits", keys, json_data, user, process)

    def update_deposit_detail_status(self, conn: DBConnection, uid: int, id: int, installment: str, status: str, user: int, process: str) -> None:
        keys = {"uid": uid, "id": id, "installment": installment}
        json_data = {"status": status}
        conn.update("deposit_details", keys, json_data, user, process)
