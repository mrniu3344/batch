# -*- coding: utf-8 -*-
from models.lp_exception import LPException
from services.singleton_service import SingletonService
from models.db_connection import DBConnection
from models.demand import Demand
import pendulum


class DemandService(SingletonService):
    def __init__(self, logger):
        self.logger = logger

    def get_expired_demands(self, conn: DBConnection, base_date: pendulum.DateTime) -> list[Demand]:
        """
        获取所有已到期的demand记录（base_date > demand_end）
        只获取status为'begin'的记录（'end'状态已通过画面处理，batch无需关心）
        """
        base_timestamp = int(base_date.timestamp() * 1000)
        sql = """
            select * from demands 
            where demand_end is not null 
            and demand_end < %s 
            and status = 'begin'
            order by demand_end asc
        """
        params = [base_timestamp]
        datas = conn.select(sql, params)
        
        demands = []
        for data in datas:
            demand = Demand(data)
            demands.append(demand)
        
        return demands

    def update_demand_status(self, conn: DBConnection, uid: int, id: int, status: str, user: int, process: str) -> None:
        """
        更新demand的状态
        """
        keys = {"uid": uid, "id": id}
        json_data = {"status": status}
        conn.update("demands", keys, json_data, user, process)

