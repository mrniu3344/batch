# -*- coding: utf-8 -*-
from models.lp_exception import LPException
from services.singleton_service import SingletonService
from models.db_connection import DBConnection
from models.user import User
from decimal import Decimal


class UserService(SingletonService):
    def __init__(self, logger):
        self.logger = logger

    def lock_user(self, conn: DBConnection, uid: int) -> User:
        sql = "select * from users where id=%s for update"
        params = [uid]
        users = conn.select(sql, params)
        if len(users) == 1:
            return User(users[0])
        else:
            raise LPException(self.logger, "UserService.get_user", "nothing to lock")

    def get_related_users(self, conn: DBConnection) -> dict[int, User]:
        sql = "select * from users"
        datas = conn.select(sql)
        
        users_dict = {}
        
        for data in datas:
            user = User(data)
            users_dict[user.id] = user
            
        for user in users_dict.values():
            if user.parent and user.parent in users_dict:
                parent_user = users_dict[user.parent]
                parent_user.children.append(user)
        
        def add_user_and_children(user: User):
            users_dict[user.id] = user
            for child in user.children:
                add_user_and_children(child)
        
        top_level_users = [user for user in users_dict.values() if not user.parent]
        for user in top_level_users:
            add_user_and_children(user)
        
        def print_user_hierarchy(user: User, level: int = 0):
            indent = "  " * level
            self.logger.debug(f"{indent}用户ID: {user.id}, 姓名: {user.name}, 上家ID: {user.parent}, 分成比例: {user.parent_divid}")
            for child in user.children:
                print_user_hierarchy(child, level + 1)
        
        self.logger.debug("=== 用户层级关系 ===")
        for user in top_level_users:
            print_user_hierarchy(user)
        self.logger.debug(f"=== 总计 {len(users_dict)} 个用户 ===")
        
        return users_dict

    def get_audit_users(self, conn: DBConnection) -> list[User]:
        sql = "select * from users where wallet is not null order by update_at desc"
        datas = conn.select(sql)
        return [User(data) for data in datas]

    def get_users(self, conn: DBConnection) -> list[User]:
        sql = "select * from users"
        datas = conn.select(sql)
        return [User(data) for data in datas]

    def get_user(self, conn: DBConnection, uid: int) -> User | None:
        sql = """select * from users where id=%s"""
        params = [uid]
        users = conn.select(sql, params)
        if len(users) == 1:
            return User(users[0])
        else:
            return None

    def insert_user(self, conn: DBConnection, json, user: int, process: str):
        conn.insert("users", json, user, process)

    def update_user(self, conn: DBConnection, keys, json, user: int, process: str):
        conn.update("users", keys, json, user, process)

    def update_audited_info(self, conn: DBConnection, uid, audited_usdt, audited_trx, user: int, process: str):
        keys = {"id": uid}
        json_data = {"audited_usdt": audited_usdt, "audited_trx": audited_trx}
        conn.update("users", keys, json_data, user, process)

    def update_risk_info(self, conn: DBConnection, uid: int, score: int, risk_level: str, user: int, process: str):
        """
        更新用户的风险评估信息
        
        参数:
            conn: 数据库连接
            uid: 用户ID
            score: 风险评分
            risk_level: 风险等级
            user: 操作用户ID
            process: 操作过程标识
        """
        keys = {"id": uid}
        json_data = {"score": score, "risk_level": risk_level}
        conn.update("users", keys, json_data, user, process)

    def update_point(self, conn: DBConnection, uid: int, add_amount: Decimal, user: int, process: str):
        existing_user = self.get_user(conn, uid)
        if existing_user is None:
            raise LPException(self.logger, "UserService.update_point", f"user with id {uid} not found")
        
        keys = {"id": uid}
        # existing_user.point 已经是 Decimal 类型，直接使用
        current_point = existing_user.point if isinstance(existing_user.point, Decimal) else Decimal('0')
        new_point = (current_point + add_amount).quantize(Decimal('1'), rounding='ROUND_DOWN')
        json_data = {"point": new_point}
        
        conn.update("users", keys, json_data, user, process)

    def update_demand_balance(self, conn: DBConnection, uid: int, subtract_amount: Decimal, user: int, process: str):
        """
        更新用户的demand_balance字段（减去金额）
        """
        existing_user = self.get_user(conn, uid)
        if existing_user is None:
            raise LPException(self.logger, "UserService.update_demand_balance", f"user with id {uid} not found")
        
        keys = {"id": uid}
        # existing_user.demand_balance 已经是 Decimal 类型，直接使用
        current_demand_balance = existing_user.demand_balance if isinstance(existing_user.demand_balance, Decimal) else Decimal('0')
        new_demand_balance = (current_demand_balance - subtract_amount).quantize(Decimal('1'), rounding='ROUND_DOWN')
        json_data = {"demand_balance": new_demand_balance}
        
        conn.update("users", keys, json_data, user, process)

    def append_risky_trn(self, conn: DBConnection, uid: int, deposit_record_id: int, user: int, process: str):
        """
        在用户的risky_trn字段后追加deposit_record_id（用逗号分隔）
        
        参数:
            conn: 数据库连接
            uid: 用户ID
            deposit_record_id: 存款记录ID
            user: 操作用户ID
            process: 操作过程标识
        """
        existing_user = self.get_user(conn, uid)
        if existing_user is None:
            raise LPException(self.logger, "UserService.append_risky_trn", f"user with id {uid} not found")
        
        keys = {"id": uid}
        current_risky_trn = existing_user.risky_trn or ""
        
        # 如果已经有值，用逗号分隔，添加新的id在最后
        if current_risky_trn:
            new_risky_trn = f"{current_risky_trn},{deposit_record_id}"
        else:
            new_risky_trn = str(deposit_record_id)
        
        json_data = {"risky_trn": new_risky_trn}
        conn.update("users", keys, json_data, user, process)
