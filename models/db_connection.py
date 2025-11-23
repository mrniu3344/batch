# -*- coding: utf-8 -*-
from xmlrpc.client import boolean
import psycopg2
import psycopg2.extras
import pendulum
from decimal import Decimal
from typing import Any
from models.lp_exception import LPException


class DBConnection:
    def __init__(self, logger, conn: psycopg2.extensions.connection):
        self.logger = logger
        self.conn = conn

    def _convert_value_for_db(self, value):
        """
        値をDBに保存するための形式に変換する
        - pendulum型の場合はunixミリ秒に変換
        - Decimal型の場合は数値に変換
        - その他の値はそのまま返す
        """
        if isinstance(value, pendulum.DateTime):
            # pendulum型をunixミリ秒に変換
            return int(value.timestamp() * 1000)
        elif isinstance(value, Decimal):
            # Decimal型はそのまま渡して精度を保持
            return value
        else:
            return value

    def _process_json_values(self, json_data):
        """
        JSONデータの値をDB用に変換する
        """
        processed_data = {}
        for key, value in json_data.items():
            processed_data[key] = self._convert_value_for_db(value)
        return processed_data

    def select(self, sql: str, params: Any = None, retries=0):
        try:
            if self.conn is None:
                raise LPException(self.logger, "DBConnection.select", "数据库连接为空")
            cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            if params != "":
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            self.logger.debug(cur.query.decode('utf-8') if hasattr(cur, 'query') and cur.query else sql)
            results = cur.fetchall()
            
            # DECIMAL型の項目をDecimal型に変換
            converted_results = []
            for row in results:
                converted_row = {}
                for key, value in row.items():
                    # psycopg2のDecimal型を検出して変換
                    if hasattr(value, '__class__') and value.__class__.__name__ == 'Decimal':
                        converted_row[key] = Decimal(str(value))
                    else:
                        converted_row[key] = value
                converted_results.append(converted_row)
            
            cur.close()
            return converted_results
        except Exception as e:
            raise LPException(
                self.logger,
                "DBConnection.select",
                str(e) + ", " + sql + ", " + ",".join(str(p) for p in params) if params else str(e) + ", " + sql,
            )

    def insert(self, table: str, json, user=None, process=None):
        try:
            if self.conn is None:
                raise LPException(self.logger, "DBConnection.insert", "数据库连接为空")
            # JSONデータの値をDB用に変換
            processed_json = self._process_json_values(json)
            
            columnsString = ""
            valuesString = ""
            values = []

            for key, value in processed_json.items():
                if columnsString == "":
                    columnsString += '"' + key + '"'
                    valuesString += "%s"
                else:
                    columnsString += ', "' + key + '"'
                    valuesString += ", %s"
                values.append(value)

            now = int(pendulum.now().timestamp() * 1000)  # 毫秒精度に変更
            if user is not None and process is not None:
                common = {
                    "create_by": user,
                    "create_at": now,
                    "create_with": process,
                    "update_by": user,
                    "update_at": now,
                    "update_with": process,
                }
                for key, value in common.items():
                    if columnsString == "":
                        columnsString += '"' + key + '"'
                        valuesString += "%s"
                    else:
                        columnsString += ', "' + key + '"'
                        valuesString += ", %s"
                    values.append(value)

            sql = 'insert into "{0}" ({1}) values ({2})'.format(
                table, columnsString, valuesString
            )
            cur = self.conn.cursor()
            cur.execute(sql, values)
            self.logger.debug(cur.query.decode('utf-8') if hasattr(cur, 'query') and cur.query else sql)
            cur.close()
        except Exception as e:
            raise LPException(
                self.logger, "DBConnection.insert {}".format(table), f"{e}"
            )

    def insert_update(self, table: str, json, key_json, user=None, process=None):
        try:
            if self.conn is None:
                raise LPException(self.logger, "DBConnection.insert_update", "数据库连接为空")
            # JSONデータの値をDB用に変換
            processed_json = self._process_json_values(json)
            
            columnsString = ""
            valuesString = ""
            values = []
            updateString = ""

            for key, value in processed_json.items():
                if columnsString == "":
                    columnsString += '"' + key + '"'
                    valuesString += "%s"
                else:
                    columnsString += ', "' + key + '"'
                    valuesString += ", %s"
                values.append(value)

            now = int(pendulum.now().timestamp() * 1000)  # 毫秒精度に変更
            if user is not None and process is not None:
                common = {
                    "create_by": user,
                    "create_at": now,
                    "create_with": process,
                    "update_by": user,
                    "update_at": now,
                    "update_with": process,
                }
                for key, value in common.items():
                    if columnsString == "":
                        columnsString += '"' + key + '"'
                        valuesString += "%s"
                    else:
                        columnsString += ', "' + key + '"'
                        valuesString += ", %s"
                    values.append(value)

            updateString = ", ".join(
                [
                    f'"{key}" = EXCLUDED."{key}"'
                    for key in processed_json.keys()
                    if key not in key_json.keys()
                ]
            )
            sql = (
                'insert into "{0}" ({1}) values ({2}) on conflict do update set {3}'.format(
                    table, columnsString, valuesString, updateString
                )
            )
            cur = self.conn.cursor()
            cur.execute(sql, values)
            self.logger.debug(cur.query.decode('utf-8') if hasattr(cur, 'query') and cur.query else sql)
            cur.close()
        except Exception as e:
            raise LPException(
                self.logger, "DBConnection.insert {}".format(table), f"{e}"
            )

    def insertMany(self, table: str, json, user=None, process=None, is_master: boolean=False):
        try:
            if self.conn is None:
                raise LPException(self.logger, "DBConnection.insertMany", "数据库连接为空")
            if len(json) < 0:
                return
            columnsString = ""
            valuesString = ""
            insert_value = []
            now = int(pendulum.now().timestamp() * 1000)  # 毫秒精度に変更
            common = {
                "create_by": user,
                "create_at": now,
                "create_with": process,
                "update_by": user,
                "update_at": now,
                "update_with": process,
            }
            if is_master == False:
                # commonをマジする
                for key, value in enumerate(json):
                    value.update(common)
            
            # 各JSONデータの値をDB用に変換
            processed_json_list = []
            for item in json:
                processed_json_list.append(self._process_json_values(item))
            
            # insert value作成
            for key, value in enumerate(processed_json_list):
                values = []
                for item_key, item_value in value.items():
                    if key == 0:
                        if columnsString == "":
                            columnsString += '"' + item_key + '"'
                            valuesString += "%s"
                        else:
                            columnsString += ', "' + item_key + '"'
                            valuesString += ", %s"
                    values.append(item_value)
                insert_value.append(values)

            sql = 'insert into "{0}" ({1}) values ({2})'.format(
                table, columnsString, valuesString
            )
            cur = self.conn.cursor()
            cur.executemany(sql, insert_value)
            cur.close()
        except Exception as e:
            raise LPException(
                self.logger, "DBConnection.insertMany {}".format(table), f"{e}"
            )

    def update(self, table: str, keys, json, user: int, process: str, is_master=False):
        try:
            if self.conn is None:
                raise LPException(self.logger, "DBConnection.update", "数据库连接为空")
            # JSONデータの値をDB用に変換
            processed_json = self._process_json_values(json)
            
            setString = ""
            whereString = ""
            values = []

            for key, value in processed_json.items():
                if setString == "":
                    setString += '"' + key + '" = %s'
                else:
                    setString += ', "' + key + '" = %s'
                values.append(value)

            now = int(pendulum.now().timestamp() * 1000)  # 毫秒精度に変更
            if is_master == False:
                common = {"update_by": user, "update_at": now, "update_with": process}
                for key, value in common.items():
                    if setString == "":
                        setString += '"' + key + '" = %s'
                    else:
                        setString += ', "' + key + '" = %s'
                    values.append(value)

            for key, value in keys.items():
                if whereString == "":
                    whereString += '"' + key + '" = %s'
                else:
                    whereString += ' and "' + key + '" = %s'
                values.append(value)
            sql = 'update "{0}" set {1} where {2} '.format(table, setString, whereString)

            cur = self.conn.cursor()
            count = cur.execute(sql, values)
            self.logger.debug(cur.query.decode('utf-8') if hasattr(cur, 'query') and cur.query else sql)
            cur.close()
            return count
        except Exception as e:
            raise LPException(
                self.logger, "DBConnection.update {}".format(table), f"{e}"
            )

    def delete(self, table: str, keys):
        try:
            if self.conn is None:
                raise LPException(self.logger, "DBConnection.delete", "数据库连接为空")
            whereString = ""
            values = []
            for key, value in keys.items():
                if whereString == "":
                    whereString += '"' + key + '" = %s'
                else:
                    whereString += ' and "' + key + '" = %s'
                values.append(value)
            sql = 'delete from "{0}" where {1} '.format(table, whereString)

            cur = self.conn.cursor()
            count = cur.execute(sql, values)
            self.logger.info("{} パラメータ:{} 削除した件数：{}".format(table, keys, count))
            self.logger.debug(cur.query.decode('utf-8') if hasattr(cur, 'query') and cur.query else sql)
            cur.close()
        except Exception as e:
            raise LPException(
                self.logger, "DBConnection.delete {}".format(table), f"{e}"
            )

    def commit(self, holdConnection: bool = False):
        try:
            if self.conn:
                self.conn.commit()
                if holdConnection == False:
                    self.conn.close()
                    self.conn = None
                self.logger.info("committed")
            else:
                raise Exception("already closed")
        except Exception as e:
            raise LPException(self.logger, "DBConnection.commit", f"{e}")

    def rollback(self, holdConnection: bool = False):
        try:
            if self.conn:
                self.conn.rollback()
                if holdConnection == False:
                    self.conn.close()
                    self.conn = None
                self.logger.info("rollbacked")
            else:
                raise Exception("already closed")
        except Exception as e:
            raise LPException(self.logger, "DBConnection.rollback", f"{e}")
