# -*- coding: utf-8 -*-
import psycopg2
import os
import constants as constants
from models.lp_exception import LPException
from models.db_connection import DBConnection
from services.singleton_service import SingletonService


class DBService(SingletonService):
    def __init__(self, logger, mode: str):
        self.logger = logger
        if mode == constants.env["development"]:
            self.host = "localhost"
            self.database = "postgres"
            self.user = "postgres"
            self.password = "laospay"
            self.port = 5432
            self.ssl_mode = None
        elif mode == constants.env["staging"]:
            self.host = "???"
            self.database = "postgres"
            self.user = "postgres"
            self.password = "laospay"
            self.port = 5432
            self.ssl_mode = {
                'sslmode': 'require',
                'sslcert': None,
                'sslkey': None,
                'sslrootcert': None,
                'sslcrl': None,
                'sslcompression': False
            }
        elif mode == constants.env["staging-aws"]:
            self.host = "???"
            self.database = "postgres"
            self.user = "postgres"
            self.password = "laospay"
            self.port = 5432
            self.ssl_mode = {
                'sslmode': 'require',
                'sslcert': None,
                'sslkey': None,
                'sslrootcert': None,
                'sslcrl': None,
                'sslcompression': False
            }
        elif mode == constants.env["production"]:
            self.host = "xinhui.cneuokygwolu.ap-southeast-1.rds.amazonaws.com"
            self.database = "postgres"
            self.user = "postgres"
            self.password = "xinhui.123"
            self.port = 5432
            self.ssl_mode = {
                'sslmode': 'require',
                'sslcert': None,
                'sslkey': None,
                'sslrootcert': None,
                'sslcrl': None,
                'sslcompression': False
            }
        elif mode == constants.env["production-aws"]:
            self.host = "xinhui.cneuokygwolu.ap-southeast-1.rds.amazonaws.com"
            self.database = "postgres"
            self.user = "postgres"
            self.password = "xinhui.123"
            self.port = 5432
            self.ssl_mode = {
                'sslmode': 'require',
                'sslcert': None,
                'sslkey': None,
                'sslrootcert': None,
                'sslcrl': None,
                'sslcompression': False
            }

    def get_connection(self) -> DBConnection:
        try:
            conn = psycopg2.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                port=self.port,
                sslmode=self.ssl_mode['sslmode'] if self.ssl_mode else None,
                sslcert=self.ssl_mode['sslcert'] if self.ssl_mode else None,
                sslkey=self.ssl_mode['sslkey'] if self.ssl_mode else None,
                sslrootcert=self.ssl_mode['sslrootcert'] if self.ssl_mode else None,
                sslcrl=self.ssl_mode['sslcrl'] if self.ssl_mode else None,
                sslcompression=self.ssl_mode['sslcompression'] if self.ssl_mode else None,
                connect_timeout=10,
                options='-c statement_timeout=30000'
            )
            return DBConnection(self.logger, conn)
        except Exception as e:
            raise LPException(self.logger, "DBService.get_connection", f"{e}")
