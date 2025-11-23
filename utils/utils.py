# -*- coding: utf-8 -*-
import pendulum
from typing import Union, Optional
from decimal import Decimal


class SingletonUtils(object):
    def __new__(cls, *args, **kargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super(SingletonUtils, cls).__new__(cls)
        return cls._instance


class Utils(SingletonUtils):
    def int_to_date(self, i, tz: str = 'Asia/Shanghai') -> pendulum.DateTime:
        return pendulum.from_timestamp(i / 1000.0, tz=tz)

    def date_to_int(self, date: pendulum.DateTime) -> int:
        return int(date.timestamp() * 1e3)

    def string_to_date(self, date_str: str, tz: str = 'Asia/Shanghai') -> pendulum.DateTime:
        result = pendulum.parse(date_str, tz=tz)
        if isinstance(result, pendulum.DateTime):
            return result
        elif isinstance(result, pendulum.Date):
            return pendulum.datetime(result.year, result.month, result.day, tz=tz)
        else:
            raise ValueError(f"Cannot convert {type(result)} to DateTime")

    def safe_decimal(self, value) -> Optional[Decimal]:
        """
        安全地将值转换为Decimal，处理None、空字符串、Decimal类型等情况
        """
        if value is None:
            return None
        if isinstance(value, Decimal):
            return value
        if isinstance(value, (int, float)):
            return Decimal(str(value))
        value_str = str(value).strip()
        if not value_str or value_str == '':
            return None
        try:
            return Decimal(value_str)
        except (ValueError, TypeError):
            return None
