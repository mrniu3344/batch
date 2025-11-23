# -*- coding: utf-8 -*-
class SingletonService(object):
    def __new__(cls, *args, **kargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super(SingletonService, cls).__new__(cls)
        return cls._instance
