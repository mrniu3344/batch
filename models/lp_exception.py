# -*- coding: utf-8 -*-
class LPException(Exception):
    def __init__(self, logger, error_function: str, error_detail: str = ""):
        super().__init__()
        self.logger = logger
        self.error_function = error_function
        self.error_detail = error_detail

    def print(self):
        self.logger.error(f"[{self.error_function}][{self.error_detail}]")
