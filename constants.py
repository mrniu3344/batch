# -*- coding: utf-8 -*-
from os.path import join as join_path
import os


env = {
    "development": "dev",
    "staging": "stg",
    "staging-aws": "stg-aws",
    "production": "prd",
    "production-aws": "prd-aws",
}

slack_webhook_url = {
    "tixian": "aaa",
    "shenpi": "bbb",
    "qianbao": "ccc",
    "fengxian": "ddd"
}

wallet_address = "TDLU7iCP5ohyy94je6emBegGeeKHaKkkSX"
wallet_address2 = "TRYgfsFyAp4WgAszig1bwvVrhGynCdLAkL"
wallet_address3 = "THkfWtH3H6RAMXSShBzbb1aoQ6y9bYp6eW"

# Risk API endpoints (base URLs, wallet address will be appended as path)
risk_api_endpoints = {
    "low": "http://172.31.50.211:8081/api/collections/collect3",  # 低级API endpoint
    "moderate": "http://172.31.50.211:8081/api/collections/collect2",  # 中级API endpoint
    "high": "http://172.31.50.211:8081/api/collections/collect"  # 高级API endpoint
}
