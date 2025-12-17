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

wallet_address = "TCQKEmxNJuYoagbDkX4W5UZX9o3y5zocSS"

# Risk API endpoints
risk_api_endpoints = {
    "low": "",  # 低级API endpoint
    "moderate": "",  # 中级API endpoint
    "high": ""  # 高级API endpoint
}
