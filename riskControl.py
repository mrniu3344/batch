# -*- coding: utf-8 -*-
import argparse
import logging
import sys
from services.riskService import RiskService

# 设置基础日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Assess wallet address risk using MistTrack')
    parser.add_argument(
        'wallet_address',
        type=str,
        help='The wallet address to assess'
    )
    
    args = parser.parse_args()
    
    wallet_address = args.wallet_address
    api_key = 'I4UGlwTCSnNsfWYtX2LeF6mbH8KcBOJZ'
    
    try:
        risk_service = RiskService(logger, api_key=api_key)
        result = risk_service.assess_wallet_risk(wallet_address)
        
        # 只输出风险值
        risk_score = result.get('score', 'N/A')
        logger.info(f"Risk assessment completed for wallet: {wallet_address}")
        logger.info(f"Risk Score: {risk_score}")
        
        # 返回风险值
        return risk_score
    except Exception as e:
        import traceback
        logger.error(f"Error assessing wallet risk for {wallet_address}: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()
