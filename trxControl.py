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
    parser = argparse.ArgumentParser(description='Assess transaction risk using MistTrack')
    parser.add_argument(
        'transaction_id',
        type=str,
        help='The transaction ID (transaction hash) to assess'
    )
    
    args = parser.parse_args()
    
    transaction_id = args.transaction_id
    
    try:
        risk_service = RiskService(logger)
        result = risk_service.assess_transaction_risk(transaction_id)
        
        # 输出完整的MistTrack评价结果
        logger.info(f"Risk assessment completed for transaction: {transaction_id}")
        logger.info(f"Risk Score: {result.get('score', 'N/A')}")
        logger.info(f"Risk Level: {result.get('risk_level', 'N/A')}")
        logger.info(f"Hacking Event: {result.get('hacking_event', 'N/A')}")
        logger.info(f"Detail List: {result.get('detail_list', [])}")
        logger.info(f"Risk Detail: {result.get('risk_detail', [])}")
        logger.info(f"Scanned TS: {result.get('scanned_ts', 'N/A')}")
        
        # 返回完整的评价结果
        return result
    except Exception as e:
        import traceback
        logger.error(f"Error assessing transaction risk for {transaction_id}: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()

