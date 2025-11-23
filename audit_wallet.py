# -*- coding: utf-8 -*-
import argparse
import logging
import sys
from services.wallet_service import WalletService

# 设置基础日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Audit wallet address')
    parser.add_argument(
        'wallet_address',
        type=str,
        help='The wallet address to audit'
    )
    
    args = parser.parse_args()
    
    wallet_address = args.wallet_address
    
    try:
        wallet_service = WalletService(logger)
        result = wallet_service.audit_wallet(wallet_address)
        
        logger.info(f"Audit completed for wallet: {wallet_address}")
        logger.info(f"Result: {result}")
        
        return result
    except Exception as e:
        logger.error(f"Error auditing wallet {wallet_address}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

