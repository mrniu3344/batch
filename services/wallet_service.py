# -*- coding: utf-8 -*-
from models.lp_exception import LPException
from services.singleton_service import SingletonService
from models.db_connection import DBConnection
import pendulum
import requests
import json
import base58
import binascii
import time
from typing import Optional, List, Dict
from decimal import Decimal


class WalletService(SingletonService):
    def __init__(self, logger):
        self.logger = logger
    
    def _base58_to_hex_parameter(self, address):
        """将Base58地址转换为hex格式（用于合约调用的parameter）"""
        try:
            decoded = base58.b58decode(address)
            # Base58解码后的格式：1字节版本(0x41) + 20字节地址 + 4字节校验和
            # 对于balanceOf(address)调用，parameter需要是64位hex字符串
            # 格式：000000000000000000000000 + 20字节地址（去掉版本号）
            address_bytes = decoded[1:-4]  # 获取20字节地址部分
            hex_str = binascii.hexlify(address_bytes).decode('utf-8')
            # 补齐到64位（32字节），前面补0
            parameter = hex_str.zfill(64)
            return parameter
        except Exception as e:
            self.logger.error(f"Failed to convert address {address} to hex: {e}")
            raise
    
    def _query_trc20_balance(self, wallet_address, contract_address, max_retries=3, initial_delay=1.0):
        """
        通过调用合约的balanceOf方法查询TRC20代币余额
        包含429错误的重试逻辑（指数退避）
        
        参数:
            wallet_address: 钱包地址
            contract_address: 合约地址
            max_retries: 最大重试次数（默认3次）
            initial_delay: 初始延迟秒数（默认1秒，每次重试会翻倍）
        """
        from tronpy import Tron
        from tronpy.providers.http import HTTPProvider
        from requests.exceptions import HTTPError
        
        # 使用TronGrid API
        provider = HTTPProvider(endpoint_uri="https://api.trongrid.io", api_key="109a39a3-a6d6-4483-bcf4-7b3267bdf395")
        tron = Tron(provider=provider)
        
        delay = initial_delay
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                # 如果不是第一次尝试，等待后重试
                if attempt > 0:
                    self.logger.info(f"Retrying TRC20 balance query (attempt {attempt + 1}/{max_retries + 1}) after {delay:.1f}s delay...")
                    time.sleep(delay)
                    delay *= 2  # 指数退避：每次重试延迟翻倍
                
                # 获取合约实例
                contract = tron.get_contract(contract_address)
                
                # 调用balanceOf方法
                result = contract.functions.balanceOf(wallet_address)
                
                # result是一个整数（最小单位），直接返回最小单位，不进行转换
                balance_decimal = Decimal(str(result))
                
                self.logger.info(f"USDT balance from tronpy (min unit): {balance_decimal}")
                return balance_decimal
                    
            except HTTPError as e:
                # 检查是否是429错误（Too Many Requests）
                is_429_error = False
                if e.response is not None and e.response.status_code == 429:
                    is_429_error = True
                elif "429" in str(e) or "Too Many Requests" in str(e):
                    # 从错误消息中检查（作为后备方案）
                    is_429_error = True
                
                if is_429_error:
                    last_exception = e
                    if attempt < max_retries:
                        # 如果是429错误且还有重试机会，继续循环
                        self.logger.warning(f"Rate limit hit (429) on attempt {attempt + 1}/{max_retries + 1}. Will retry...")
                        continue
                    else:
                        # 最后一次重试也失败了
                        self.logger.error(f"Failed to query TRC20 balance after {max_retries + 1} attempts due to rate limiting (429)")
                        import traceback
                        self.logger.error(f"Traceback: {traceback.format_exc()}")
                        return None
                else:
                    # 其他HTTP错误，直接返回
                    self.logger.warning(f"Failed to query TRC20 balance via tronpy: {e}")
                    import traceback
                    self.logger.warning(f"Traceback: {traceback.format_exc()}")
                    return None
            except Exception as e:
                # 检查是否是429错误（可能被包装在其他异常中）
                error_str = str(e)
                if "429" in error_str or "Too Many Requests" in error_str:
                    last_exception = e
                    if attempt < max_retries:
                        # 如果是429错误且还有重试机会，继续循环
                        self.logger.warning(f"Rate limit hit (429) detected in exception message on attempt {attempt + 1}/{max_retries + 1}. Will retry...")
                        continue
                    else:
                        # 最后一次重试也失败了
                        self.logger.error(f"Failed to query TRC20 balance after {max_retries + 1} attempts due to rate limiting (429)")
                        import traceback
                        self.logger.error(f"Traceback: {traceback.format_exc()}")
                        return None
                else:
                    # 其他类型的异常，直接返回
                    self.logger.warning(f"Failed to query TRC20 balance via tronpy: {e}")
                    import traceback
                    self.logger.warning(f"Traceback: {traceback.format_exc()}")
                    return None
        
        # 所有重试都失败
        if last_exception:
            self.logger.error(f"Failed to query TRC20 balance after {max_retries + 1} attempts: {last_exception}")
        return None

    def audit_wallet(self, wallet_address, max_retries=3, initial_delay=1.0):
        """
        审计钱包地址，查询TRX和USDT余额
        包含重试逻辑以处理网络错误和429错误（指数退避）
        
        参数:
            wallet_address (str): TRON钱包地址（Base58格式）
            max_retries (int): 最大重试次数（默认3次）
            initial_delay (float): 初始延迟秒数（默认1秒，每次重试会翻倍）
        
        返回:
            dict: 包含以下键的字典:
                - wallet_address (str): 钱包地址
                - trx_balance (Decimal): TRX余额（单位：sun，最小单位）
                - usdt_balance (Decimal): USDT余额（单位：最小单位，1 USDT = 1,000,000）
                - tokens (list): TRC20代币列表
        
        异常:
            LPException: 当API请求失败或查询失败时抛出
        
        注意: 此函数接口被midnight_batch.py调用，请勿修改输入输出格式
        """
        # 在方法开始时无条件sleep 1秒，降低API调用频率，避免触发限流
        time.sleep(1.0)
        
        usdt_contract_address = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
        api_key = "109a39a3-a6d6-4483-bcf4-7b3267bdf395"
        
        # 初始化余额信息（确保返回格式一致）
        balance_info = {
            'wallet_address': wallet_address,
            'trx_balance': Decimal('0'),
            'usdt_balance': Decimal('0'),
            'tokens': []
        }
        
        # 1. 查询账户基本信息（带重试逻辑）
        api_url = f"https://api.trongrid.io/v1/accounts/{wallet_address}"
        headers = {
            'Accept': 'application/json',
            'TRON-PRO-API-KEY': api_key
        }
        
        delay = initial_delay
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                # 如果不是第一次尝试，等待后重试
                if attempt > 0:
                    self.logger.info(f"Retrying wallet audit query for {wallet_address} (attempt {attempt + 1}/{max_retries + 1}) after {delay:.1f}s delay...")
                    time.sleep(delay)
                    delay *= 2  # 指数退避：每次重试延迟翻倍
                
                response = requests.get(api_url, headers=headers, timeout=10)
                
                # 检查是否是429错误（Rate Limit）
                if response.status_code == 429:
                    last_exception = requests.exceptions.HTTPError(f"Rate limit (429) on attempt {attempt + 1}")
                    if attempt < max_retries:
                        self.logger.warning(f"Rate limit hit (429) for wallet {wallet_address} on attempt {attempt + 1}/{max_retries + 1}. Will retry...")
                        continue
                    else:
                        raise LPException(self.logger, "WalletService.audit_wallet", f"Rate limit (429) after {max_retries + 1} attempts for wallet {wallet_address}")
                
                response.raise_for_status()
                
                data = response.json()
                self.logger.info(f"wallet data: {data}")
                
                data_list = data.get('data', [])
                if data_list and len(data_list) > 0:
                    wallet_data = data_list[0]
                    self.logger.info(f"wallet_data: {wallet_data}")
                    
                    # 获取TRX余额（单位是sun，直接返回最小单位sun，不进行转换）
                    balance_sun = wallet_data.get('balance', 0)
                    balance_info['trx_balance'] = Decimal(str(balance_sun))
                    
                    # 尝试从trc20字段获取代币信息
                    tokens = wallet_data.get('trc20', [])
                    if tokens:
                        self.logger.info(f"Found trc20 tokens in account data: {tokens}")
                        for token in tokens:
                            for token_address, balance in token.items():
                                if balance != '0':
                                    # USDT是TRC20代币，直接返回最小单位，不进行转换
                                    balance_decimal = Decimal(str(balance))
                                    if token_address == usdt_contract_address:
                                        balance_info['usdt_balance'] = balance_decimal
                                    
                                    balance_info['tokens'].append({
                                        'token_address': token_address,
                                        'balance': str(balance_decimal)
                                    })
                
                # 成功获取数据，跳出重试循环
                break
                
            except requests.exceptions.Timeout as e:
                last_exception = e
                if attempt < max_retries:
                    self.logger.warning(f"Request timeout for wallet {wallet_address} on attempt {attempt + 1}/{max_retries + 1}. Will retry...")
                    continue
                else:
                    # 最后一次重试也失败，抛出异常
                    raise LPException(self.logger, "WalletService.audit_wallet", f"Request timeout after {max_retries + 1} attempts for wallet {wallet_address}: {e}")
            except requests.exceptions.ConnectionError as e:
                last_exception = e
                if attempt < max_retries:
                    self.logger.warning(f"Connection error for wallet {wallet_address} on attempt {attempt + 1}/{max_retries + 1}. Will retry...")
                    continue
                else:
                    # 最后一次重试也失败，抛出异常
                    raise LPException(self.logger, "WalletService.audit_wallet", f"Connection error after {max_retries + 1} attempts for wallet {wallet_address}: {e}")
            except requests.exceptions.RequestException as e:
                # 其他请求异常（如4xx, 5xx等非429错误），直接抛出
                raise LPException(self.logger, "WalletService.audit_wallet", f"API request failed for wallet {wallet_address}: {e}")
            except Exception as e:
                # 其他未知异常，直接抛出
                raise LPException(self.logger, "WalletService.audit_wallet", f"Unexpected error while querying wallet {wallet_address}: {e}")
        
        # 2. 如果data为空或没有找到USDT余额，通过合约调用查询USDT余额
        if balance_info['usdt_balance'] == Decimal('0'):
            self.logger.info("Data is empty or USDT balance is 0, trying to query via contract call...")
            usdt_balance = self._query_trc20_balance(wallet_address, usdt_contract_address)
            if usdt_balance is not None:
                balance_info['usdt_balance'] = usdt_balance
                self.logger.info(f"USDT balance from contract call: {usdt_balance}")
        
        self.logger.info(f"balance_info: {balance_info}")
        return balance_info
