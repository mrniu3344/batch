# -*- coding: utf-8 -*-
from models.lp_exception import LPException
from services.singleton_service import SingletonService
import requests
import json
import time
from typing import Optional, Dict


class RiskService(SingletonService):
    def __init__(self, logger, api_key: str = 'I4UGlwTCSnNsfWYtX2LeF6mbH8KcBOJZ'):
        self.logger = logger
        self.api_key = api_key
        self.create_task_url = "https://openapi.misttrack.io/v2/risk_score_create_task"
        self.query_task_url = "https://openapi.misttrack.io/v2/risk_score_query_task"
    
    def _create_risk_task(self, wallet_address: str, coin: str = "USDT-TRC20", max_retries: int = 3, initial_delay: float = 1.0) -> Dict:
        """
        创建风险评估任务
        
        参数:
            wallet_address: 钱包地址
            coin: 币种类型（默认USDT-TRC20）
            max_retries: 最大重试次数
            initial_delay: 初始延迟秒数
        
        返回:
            dict: API响应数据
        """
        if not self.api_key:
            raise LPException(self.logger, "RiskService._create_risk_task", "API key is not set")
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        payload = {
            "address": wallet_address,
            "coin": coin,
            "api_key": self.api_key
        }
        
        delay = initial_delay
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    self.logger.info(f"Retrying create risk task for {wallet_address} (attempt {attempt + 1}/{max_retries + 1}) after {delay:.1f}s delay...")
                    time.sleep(delay)
                    delay *= 2
                
                response = requests.post(
                    self.create_task_url,
                    headers=headers,
                    json=payload,
                    timeout=30
                )
                
                self.logger.info(f"API response status: {response.status_code}")
                self.logger.info(f"API response content: {response.text}")
                
                if response.status_code == 429:
                    last_exception = requests.exceptions.HTTPError(f"Rate limit (429) on attempt {attempt + 1}")
                    if attempt < max_retries:
                        self.logger.warning(f"Rate limit hit (429) for wallet {wallet_address} on attempt {attempt + 1}/{max_retries + 1}. Will retry...")
                        continue
                    else:
                        raise LPException(self.logger, "RiskService._create_risk_task", f"Rate limit (429) after {max_retries + 1} attempts for wallet {wallet_address}")
                
                # 检查 HTTP 状态码
                if response.status_code != 200:
                    error_text = response.text
                    self.logger.error(f"API returned non-200 status code {response.status_code}: {error_text}")
                    raise LPException(self.logger, "RiskService._create_risk_task", f"API returned status {response.status_code}: {error_text}")
                
                try:
                    result = response.json()
                except ValueError as e:
                    self.logger.error(f"Failed to parse JSON response: {response.text}")
                    raise LPException(self.logger, "RiskService._create_risk_task", f"Invalid JSON response: {response.text}")
                
                # 检查 API 响应格式
                if 'success' not in result:
                    self.logger.error(f"API response missing 'success' field: {result}")
                    raise LPException(self.logger, "RiskService._create_risk_task", f"Invalid API response format: missing 'success' field")
                
                if not result.get('success', False):
                    error_msg = result.get('msg', 'Unknown error')
                    self.logger.error(f"API returned error: {error_msg}, full response: {result}")
                    raise LPException(self.logger, "RiskService._create_risk_task", f"API returned error: {error_msg}")
                
                # 检查 data 字段
                data = result.get('data', {})
                if not data:
                    self.logger.error(f"API response missing 'data' field: {result}")
                    raise LPException(self.logger, "RiskService._create_risk_task", f"API response missing 'data' field: {result}")
                
                self.logger.info(f"Task created successfully. Data: {data}")
                return data
                
            except requests.exceptions.Timeout as e:
                last_exception = e
                if attempt < max_retries:
                    self.logger.warning(f"Request timeout for wallet {wallet_address} on attempt {attempt + 1}/{max_retries + 1}. Will retry...")
                    continue
                else:
                    raise LPException(self.logger, "RiskService._create_risk_task", f"Request timeout after {max_retries + 1} attempts for wallet {wallet_address}: {e}")
            except requests.exceptions.ConnectionError as e:
                last_exception = e
                if attempt < max_retries:
                    self.logger.warning(f"Connection error for wallet {wallet_address} on attempt {attempt + 1}/{max_retries + 1}. Will retry...")
                    continue
                else:
                    raise LPException(self.logger, "RiskService._create_risk_task", f"Connection error after {max_retries + 1} attempts for wallet {wallet_address}: {e}")
            except requests.exceptions.RequestException as e:
                raise LPException(self.logger, "RiskService._create_risk_task", f"API request failed for wallet {wallet_address}: {e}")
            except Exception as e:
                raise LPException(self.logger, "RiskService._create_risk_task", f"Unexpected error while creating risk task for wallet {wallet_address}: {e}")
        
        if last_exception:
            raise LPException(self.logger, "RiskService._create_risk_task", f"Failed to create risk task after {max_retries + 1} attempts: {last_exception}")
    
    def _query_risk_task(self, wallet_address: str, coin: str = "USDT-TRC20", max_retries: int = 3, initial_delay: float = 1.0) -> Optional[Dict]:
        """
        查询风险评估任务结果
        
        参数:
            wallet_address: 钱包地址
            coin: 币种类型（默认USDT-TRC20）
            max_retries: 最大重试次数
            initial_delay: 初始延迟秒数
        
        返回:
            dict: 风险评估结果，如果任务还未完成则返回None
        """
        if not self.api_key:
            raise LPException(self.logger, "RiskService._query_risk_task", "API key is not set")
        
        params = {
            "address": wallet_address,
            "coin": coin,
            "api_key": self.api_key
        }
        
        delay = initial_delay
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    self.logger.info(f"Retrying query risk task for {wallet_address} (attempt {attempt + 1}/{max_retries + 1}) after {delay:.1f}s delay...")
                    time.sleep(delay)
                    delay *= 2
                
                response = requests.get(
                    self.query_task_url,
                    params=params,
                    timeout=30
                )
                
                self.logger.info(f"Query API response status: {response.status_code}")
                self.logger.info(f"Query API response content: {response.text}")
                
                if response.status_code == 429:
                    last_exception = requests.exceptions.HTTPError(f"Rate limit (429) on attempt {attempt + 1}")
                    if attempt < max_retries:
                        self.logger.warning(f"Rate limit hit (429) for wallet {wallet_address} on attempt {attempt + 1}/{max_retries + 1}. Will retry...")
                        continue
                    else:
                        raise LPException(self.logger, "RiskService._query_risk_task", f"Rate limit (429) after {max_retries + 1} attempts for wallet {wallet_address}")
                
                # 检查 HTTP 状态码
                if response.status_code != 200:
                    error_text = response.text
                    self.logger.error(f"Query API returned non-200 status code {response.status_code}: {error_text}")
                    raise LPException(self.logger, "RiskService._query_risk_task", f"API returned status {response.status_code}: {error_text}")
                
                try:
                    result = response.json()
                except ValueError as e:
                    self.logger.error(f"Failed to parse JSON response: {response.text}")
                    raise LPException(self.logger, "RiskService._query_risk_task", f"Invalid JSON response: {response.text}")
                
                if not result.get('success', False):
                    error_msg = result.get('msg', 'Unknown error')
                    # 如果任务还未完成，返回None而不是抛出异常
                    if 'not ready' in error_msg.lower() or 'no result' in error_msg.lower():
                        self.logger.info(f"Task not ready yet: {error_msg}")
                        return None
                    raise LPException(self.logger, "RiskService._query_risk_task", f"API returned error: {error_msg}")
                
                data = result.get('data', {})
                # 检查任务是否已完成：如果数据中包含 score 字段，说明结果已经准备好
                if 'score' in data:
                    self.logger.info(f"Task result ready, score: {data.get('score')}")
                    return data
                
                # 如果没有 score 字段，检查 has_result 字段
                if data.get('has_result', False):
                    self.logger.info(f"Task result ready (has_result=True)")
                    return data
                
                # 结果还未准备好
                self.logger.info(f"Task result not ready, has_result: {data.get('has_result', False)}, has_score: {'score' in data}")
                return None
                
            except requests.exceptions.Timeout as e:
                last_exception = e
                if attempt < max_retries:
                    self.logger.warning(f"Request timeout for wallet {wallet_address} on attempt {attempt + 1}/{max_retries + 1}. Will retry...")
                    continue
                else:
                    raise LPException(self.logger, "RiskService._query_risk_task", f"Request timeout after {max_retries + 1} attempts for wallet {wallet_address}: {e}")
            except requests.exceptions.ConnectionError as e:
                last_exception = e
                if attempt < max_retries:
                    self.logger.warning(f"Connection error for wallet {wallet_address} on attempt {attempt + 1}/{max_retries + 1}. Will retry...")
                    continue
                else:
                    raise LPException(self.logger, "RiskService._query_risk_task", f"Connection error after {max_retries + 1} attempts for wallet {wallet_address}: {e}")
            except requests.exceptions.RequestException as e:
                raise LPException(self.logger, "RiskService._query_risk_task", f"API request failed for wallet {wallet_address}: {e}")
            except Exception as e:
                raise LPException(self.logger, "RiskService._query_risk_task", f"Unexpected error while querying risk task for wallet {wallet_address}: {e}")
        
        if last_exception:
            raise LPException(self.logger, "RiskService._query_risk_task", f"Failed to query risk task after {max_retries + 1} attempts: {last_exception}")
    
    def assess_wallet_risk(self, wallet_address: str, coin: str = "USDT-TRC20", max_polling_attempts: int = 3, polling_interval: float = 2.0) -> Dict:
        """
        评估钱包地址的风险
        
        参数:
            wallet_address: 钱包地址
            coin: 币种类型（默认USDT-TRC20，内部使用，用户无需指定）
            max_polling_attempts: 最大轮询次数（默认3次）
            polling_interval: 轮询间隔秒数（默认2秒）
        
        返回:
            dict: 包含以下键的字典:
                - wallet_address (str): 钱包地址
                - score (int): 风险评分（3-100）
                - risk_level (str): 风险等级（Low, Moderate, High, Severe）
                - hacking_event (str): 相关安全事件名称
                - detail_list (list): 风险描述列表
                - risk_detail (list): 详细风险信息列表
                - scanned_ts (int): 扫描时间戳
        
        异常:
            LPException: 当API请求失败或查询失败时抛出
        """
        # 在方法开始时sleep 1秒，降低API调用频率
        time.sleep(1.0)
        
        self.logger.info(f"Starting risk assessment for wallet: {wallet_address}")
        
        # 1. 创建风险评估任务
        try:
            self.logger.info(f"Creating risk assessment task for wallet: {wallet_address}, coin: {coin}")
            task_data = self._create_risk_task(wallet_address, coin)
            
            # 验证任务数据
            if not task_data:
                raise LPException(self.logger, "RiskService.assess_wallet_risk", f"Task creation returned empty data for wallet {wallet_address}")
            
            self.logger.info(f"Risk task created successfully for wallet {wallet_address}: {task_data}")
            
            # 检查是否有错误信息
            if 'error' in task_data or 'error_msg' in task_data:
                error_info = task_data.get('error') or task_data.get('error_msg')
                raise LPException(self.logger, "RiskService.assess_wallet_risk", f"Task creation returned error: {error_info}")
                
        except LPException as e:
            self.logger.error(f"Failed to create risk task for wallet {wallet_address}: {e.error_detail}")
            raise
        except Exception as e:
            import traceback
            self.logger.error(f"Failed to create risk task for wallet {wallet_address}: {e}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise
        
        # 2. 轮询查询任务结果
        # 第一次查询前等待一段时间，给任务处理时间
        self.logger.info(f"Waiting {polling_interval}s before first query...")
        time.sleep(polling_interval)
        
        result = None
        for attempt in range(max_polling_attempts):
            try:
                result = self._query_risk_task(wallet_address, coin)
                
                if result is not None:
                    self.logger.info(f"Risk assessment result obtained for wallet {wallet_address} after {attempt + 1} polling attempts")
                    break
                
                # 如果结果还未准备好，等待后继续轮询
                if attempt < max_polling_attempts - 1:
                    self.logger.info(f"Risk task result not ready yet for wallet {wallet_address}, waiting {polling_interval}s before next poll (attempt {attempt + 1}/{max_polling_attempts})...")
                    time.sleep(polling_interval)
                else:
                    self.logger.warning(f"Risk task result not ready after {max_polling_attempts} polling attempts for wallet {wallet_address}")
                    
            except Exception as e:
                self.logger.error(f"Error while polling risk task result for wallet {wallet_address}: {e}")
                raise
        
        if result is None:
            raise LPException(self.logger, "RiskService.assess_wallet_risk", f"Failed to get risk assessment result for wallet {wallet_address} after {max_polling_attempts} polling attempts")
        
        # 3. 格式化返回结果
        risk_assessment = {
            'wallet_address': wallet_address,
            'score': result.get('score', 0),
            'risk_level': result.get('risk_level', 'Unknown'),
            'hacking_event': result.get('hacking_event', ''),
            'detail_list': result.get('detail_list', []),
            'risk_detail': result.get('risk_detail', []),
            'scanned_ts': result.get('scanned_ts', 0)
        }
        
        self.logger.info(f"Risk assessment completed for wallet {wallet_address}: {risk_assessment}")
        return risk_assessment
    
    def assess_transaction_risk(self, transaction_id: str, coin: str = "USDT-TRC20", max_polling_attempts: int = 3, polling_interval: float = 2.0) -> Dict:
        """
        评估交易的风险
        
        参数:
            transaction_id: 交易ID（交易哈希）
            coin: 币种类型（默认USDT-TRC20）
            max_polling_attempts: 最大轮询次数（默认3次）
            polling_interval: 轮询间隔秒数（默认2秒）
        
        返回:
            dict: 包含以下键的字典:
                - transaction_id (str): 交易ID
                - score (int): 风险评分（3-100）
                - risk_level (str): 风险等级（Low, Moderate, High, Severe）
                - hacking_event (str): 相关安全事件名称
                - detail_list (list): 风险描述列表
                - risk_detail (list): 详细风险信息列表
                - scanned_ts (int): 扫描时间戳
        
        异常:
            LPException: 当API请求失败或查询失败时抛出
        """
        # 在方法开始时sleep 1秒，降低API调用频率
        time.sleep(1.0)
        
        self.logger.info(f"Starting risk assessment for transaction: {transaction_id}")
        
        # 1. 创建交易风险评估任务
        try:
            self.logger.info(f"Creating risk assessment task for transaction: {transaction_id}, coin: {coin}")
            task_data = self._create_transaction_risk_task(transaction_id, coin)
            
            # 验证任务数据
            if not task_data:
                raise LPException(self.logger, "RiskService.assess_transaction_risk", f"Task creation returned empty data for transaction {transaction_id}")
            
            self.logger.info(f"Risk task created successfully for transaction {transaction_id}: {task_data}")
            
            # 检查是否有错误信息
            if 'error' in task_data or 'error_msg' in task_data:
                error_info = task_data.get('error') or task_data.get('error_msg')
                raise LPException(self.logger, "RiskService.assess_transaction_risk", f"Task creation returned error: {error_info}")
                
        except LPException as e:
            self.logger.error(f"Failed to create risk task for transaction {transaction_id}: {e.error_detail}")
            raise
        except Exception as e:
            import traceback
            self.logger.error(f"Failed to create risk task for transaction {transaction_id}: {e}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise
        
        # 2. 轮询查询任务结果
        # 第一次查询前等待一段时间，给任务处理时间
        self.logger.info(f"Waiting {polling_interval}s before first query...")
        time.sleep(polling_interval)
        
        result = None
        for attempt in range(max_polling_attempts):
            try:
                result = self._query_transaction_risk_task(transaction_id, coin)
                
                if result is not None:
                    self.logger.info(f"Risk assessment result obtained for transaction {transaction_id} after {attempt + 1} polling attempts")
                    break
                
                # 如果结果还未准备好，等待后继续轮询
                if attempt < max_polling_attempts - 1:
                    self.logger.info(f"Risk task result not ready yet for transaction {transaction_id}, waiting {polling_interval}s before next poll (attempt {attempt + 1}/{max_polling_attempts})...")
                    time.sleep(polling_interval)
                else:
                    self.logger.warning(f"Risk task result not ready after {max_polling_attempts} polling attempts for transaction {transaction_id}")
                    
            except Exception as e:
                self.logger.error(f"Error while polling risk task result for transaction {transaction_id}: {e}")
                raise
        
        if result is None:
            raise LPException(self.logger, "RiskService.assess_transaction_risk", f"Failed to get risk assessment result for transaction {transaction_id} after {max_polling_attempts} polling attempts")
        
        # 3. 格式化返回结果
        risk_assessment = {
            'transaction_id': transaction_id,
            'score': result.get('score', 0),
            'risk_level': result.get('risk_level', 'Unknown'),
            'hacking_event': result.get('hacking_event', ''),
            'detail_list': result.get('detail_list', []),
            'risk_detail': result.get('risk_detail', []),
            'scanned_ts': result.get('scanned_ts', 0)
        }
        
        self.logger.info(f"Risk assessment completed for transaction {transaction_id}: {risk_assessment}")
        return risk_assessment
    
    def _create_transaction_risk_task(self, transaction_id: str, coin: str = "USDT-TRC20", max_retries: int = 3, initial_delay: float = 1.0) -> Dict:
        """
        创建交易风险评估任务
        
        参数:
            transaction_id: 交易ID（交易哈希）
            coin: 币种类型（默认USDT-TRC20）
            max_retries: 最大重试次数
            initial_delay: 初始延迟秒数
        
        返回:
            dict: API响应数据
        """
        if not self.api_key:
            raise LPException(self.logger, "RiskService._create_transaction_risk_task", "API key is not set")
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        payload = {
            "txid": transaction_id,
            "coin": coin,
            "api_key": self.api_key
        }
        
        delay = initial_delay
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    self.logger.info(f"Retrying create transaction risk task for {transaction_id} (attempt {attempt + 1}/{max_retries + 1}) after {delay:.1f}s delay...")
                    time.sleep(delay)
                    delay *= 2
                
                response = requests.post(
                    self.create_task_url,
                    headers=headers,
                    json=payload,
                    timeout=30
                )
                
                self.logger.info(f"API response status: {response.status_code}")
                self.logger.info(f"API response content: {response.text}")
                
                if response.status_code == 429:
                    last_exception = requests.exceptions.HTTPError(f"Rate limit (429) on attempt {attempt + 1}")
                    if attempt < max_retries:
                        self.logger.warning(f"Rate limit hit (429) for transaction {transaction_id} on attempt {attempt + 1}/{max_retries + 1}. Will retry...")
                        continue
                    else:
                        raise LPException(self.logger, "RiskService._create_transaction_risk_task", f"Rate limit (429) after {max_retries + 1} attempts for transaction {transaction_id}")
                
                # 检查 HTTP 状态码
                if response.status_code != 200:
                    error_text = response.text
                    self.logger.error(f"API returned non-200 status code {response.status_code}: {error_text}")
                    raise LPException(self.logger, "RiskService._create_transaction_risk_task", f"API returned status {response.status_code}: {error_text}")
                
                try:
                    result = response.json()
                except ValueError as e:
                    self.logger.error(f"Failed to parse JSON response: {response.text}")
                    raise LPException(self.logger, "RiskService._create_transaction_risk_task", f"Invalid JSON response: {response.text}")
                
                # 检查 API 响应格式
                if 'success' not in result:
                    self.logger.error(f"API response missing 'success' field: {result}")
                    raise LPException(self.logger, "RiskService._create_transaction_risk_task", f"Invalid API response format: missing 'success' field")
                
                if not result.get('success', False):
                    error_msg = result.get('msg', 'Unknown error')
                    self.logger.error(f"API returned error: {error_msg}, full response: {result}")
                    raise LPException(self.logger, "RiskService._create_transaction_risk_task", f"API returned error: {error_msg}")
                
                # 检查 data 字段
                data = result.get('data', {})
                if not data:
                    self.logger.error(f"API response missing 'data' field: {result}")
                    raise LPException(self.logger, "RiskService._create_transaction_risk_task", f"API response missing 'data' field: {result}")
                
                self.logger.info(f"Task created successfully. Data: {data}")
                return data
                
            except requests.exceptions.Timeout as e:
                last_exception = e
                if attempt < max_retries:
                    self.logger.warning(f"Request timeout for transaction {transaction_id} on attempt {attempt + 1}/{max_retries + 1}. Will retry...")
                    continue
                else:
                    raise LPException(self.logger, "RiskService._create_transaction_risk_task", f"Request timeout after {max_retries + 1} attempts for transaction {transaction_id}: {e}")
            except requests.exceptions.ConnectionError as e:
                last_exception = e
                if attempt < max_retries:
                    self.logger.warning(f"Connection error for transaction {transaction_id} on attempt {attempt + 1}/{max_retries + 1}. Will retry...")
                    continue
                else:
                    raise LPException(self.logger, "RiskService._create_transaction_risk_task", f"Connection error after {max_retries + 1} attempts for transaction {transaction_id}: {e}")
            except requests.exceptions.RequestException as e:
                raise LPException(self.logger, "RiskService._create_transaction_risk_task", f"API request failed for transaction {transaction_id}: {e}")
            except Exception as e:
                raise LPException(self.logger, "RiskService._create_transaction_risk_task", f"Unexpected error while creating risk task for transaction {transaction_id}: {e}")
        
        if last_exception:
            raise LPException(self.logger, "RiskService._create_transaction_risk_task", f"Failed to create risk task after {max_retries + 1} attempts: {last_exception}")
    
    def _query_transaction_risk_task(self, transaction_id: str, coin: str = "USDT-TRC20", max_retries: int = 3, initial_delay: float = 1.0) -> Optional[Dict]:
        """
        查询交易风险评估任务结果
        
        参数:
            transaction_id: 交易ID（交易哈希）
            coin: 币种类型（默认USDT-TRC20）
            max_retries: 最大重试次数
            initial_delay: 初始延迟秒数
        
        返回:
            dict: 风险评估结果，如果任务还未完成则返回None
        """
        if not self.api_key:
            raise LPException(self.logger, "RiskService._query_transaction_risk_task", "API key is not set")
        
        params = {
            "txid": transaction_id,
            "coin": coin,
            "api_key": self.api_key
        }
        
        delay = initial_delay
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    self.logger.info(f"Retrying query transaction risk task for {transaction_id} (attempt {attempt + 1}/{max_retries + 1}) after {delay:.1f}s delay...")
                    time.sleep(delay)
                    delay *= 2
                
                response = requests.get(
                    self.query_task_url,
                    params=params,
                    timeout=30
                )
                
                self.logger.info(f"Query API response status: {response.status_code}")
                self.logger.info(f"Query API response content: {response.text}")
                
                if response.status_code == 429:
                    last_exception = requests.exceptions.HTTPError(f"Rate limit (429) on attempt {attempt + 1}")
                    if attempt < max_retries:
                        self.logger.warning(f"Rate limit hit (429) for transaction {transaction_id} on attempt {attempt + 1}/{max_retries + 1}. Will retry...")
                        continue
                    else:
                        raise LPException(self.logger, "RiskService._query_transaction_risk_task", f"Rate limit (429) after {max_retries + 1} attempts for transaction {transaction_id}")
                
                # 检查 HTTP 状态码
                if response.status_code != 200:
                    error_text = response.text
                    self.logger.error(f"Query API returned non-200 status code {response.status_code}: {error_text}")
                    raise LPException(self.logger, "RiskService._query_transaction_risk_task", f"API returned status {response.status_code}: {error_text}")
                
                try:
                    result = response.json()
                except ValueError as e:
                    self.logger.error(f"Failed to parse JSON response: {response.text}")
                    raise LPException(self.logger, "RiskService._query_transaction_risk_task", f"Invalid JSON response: {response.text}")
                
                if not result.get('success', False):
                    error_msg = result.get('msg', 'Unknown error')
                    # 如果任务还未完成，返回None而不是抛出异常
                    if 'not ready' in error_msg.lower() or 'no result' in error_msg.lower():
                        self.logger.info(f"Task not ready yet: {error_msg}")
                        return None
                    raise LPException(self.logger, "RiskService._query_transaction_risk_task", f"API returned error: {error_msg}")
                
                data = result.get('data', {})
                # 检查任务是否已完成：如果数据中包含 score 字段，说明结果已经准备好
                if 'score' in data:
                    self.logger.info(f"Task result ready, score: {data.get('score')}")
                    return data
                
                # 如果没有 score 字段，检查 has_result 字段
                if data.get('has_result', False):
                    self.logger.info(f"Task result ready (has_result=True)")
                    return data
                
                # 结果还未准备好
                self.logger.info(f"Task result not ready, has_result: {data.get('has_result', False)}, has_score: {'score' in data}")
                return None
                
            except requests.exceptions.Timeout as e:
                last_exception = e
                if attempt < max_retries:
                    self.logger.warning(f"Request timeout for transaction {transaction_id} on attempt {attempt + 1}/{max_retries + 1}. Will retry...")
                    continue
                else:
                    raise LPException(self.logger, "RiskService._query_transaction_risk_task", f"Request timeout after {max_retries + 1} attempts for transaction {transaction_id}: {e}")
            except requests.exceptions.ConnectionError as e:
                last_exception = e
                if attempt < max_retries:
                    self.logger.warning(f"Connection error for transaction {transaction_id} on attempt {attempt + 1}/{max_retries + 1}. Will retry...")
                    continue
                else:
                    raise LPException(self.logger, "RiskService._query_transaction_risk_task", f"Connection error after {max_retries + 1} attempts for transaction {transaction_id}: {e}")
            except requests.exceptions.RequestException as e:
                raise LPException(self.logger, "RiskService._query_transaction_risk_task", f"API request failed for transaction {transaction_id}: {e}")
            except Exception as e:
                raise LPException(self.logger, "RiskService._query_transaction_risk_task", f"Unexpected error while querying risk task for transaction {transaction_id}: {e}")
        
        if last_exception:
            raise LPException(self.logger, "RiskService._query_transaction_risk_task", f"Failed to query risk task after {max_retries + 1} attempts: {last_exception}")
    
    def analyseRisk(self, score: int, risk_level: str, hacking_event: str, detail_list: list, risk_detail: list) -> tuple:
        """
        分析风险等级和风险评分
        
        参数:
            score: 原始风险评分
            risk_level: 风险等级（Low, Moderate, High, Severe）
            hacking_event: 相关安全事件名称
            detail_list: 风险描述列表
            risk_detail: 详细风险信息列表
        
        返回:
            tuple: (level, score) 其中:
                - level (str): 风险等级（None, Low, Moderate, High）
                - score (int): 二进制风险评分（转换为十进制）
        """
        # 检查是否为空（处理None、空字符串、空列表等情况）
        is_hacking_event_empty = not hacking_event or (isinstance(hacking_event, str) and not hacking_event.strip())
        is_detail_list_empty = not detail_list or (isinstance(detail_list, list) and len(detail_list) == 0)
        is_risk_detail_empty = not risk_detail or (isinstance(risk_detail, list) and len(risk_detail) == 0)
        
        # 判断是否为None级别
        if (risk_level in ['Low', 'Moderate']) and is_hacking_event_empty and is_detail_list_empty and is_risk_detail_empty:
            level = 'None'
        # 判断是否为Low级别
        elif risk_level in ['Low', 'Moderate']:
            level = 'Low'
        # 判断是否为Moderate级别
        elif risk_level == 'High':
            level = 'Moderate'
        # 判断是否为High级别
        elif risk_level == 'Severe':
            level = 'High'
        else:
            level = 'Unknown'
        
        # 计算二进制风险评分
        binary_score = 0
        
        # 第一位：hacking_event不为空
        if not is_hacking_event_empty:
            binary_score |= 1  # 2^0 = 1
            level = 'High'
        
        # 第二位：detail_list里包含"Involved Illicit Activity"
        if detail_list and isinstance(detail_list, list):
            if any('Involved Illicit Activity' in str(item) for item in detail_list):
                binary_score |= 2  # 2^1 = 2
                level = 'High'
        
        # 第三位：detail_list里包含"Interact with high-risk tag address"
        if detail_list and isinstance(detail_list, list):
            if any('Interact With High-risk Tag Address' in str(item) for item in detail_list):
                binary_score |= 4  # 2^2 = 4
        
        # 第四位：risk_detail里包含"illicit_activity"
        # if risk_detail and isinstance(risk_detail, list):
        #     if any('illicit_activity' in str(item) for item in risk_detail):
        #         binary_score |= 8  # 2^3 = 8
        
        return (level, binary_score)
    
    def mergeRisk(self, level1: str, binary_score1: int, level2: str, binary_score2: int) -> tuple:
        """
        合并两个风险等级和风险评分
        
        参数:
            level1: 第一个风险等级（None, Low, Moderate, High, Unknown）
            binary_score1: 第一个二进制风险评分
            level2: 第二个风险等级（None, Low, Moderate, High, Unknown）
            binary_score2: 第二个二进制风险评分
        
        返回:
            tuple: (level, binary_score) 其中:
                - level (str): 合并后的风险等级（取风险高的）
                - binary_score (int): 合并后的二进制风险评分（按位或操作）
        """
        # 定义风险等级优先级：Unknown < None < Low < Moderate < High
        level_priority = {
            'Unknown': 0,
            'None': 1,
            'Low': 2,
            'Moderate': 3,
            'High': 4
        }
        
        # 获取两个level的优先级，如果level不在映射中，默认使用Unknown的优先级
        priority1 = level_priority.get(level1, 0)
        priority2 = level_priority.get(level2, 0)
        
        # 取风险高的level（优先级数值大的）
        if priority1 >= priority2:
            merged_level = level1
        else:
            merged_level = level2
        
        # 对binary_score进行按位或操作，任意一个score里是1，结果就是1
        merged_binary_score = binary_score1 | binary_score2
        
        return (merged_level, merged_binary_score)