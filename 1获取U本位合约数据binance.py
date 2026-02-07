"""
币安永续合约历史持仓数据获取工具
获取合约持仓量(Open Interest)及多空比、基差、资金费率等数据
数据获取后直接导出到Excel
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class BinanceOIHistory:
    """币安合约持仓量历史数据获取类"""
    
    def __init__(self, symbol):
        """
        初始化
        
        参数:
            symbol: 交易对,如 'BTCUSDT', 'ETHUSDT'
        """
        self.base_url = "https://fapi.binance.com"
        self.symbol = symbol.upper()
        self.session = self._create_session()
        
    def _create_session(self):
        """
        创建带有重试机制的session
        """
        session = requests.Session()
        
        # 配置重试策略
        retry_strategy = Retry(
            total=5,  # 最多重试5次
            backoff_factor=1,  # 重试间隔:1秒、2秒、4秒...
            status_forcelist=[429, 500, 502, 503, 504],  # 这些状态码会触发重试
            allowed_methods=["GET"]  # 只对GET请求重试
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _make_request(self, url, params, max_retries=3):
        """
        统一的请求方法,带有重试机制
        
        参数:
            url: 请求URL
            params: 请求参数
            max_retries: 最大重试次数
        
        返回:
            响应数据或None
        """
        for attempt in range(max_retries):
            try:
                response = self.session.get(
                    url, 
                    params=params, 
                    timeout=30,  # 增加超时时间到30秒
                    verify=True  # 启用SSL验证
                )
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.SSLError as e:
                print(f"    ⚠ SSL错误 (尝试 {attempt + 1}/{max_retries}): {str(e)[:100]}")
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    print(f"    ⏳ 等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    print(f"    ✗ 达到最大重试次数,跳过此批次")
                    return None
                    
            except requests.exceptions.Timeout as e:
                print(f"    ⚠ 请求超时 (尝试 {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    print(f"    ⏳ 等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    print(f"    ✗ 达到最大重试次数,跳过此批次")
                    return None
                    
            except requests.exceptions.RequestException as e:
                print(f"    ⚠ 请求错误 (尝试 {attempt + 1}/{max_retries}): {str(e)[:100]}")
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    print(f"    ⏳ 等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    print(f"    ✗ 达到最大重试次数,跳过此批次")
                    return None
        
        return None
        
    def get_open_interest_hist(self, period='5m', start_time=None, end_time=None, limit=500):
        """
        获取历史持仓量数据
        
        参数:
            period: 时间周期 '5m','15m','30m','1h','2h','4h','6h','12h','1d'
            start_time: 开始时间 (datetime对象或毫秒时间戳)
            end_time: 结束时间 (datetime对象或毫秒时间戳)
            limit: 返回数据条数 (最大500)
        
        返回:
            DataFrame: 历史持仓数据
        """
        endpoint = "/futures/data/openInterestHist"
        url = self.base_url + endpoint
        
        # 转换时间格式
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp() * 1000)
        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp() * 1000)
        
        params = {
            'symbol': self.symbol,
            'period': period,
            'limit': limit
        }
        
        if start_time:
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time
        
        data = self._make_request(url, params)
        
        if not data:
            return None
        
        # 转换为DataFrame
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['sumOpenInterest'] = df['sumOpenInterest'].astype(float)
        df['sumOpenInterestValue'] = df['sumOpenInterestValue'].astype(float)
        
        return df
    
    def get_top_long_short_account_ratio(self, period='5m', start_time=None, end_time=None, limit=500):
        """
        获取大户账户数多空比(Top Trader Long/Short Ratio - Accounts)
        
        参数:
            period: 时间周期 '5m','15m','30m','1h','2h','4h','6h','12h','1d'
            start_time: 开始时间
            end_time: 结束时间
            limit: 返回数据条数 (最大500)
        
        返回:
            DataFrame: 大户账户数多空比数据
        """
        endpoint = "/futures/data/topLongShortAccountRatio"
        url = self.base_url + endpoint
        
        # 转换时间格式
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp() * 1000)
        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp() * 1000)
        
        params = {
            'symbol': self.symbol,
            'period': period,
            'limit': limit
        }
        
        if start_time:
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time
        
        data = self._make_request(url, params)
        
        if not data:
            return None
        
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['longShortRatio'] = df['longShortRatio'].astype(float)
        df['longAccount'] = df['longAccount'].astype(float)
        df['shortAccount'] = df['shortAccount'].astype(float)
        
        return df
    
    def get_top_long_short_position_ratio(self, period='5m', start_time=None, end_time=None, limit=500):
        """
        获取大户持仓量多空比(Top Trader Long/Short Ratio - Positions)
        
        参数:
            period: 时间周期 '5m','15m','30m','1h','2h','4h','6h','12h','1d'
            start_time: 开始时间
            end_time: 结束时间
            limit: 返回数据条数 (最大500)
        
        返回:
            DataFrame: 大户持仓量多空比数据
        """
        endpoint = "/futures/data/topLongShortPositionRatio"
        url = self.base_url + endpoint
        
        # 转换时间格式
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp() * 1000)
        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp() * 1000)
        
        params = {
            'symbol': self.symbol,
            'period': period,
            'limit': limit
        }
        
        if start_time:
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time
        
        data = self._make_request(url, params)
        
        if not data:
            return None
        
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['longShortRatio'] = df['longShortRatio'].astype(float)
        df['longAccount'] = df['longAccount'].astype(float)
        df['shortAccount'] = df['shortAccount'].astype(float)
        
        return df
    
    def get_global_long_short_account_ratio(self, period='5m', start_time=None, end_time=None, limit=500):
        """
        获取多空持仓人数比(Global Long/Short Ratio)
        
        参数:
            period: 时间周期 '5m','15m','30m','1h','2h','4h','6h','12h','1d'
            start_time: 开始时间
            end_time: 结束时间
            limit: 返回数据条数 (最大500)
        
        返回:
            DataFrame: 多空持仓人数比数据
        """
        endpoint = "/futures/data/globalLongShortAccountRatio"
        url = self.base_url + endpoint
        
        # 转换时间格式
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp() * 1000)
        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp() * 1000)
        
        params = {
            'symbol': self.symbol,
            'period': period,
            'limit': limit
        }
        
        if start_time:
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time
        
        data = self._make_request(url, params)
        
        if not data:
            return None
        
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['longShortRatio'] = df['longShortRatio'].astype(float)
        df['longAccount'] = df['longAccount'].astype(float)
        df['shortAccount'] = df['shortAccount'].astype(float)
        
        return df
    
    
    def get_basis_data(self, period='5m', start_time=None, end_time=None, limit=500):
        """
        获取基差数据(Basis = 合约价格 - 现货价格)
        """
        endpoint = "/futures/data/basis"
        url = self.base_url + endpoint
        
        # 转换时间格式
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp() * 1000)
        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp() * 1000)
        
        params = {
            'pair': self.symbol,
            'contractType': 'PERPETUAL',
            'period': period,
            'limit': limit
        }
        
        if start_time:
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time
        
        data = self._make_request(url, params)
        
        # *** 关键修复:检查数据有效性 ***
        if not data:
            return None
        
        # 检查是否为列表且不为空
        if not isinstance(data, list):
            print(f"    ⚠ API返回非预期格式: {data}")
            return None
        
        if len(data) == 0:
            print(f"    ⚠ API返回空数据")
            return None
        
        # 转换为DataFrame
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['basisRate'] = df['basisRate'].astype(float)
        df['basis'] = df['basis'].astype(float)
        return df

    def get_funding_rate(self, start_time=None, end_time=None, limit=1000):
        """
        获取资金费率历史数据
        
        参数:
            start_time: 开始时间 (datetime对象或毫秒时间戳)
            end_time: 结束时间 (datetime对象或毫秒时间戳)
            limit: 返回数据条数 (最大1000)
        
        返回:
            DataFrame: 资金费率历史数据
        """
        endpoint = "/fapi/v1/fundingRate"
        url = self.base_url + endpoint
        
        # 转换时间格式
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp() * 1000)
        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp() * 1000)
        
        params = {
            'symbol': self.symbol,
            'limit': min(limit, 1000)  # 资金费率接口最大支持1000
        }
        
        if start_time:
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time
        
        data = self._make_request(url, params)
        
        if not data or not isinstance(data, list) or len(data) == 0:
            return None
        
        # 转换为DataFrame
        df = pd.DataFrame(data)
        df['fundingTime'] = pd.to_datetime(df['fundingTime'], unit='ms')
        df['fundingRate'] = df['fundingRate'].astype(float)
        
        # 重命名列以保持一致性
        df = df.rename(columns={'fundingTime': 'timestamp'})
        
        return df[['timestamp', 'fundingRate']]

    def get_klines(self, period='5m', start_time=None, end_time=None, limit=500):
        """
        获取K线数据(OHLC)
        
        参数:
            period: 时间周期 '5m','15m','30m','1h','2h','4h','6h','12h','1d'
            start_time: 开始时间
            end_time: 结束时间
            limit: 返回数据条数 (最大1500)
        
        返回:
            DataFrame: K线数据
        """
        endpoint = "/fapi/v1/klines"
        url = self.base_url + endpoint
        
        # 转换时间格式
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp() * 1000)
        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp() * 1000)
        
        params = {
            'symbol': self.symbol,
            'interval': period,
            'limit': min(limit, 1500)  # K线接口最大支持1500
        }
        
        if start_time:
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time
        
        data = self._make_request(url, params)
        
        if not data or not isinstance(data, list) or len(data) == 0:
            return None
        
        # 转换为DataFrame
        df = pd.DataFrame(data, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_volume', 'trades', 'taker_buy_volume',
            'taker_buy_quote_volume', 'ignore'
        ])
        
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        # 只保留需要的列
        return df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]



    def _get_batched_data(self, fetch_func, data_name, period, start_date, end_date):
        """
        通用的分批获取数据方法
        
        参数:
            fetch_func: 获取数据的函数
            data_name: 数据名称(用于显示)
            period: 时间周期
            start_date: 开始日期
            end_date: 结束日期
        
        返回:
            DataFrame: 完整数据
        """
        all_data = []
        current_start = start_date
        last_timestamp = None  # 记录上一次的最后时间戳,用于检测是否陷入循环
        
        period_map = {
            '5m': timedelta(minutes=2500),
            '15m': timedelta(minutes=7500),
            '30m': timedelta(minutes=15000),
            '1h': timedelta(hours=500),
            '2h': timedelta(hours=1000),
            '4h': timedelta(hours=2000),
            '6h': timedelta(hours=3000),
            '12h': timedelta(hours=6000),
            '1d': timedelta(days=500),
        }
        
        time_delta = period_map.get(period, timedelta(hours=500))
        
        batch_num = 0
        consecutive_failures = 0  # 连续失败次数
        max_consecutive_failures = 3  # 最大连续失败次数
        
        while current_start < end_date:
            batch_num += 1
            current_end = min(current_start + time_delta, end_date)
            
            print(f"  📥 批次 {batch_num}: 请求时间段 {current_start.strftime('%Y-%m-%d %H:%M')} 至 {current_end.strftime('%Y-%m-%d %H:%M')}")
            
            df = fetch_func(
                period=period,
                start_time=current_start,
                end_time=current_end,
                limit=500
            )
            
            if df is not None and len(df) > 0:
                # 获取本次数据的最后时间戳
                current_last_timestamp = df['timestamp'].iloc[-1]
                
                # 检测是否陷入循环(时间戳没有推进)
                if last_timestamp is not None and current_last_timestamp <= last_timestamp:
                    print(f"  ⚠ 批次 {batch_num}: 时间未推进,已到达数据末尾")
                    break
                
                all_data.append(df)
                print(f"  ✓ 批次 {batch_num}: 成功获取 {len(df)} 条数据 ({df['timestamp'].iloc[0].strftime('%Y-%m-%d %H:%M')} 至 {df['timestamp'].iloc[-1].strftime('%Y-%m-%d %H:%M')})")
                
                # 更新下次开始时间和上次时间戳
                last_timestamp = current_last_timestamp
                current_start = current_last_timestamp + timedelta(milliseconds=1)
                consecutive_failures = 0  # 重置连续失败计数
                
                # 如果已经到达或超过结束时间,停止循环
                if current_last_timestamp >= end_date:
                    print(f"  ✓ 已到达结束时间")
                    break
                    
            else:
                consecutive_failures += 1
                print(f"  ✗ 批次 {batch_num}: 未获取到数据 (连续失败 {consecutive_failures} 次)")
                
                if consecutive_failures >= max_consecutive_failures:
                    print(f"  ⚠ 连续失败 {max_consecutive_failures} 次,停止获取")
                    break
                
                # 即使失败也推进时间,避免无限循环
                current_start = current_end + timedelta(milliseconds=1)
            
            # 请求间隔,避免触发API限制
            time.sleep(2)
        
        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            result = result.drop_duplicates(subset=['timestamp'])
            result = result.sort_values('timestamp').reset_index(drop=True)
            print(f"  ✅ {data_name}获取完成: 共 {len(result)} 条数据")
            print(f"     时间范围: {result['timestamp'].iloc[0].strftime('%Y-%m-%d %H:%M:%S')} 至 {result['timestamp'].iloc[-1].strftime('%Y-%m-%d %H:%M:%S')}\n")
            return result
        else:
            print(f"  ❌ {data_name}获取失败\n")
            return None

    def _get_batched_funding_rate(self, start_date, end_date):
        """
        分批获取资金费率数据(特殊处理,因为资金费率每8小时一次)
        
        参数:
            start_date: 开始日期
            end_date: 结束日期
        
        返回:
            DataFrame: 完整资金费率数据
        """
        all_data = []
        current_start = start_date
        last_timestamp = None
        
        # 资金费率每次最多返回1000条,每8小时一次,所以1000条约覆盖333天
        time_delta = timedelta(days=300)
        
        batch_num = 0
        consecutive_failures = 0
        max_consecutive_failures = 3
        
        while current_start < end_date:
            batch_num += 1
            current_end = min(current_start + time_delta, end_date)
            
            print(f"  📥 批次 {batch_num}: 请求时间段 {current_start.strftime('%Y-%m-%d %H:%M')} 至 {current_end.strftime('%Y-%m-%d %H:%M')}")
            
            df = self.get_funding_rate(
                start_time=current_start,
                end_time=current_end,
                limit=1000
            )
            
            if df is not None and len(df) > 0:
                current_last_timestamp = df['timestamp'].iloc[-1]
                
                if last_timestamp is not None and current_last_timestamp <= last_timestamp:
                    print(f"  ⚠ 批次 {batch_num}: 时间未推进,已到达数据末尾")
                    break
                
                all_data.append(df)
                print(f"  ✓ 批次 {batch_num}: 成功获取 {len(df)} 条数据 ({df['timestamp'].iloc[0].strftime('%Y-%m-%d %H:%M')} 至 {df['timestamp'].iloc[-1].strftime('%Y-%m-%d %H:%M')})")
                
                last_timestamp = current_last_timestamp
                current_start = current_last_timestamp + timedelta(milliseconds=1)
                consecutive_failures = 0
                
                if current_last_timestamp >= end_date:
                    print(f"  ✓ 已到达结束时间")
                    break
                    
            else:
                consecutive_failures += 1
                print(f"  ✗ 批次 {batch_num}: 未获取到数据 (连续失败 {consecutive_failures} 次)")
                
                if consecutive_failures >= max_consecutive_failures:
                    print(f"  ⚠ 连续失败 {max_consecutive_failures} 次,停止获取")
                    break
                
                current_start = current_end + timedelta(milliseconds=1)
            
            time.sleep(2)
        
        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            result = result.drop_duplicates(subset=['timestamp'])
            result = result.sort_values('timestamp').reset_index(drop=True)
            print(f"  ✅ 资金费率数据获取完成: 共 {len(result)} 条数据")
            print(f"     时间范围: {result['timestamp'].iloc[0].strftime('%Y-%m-%d %H:%M:%S')} 至 {result['timestamp'].iloc[-1].strftime('%Y-%m-%d %H:%M:%S')}\n")
            return result
        else:
            print(f"  ❌ 资金费率数据获取失败\n")
            return None
    
    def get_all_comprehensive_data(self, period='5m', start_date=None, end_date=None):
        """
        获取所有综合数据(持仓量、多空比、基差、资金费率等)
        
        参数:
            period: 时间周期
            start_date: 开始日期
            end_date: 结束日期
        
        返回:
            dict: 包含所有数据的字典
        """
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        
        if not start_date:
            start_date = datetime.now() - timedelta(days=30)
        if not end_date:
            end_date = datetime.now()
        
        print(f"\n{'='*70}")
        print(f"开始获取 {self.symbol} 综合数据")
        print(f"时间范围: {start_date.strftime('%Y-%m-%d %H:%M:%S')} 至 {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"时间周期: {period}")
        print(f"{'='*70}\n")
        
        # 1. 获取持仓量数据
        print("📊 [1/7] 获取持仓量数据...")
        oi_data = self._get_batched_data(
            self.get_open_interest_hist,
            "持仓量数据",
            period, start_date, end_date
        )
        
        # 2. 获取大户账户数多空比
        print("📊 [2/7] 获取大户账户数多空比...")
        top_account_ratio = self._get_batched_data(
            self.get_top_long_short_account_ratio,
            "大户账户数多空比",
            period, start_date, end_date
        )
        
        # 3. 获取大户持仓量多空比
        print("📊 [3/7] 获取大户持仓量多空比...")
        top_position_ratio = self._get_batched_data(
            self.get_top_long_short_position_ratio,
            "大户持仓量多空比",
            period, start_date, end_date
        )
        
        # 4. 获取多空持仓人数比
        print("📊 [4/7] 获取多空持仓人数比...")
        global_ratio = self._get_batched_data(
            self.get_global_long_short_account_ratio,
            "多空持仓人数比",
            period, start_date, end_date
        )
        
        # 5. 获取基差数据
        print("📊 [5/7] 获取基差数据...")
        basis_data = self._get_batched_data(
            self.get_basis_data,
            "基差数据",
            period, start_date, end_date
        )

        # 6. 获取K线数据(OHLC)
        print("📊 [6/7] 获取K线数据(OHLC)...")
        klines_data = self._get_batched_data(
            self.get_klines,
            "K线数据",
            period, start_date, end_date
        )

        # 7. 获取资金费率数据
        print("📊 [7/7] 获取资金费率数据...")
        funding_rate_data = self._get_batched_funding_rate(start_date, end_date)

        print(f"{'='*70}")
        print(f"✓ 所有数据获取完成!")
        print(f"{'='*70}\n")

        return {
            'open_interest': oi_data,
            'top_account_ratio': top_account_ratio,
            'top_position_ratio': top_position_ratio,
            'global_ratio': global_ratio,
            'basis': basis_data,
            'klines': klines_data,
            'funding_rate': funding_rate_data  # 添加资金费率数据
        }


    def export_to_excel(self, data_dict, period, start_date, end_date, filename=None):
        """
        导出所有数据到Excel(单个sheet,所有数据合并)
        
        参数:
            data_dict: 包含所有数据的字典
            period: 时间周期,如 '5m', '15m', '1h'
            start_date: 开始日期(datetime对象)
            end_date: 结束日期(datetime对象)
            filename: 自定义文件名(不含扩展名),如果为None则自动生成
        
        返回:
            str: 保存的文件路径
        """
        if not filename:
            # 转换时间格式
            if isinstance(start_date, str):
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            else:
                start_dt = start_date
            
            if isinstance(end_date, str):
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            else:
                end_dt = end_date
            
            # 格式化时间字符串: YYYYMMDD_HHMM (使用北京时间)
            # 将UTC时间转换为北京时间
            start_str = (start_dt + timedelta(hours=8)).strftime('%Y%m%d_%H%M')
            end_str = (end_dt + timedelta(hours=8)).strftime('%Y%m%d_%H%M')
            
            # 构建文件名: 币种_开始时间_结束时间_周期
            filename = f"{self.symbol}_{start_str}_{end_str}_{period}"
        
        filepath = f"{filename}.xlsx"
        
        print(f"正在导出数据到 {filepath}...")
        
        # 辅助函数:将数字转换为n位有效数字
        def round_to_n_sig_figs(x, n=4):
            """将数字保留n位有效数字"""
            if pd.isna(x) or x == 0:
                return x
            from math import log10, floor
            return round(x, -int(floor(log10(abs(x)))) + (n - 1))
        
        # 辅助函数:将UTC时间转换为北京时间字符串
        def to_beijing_time_str(timestamp):
            """将pandas timestamp转换为北京时间字符串"""
            beijing_time = timestamp + pd.Timedelta(hours=8)
            return beijing_time.strftime('%Y-%m-%d %H:%M:%S')
        
        # 合并所有数据到一个DataFrame
        merged_df = None
        
        # 1. 从持仓量数据开始 (保留原数据,不做有效数字处理)
        if data_dict.get('open_interest') is not None:
            df = data_dict['open_interest']
            merged_df = pd.DataFrame({
                '时间': df['timestamp'].apply(to_beijing_time_str),
                '持仓量': df['sumOpenInterest'],  # 保留原数据
                '持仓价值(USD)': df['sumOpenInterestValue'],  # 保留原数据
            })
            print(f"  ✓ 添加持仓量数据")
        
        # 2. 合并大户账户数多空比 (保留4位有效数字)
        if data_dict.get('top_account_ratio') is not None:
            df = data_dict['top_account_ratio']
            temp_df = pd.DataFrame({
                '时间': df['timestamp'].apply(to_beijing_time_str),
                '大户账户多空比': df['longShortRatio'].apply(lambda x: round_to_n_sig_figs(x, 4)),
                '大户多头账户占比': df['longAccount'].apply(lambda x: round_to_n_sig_figs(x, 4)),
                '大户空头账户占比': df['shortAccount'].apply(lambda x: round_to_n_sig_figs(x, 4)),
            })
            if merged_df is not None:
                merged_df = pd.merge(merged_df, temp_df, on='时间', how='outer')
            else:
                merged_df = temp_df
            print(f"  ✓ 添加大户账户数多空比")
        
        # 3. 合并大户持仓量多空比 (保留4位有效数字)
        if data_dict.get('top_position_ratio') is not None:
            df = data_dict['top_position_ratio']
            temp_df = pd.DataFrame({
                '时间': df['timestamp'].apply(to_beijing_time_str),
                '大户持仓多空比': df['longShortRatio'].apply(lambda x: round_to_n_sig_figs(x, 4)),
                '大户多头持仓占比': df['longAccount'].apply(lambda x: round_to_n_sig_figs(x, 4)),
                '大户空头持仓占比': df['shortAccount'].apply(lambda x: round_to_n_sig_figs(x, 4)),
            })
            if merged_df is not None:
                merged_df = pd.merge(merged_df, temp_df, on='时间', how='outer')
            else:
                merged_df = temp_df
            print(f"  ✓ 添加大户持仓量多空比")
        
        # 4. 合并多空持仓人数比 (保留4位有效数字)
        if data_dict.get('global_ratio') is not None:
            df = data_dict['global_ratio']
            temp_df = pd.DataFrame({
                '时间': df['timestamp'].apply(to_beijing_time_str),
                '全市场多空比': df['longShortRatio'].apply(lambda x: round_to_n_sig_figs(x, 4)),
                '全市场多头人数占比': df['longAccount'].apply(lambda x: round_to_n_sig_figs(x, 4)),
                '全市场空头人数占比': df['shortAccount'].apply(lambda x: round_to_n_sig_figs(x, 4)),
            })
            if merged_df is not None:
                merged_df = pd.merge(merged_df, temp_df, on='时间', how='outer')
            else:
                merged_df = temp_df
            print(f"  ✓ 添加多空持仓人数比")
        
        # 5. 合并基差数据 (保留4位有效数字)
        if data_dict.get('basis') is not None:
            df = data_dict['basis']
            temp_df = pd.DataFrame({
                '时间': df['timestamp'].apply(to_beijing_time_str),
                '基差': df['basis'].apply(lambda x: round_to_n_sig_figs(x, 4)),
                '基差率': df['basisRate'].apply(lambda x: round_to_n_sig_figs(x, 4)),
            })
            if merged_df is not None:
                merged_df = pd.merge(merged_df, temp_df, on='时间', how='outer')
            else:
                merged_df = temp_df
            print(f"  ✓ 添加基差数据")
        
        # 6. 合并K线数据(OHLC) (保留4位有效数字)
        if data_dict.get('klines') is not None:
            df = data_dict['klines']
            temp_df = pd.DataFrame({
                '时间': df['timestamp'].apply(to_beijing_time_str),
                '开盘价': df['open'].apply(lambda x: round_to_n_sig_figs(x, 4)),
                '最高价': df['high'].apply(lambda x: round_to_n_sig_figs(x, 4)),
                '最低价': df['low'].apply(lambda x: round_to_n_sig_figs(x, 4)),
                '收盘价': df['close'].apply(lambda x: round_to_n_sig_figs(x, 4)),
                '成交量': df['volume'].apply(lambda x: round_to_n_sig_figs(x, 4)),
            })
            if merged_df is not None:
                merged_df = pd.merge(merged_df, temp_df, on='时间', how='outer')
            else:
                merged_df = temp_df
            print(f"  ✓ 添加K线数据(OHLC)")

        # 7. 合并资金费率数据 (保留6位有效数字,因为资金费率通常很小)
        if data_dict.get('funding_rate') is not None:
            df = data_dict['funding_rate']
            temp_df = pd.DataFrame({
                '时间': df['timestamp'].apply(to_beijing_time_str),
                '资金费率': df['fundingRate'].apply(lambda x: round_to_n_sig_figs(x, 6)),
            })
            if merged_df is not None:
                merged_df = pd.merge(merged_df, temp_df, on='时间', how='outer')
            else:
                merged_df = temp_df
            print(f"  ✓ 添加资金费率数据")

        # 按时间排序
        if merged_df is not None:
            merged_df = merged_df.sort_values('时间').reset_index(drop=True)
            
            # 导出到Excel
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                merged_df.to_excel(writer, sheet_name='综合数据', index=False)
            
            print(f"\n✓ 数据已成功导出到: {filepath}")
            print(f"  总记录数: {len(merged_df)}")
            print(f"  总列数: {len(merged_df.columns)}")
            print(f"  数据精度: 持仓量和持仓价值保留原数据,其他数据保留4位有效数字,资金费率保留6位有效数字")
            print(f"  时区: 北京时间 (UTC+8)\n")
            return filepath
        else:
            print(f"\n✗ 没有数据可导出\n")
            return None


def main():
    """主程序 - 交互式使用"""
    
    print("\n" + "="*70)
    print("币安永续合约综合数据获取工具 v2.2 (新增资金费率)")
    print("="*70 + "\n")
    
    # 1. 输入交易对
    symbol = input("请输入交易对 (例如: BTCUSDT, ETHUSDT): ").strip().upper()
    if not symbol:
        symbol = "BTCUSDT"
        print(f"使用默认: {symbol}")
    
    # 2. 输入时间周期
    print("\n可选时间周期: 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d")
    period = input("请输入时间周期 (默认: 5m): ").strip()
    if not period:
        period = "5m"
    
    # 3. 输入时间范围
    print("\n请输入时间范围 (格式: YYYY-MM-DD)")
    start_input = input("开始日期 (回车使用默认: 30天前): ").strip()
    end_input = input("结束日期 (回车使用默认: 现在): ").strip()
    
    start_date = start_input if start_input else None
    end_date = end_input if end_input else None
    
    # 创建获取器
    fetcher = BinanceOIHistory(symbol)
    
    # 获取所有综合数据
    data_dict = fetcher.get_all_comprehensive_data(
        period=period,
        start_date=start_date,
        end_date=end_date
    )
    
    # 检查是否有数据
    has_data = any(data_dict.get(key) is not None for key in data_dict.keys())
    
    if has_data:
        # 获取实际的开始和结束日期(用于文件名)
        actual_start = None
        actual_end = None
        for key in ['open_interest', 'top_account_ratio', 'top_position_ratio', 'global_ratio', 'basis', 'klines', 'funding_rate']:
            if data_dict.get(key) is not None:
                df = data_dict[key]
                actual_start = df['timestamp'].iloc[0]
                actual_end = df['timestamp'].iloc[-1]
                break
        
        # 自动导出Excel
        custom_filename = input("请输入自定义文件名 (回车使用自动生成): ").strip()
        fetcher.export_to_excel(
            data_dict, 
            period=period,
            start_date=actual_start if actual_start else start_date,
            end_date=actual_end if actual_end else end_date,
            filename=custom_filename if custom_filename else None
        )
    else:
        print("\n✗ 未获取到任何数据,无法导出")



if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n用户中断程序")
    except Exception as e:
        print(f"\n程序错误: {e}")
        import traceback
        traceback.print_exc()