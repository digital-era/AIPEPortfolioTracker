# api/index.py
# 版本：API 驱动的动态数据处理 - 使用新akshare接口
# 描述：此脚本专门用于处理由 Vercel API 触发的 GitHub Action 工作流。
# 它从环境变量中读取 A 股、港股和 ETF 的代码列表，
# 获取这些证券的实时行情数据，并将它们合并后保存到一个单一的 JSON 文件中。

import os
import json
import pandas as pd
import akshare as ak
from datetime import datetime, timezone, timedelta
import time

def get_stock_symbol_prefix(code):
    """根据A股/ETF代码构造查询前缀"""
    # 增加以5开头的股票返回SH
    if code.startswith('6') or code.startswith('9') or code.startswith('5'):
        return "SH" + code
    else:
        return "SZ" + code

def get_hk_symbol_for_xq(code):
    """处理港股代码用于stock_individual_spot_xq接口，去掉HK前缀并添加HK"""
    if code.startswith('HK'):
        return "HK" + code[2:]  # 去掉HK前缀，然后再添加HK
    return code

def fetch_single_stock_data(symbol, security_type):
    """获取单个股票数据，加入更健壮的错误处理"""
    try:
        if security_type == 'hk_stock':
            # 港股使用stock_individual_spot_xq接口
            hk_symbol = get_hk_symbol_for_xq(symbol)
            print(f"    HK symbol: {hk_symbol}")
            df = ak.stock_individual_spot_xq(symbol=hk_symbol)
        else:
            # A股/ETF使用stock_individual_spot_xq接口
            stock_symbol = get_stock_symbol_prefix(symbol)
            print(f"    A-share/ETF symbol: {stock_symbol}")
            df = ak.stock_individual_spot_xq(symbol=stock_symbol)
        
        # 调试信息：打印返回数据的列名
        if df is not None and not df.empty:
            print(f"    DataFrame columns: {df.columns.tolist()}")
            print(f"    DataFrame shape: {df.shape}")
            # 转换为字典格式
            data_dict = dict(zip(df['item'], df['value']))
            return data_dict
        else:
            print(f"    Empty DataFrame returned for {symbol}")
            return None
            
    except Exception as e:
        print(f"    Detailed error for {symbol}: {str(e)}")
        import traceback
        print(f"    Traceback: {traceback.format_exc()}")
        return None

def process_dynamic_securities_report(all_codes, trade_date):
    """
    处理一个包含A股、港股和ETF代码的统一列表，并返回一个包含其市场数据的列表。
    使用新的akshare接口逐个获取数据。
    """
    print(f"\n--- Processing a unified list of {len(all_codes)} dynamic securities ---")
    
    if not all_codes:
        print("No dynamic codes provided to process. Skipping.")
        return []

    result_list = []
    
    def safe_round(value, digits, divisor=1):
        """安全地将值转换为数字、进行除法和四舍五入。如果失败则返回 None。"""
        if value is None:
            return None
        numeric_val = pd.to_numeric(value, errors='coerce')
        if pd.isna(numeric_val):
            return None
        return round(numeric_val / divisor, digits)

    # 遍历所有传入的代码
    for i, code in enumerate(all_codes):
        print(f"  Fetching data for {code} ({i+1}/{len(all_codes)})")
        
        # 判断代码属于哪个市场
        if code.startswith('HK'):
            security_type = 'hk_stock'
        else:
            # 这里假设所有非HK代码都是A股/ETF
            # 如果需要区分A股和ETF，可以添加更多逻辑
            security_type = 'stock'
        
        # 获取单个股票数据
        item = fetch_single_stock_data(code, security_type)
        
        if not item:
            print(f"  - Warning: Code '{code}' not found in market data.")
            continue
        
        # 构建标准化的数据结构
        common_info = {
            "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'), 
            "trade_date": trade_date
        }

        if security_type == 'stock':
            # A股或ETF
            security_info = {
                '代码': code, 
                '名称': item.get('名称'), 
                'Price': safe_round(item.get('现价'), 3),  # 新接口用'现价'
                'Percent': safe_round(item.get('涨幅'), 2),  # 新接口用'涨幅'
                'Amount': safe_round(item.get('成交额'), 2, 100_000_000),
                'PE_TTM': safe_round(item.get('市盈率(TTM)'), 2),  # 新接口用'市盈率(TTM)'
                'PB': safe_round(item.get('市净率'), 2)
            }
        else:
            # 港股
            security_info = {
                '代码': code, 
                '名称': item.get('名称'), 
                'Price': safe_round(item.get('现价'), 3),  # 港股也用'现价'
                'Percent': safe_round(item.get('涨幅'), 2),  # 港股也用'涨幅'
                'Amount': safe_round(item.get('成交额'), 2, 100_000_000)
            }
        
        # 合并通用信息和特定证券信息
        security_info.update(common_info)
        result_list.append(security_info)
        
        # 添加延迟避免请求过快
        time.sleep(0.1)
        
    print(f"Successfully processed {len(result_list)} securities from the dynamic list.")
    return result_list

def get_trade_date():
    """获取交易日期的备用方案"""
    try:
        # 尝试从A股市场获取一个股票数据来提取日期
        df = ak.stock_individual_spot_xq(symbol="SH000001")
        if not df.empty:
            data_dict = dict(zip(df['item'], df['value']))
            time_str = data_dict.get('时间')
            if time_str:
                trade_date = pd.to_datetime(time_str).strftime('%Y-%m-%d')
                print(f"Trade date extracted from market data: {trade_date}")
                return trade_date
    except Exception as e:
        print(f"Could not extract trade date from market data: {e}")
    
    # 备用：使用当前日期
    trade_date = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
    print(f"Using current date as trade date: {trade_date}")
    return trade_date

if __name__ == "__main__":
    # --- 1. 从环境变量读取和解析输入 ---
    print("--- Reading dynamic lists from environment variables ---")
    
    dynamic_a_list, dynamic_hk_list, dynamic_etf_list = [], [], []
    
    # 解析A股列表
    dynamic_a_list_str = os.environ.get('INPUT_DYNAMICLIST')
    if dynamic_a_list_str:
        try:
            dynamic_a_list = json.loads(dynamic_a_list_str)
            print(f"Found and parsed {len(dynamic_a_list)} codes from A-share list input.")
        except json.JSONDecodeError:
            print(f"Error: Could not parse dynamic A-share list: '{dynamic_a_list_str}'.")
    
    # 解析港股列表
    dynamic_hk_list_str = os.environ.get('INPUT_DYNAMICHKLIST')
    if dynamic_hk_list_str:
        try:
            dynamic_hk_list = json.loads(dynamic_hk_list_str)
            print(f"Found and parsed {len(dynamic_hk_list)} codes from HK-share list input.")
        except json.JSONDecodeError:
            print(f"Error: Could not parse dynamic HK-share list: '{dynamic_hk_list_str}'.")
            
    # 解析ETF列表
    dynamic_etf_list_str = os.environ.get('INPUT_DYNAMICETFLIST')
    if dynamic_etf_list_str:
        try:
            dynamic_etf_list = json.loads(dynamic_etf_list_str)
            print(f"Found and parsed {len(dynamic_etf_list)} codes from ETF list input.")
        except json.JSONDecodeError:
            print(f"Error: Could not parse dynamic ETF list: '{dynamic_etf_list_str}'.")

    # 合并所有列表
    all_dynamic_codes = dynamic_a_list + dynamic_hk_list + dynamic_etf_list
    
    if not all_dynamic_codes:
        print("\nNo dynamic codes to process. Exiting script.")
    else:
        # --- 2. 获取交易日期 ---
        print("\n--- Starting Data Acquisition Phase ---")
        base_trade_date = get_trade_date()

        # --- 3. 处理数据并保存 ---
        final_data = process_dynamic_securities_report(all_dynamic_codes, base_trade_date)
        
        output_dir = "data"
        output_filename = "stock_dynamic_data_portfolio.json"
        output_filepath = os.path.join(output_dir, output_filename)
        
        os.makedirs(output_dir, exist_ok=True)
        
        with open(output_filepath, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=4)
        
        print(f"\n[Finished] -> Dynamic portfolio data saved to {output_filepath}")
