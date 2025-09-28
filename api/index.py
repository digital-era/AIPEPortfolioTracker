# api/index.py
# 版本：API 驱动的动态数据处理
# 描述：此脚本专门用于处理由 Vercel API 触发的 GitHub Action 工作流。
# 它从环境变量中读取 A 股、港股和 ETF 的代码列表，
# 获取这些证券的实时行情数据，并将它们合并后保存到一个单一的 JSON 文件中。

import os
import json
import pandas as pd
import akshare as ak
from datetime import datetime, timezone, timedelta

def process_dynamic_securities_report(df_stock_raw, df_etf_raw, df_hk_stock_raw, trade_date, all_codes):
    """
    处理一个包含A股、港股和ETF代码的统一列表，并返回一个包含其市场数据的列表。
    """
    print(f"\n--- Processing a unified list of {len(all_codes)} dynamic securities ---")
    
    if not all_codes:
        print("No dynamic codes provided to process. Skipping.")
        return []

    # 为不同市场的原始数据创建索引，以便快速查找
    stock_data_map = df_stock_raw.set_index('代码').to_dict('index') if not df_stock_raw.empty else {}
    etf_data_map = df_etf_raw.set_index('代码').to_dict('index') if not df_etf_raw.empty else {}
    hk_stock_data_map = df_hk_stock_raw.set_index('代码').to_dict('index') if not df_hk_stock_raw.empty else {}

    result_list = []
    
    def safe_round(value, digits, divisor=1):
        """安全地将值转换为数字、进行除法和四舍五入。如果失败则返回 None。"""
        numeric_val = pd.to_numeric(value, errors='coerce')
        if pd.isna(numeric_val):
            return None
        return round(numeric_val / divisor, digits)

    # 遍历所有传入的代码
    for code in all_codes:
        item, security_type = None, None
        
        # 判断代码属于哪个市场
        if code in stock_data_map:
            item, security_type = stock_data_map[code], 'stock'
        elif code in hk_stock_data_map:
            item, security_type = hk_stock_data_map[code], 'hk_stock'
        elif code in etf_data_map:
            item, security_type = etf_data_map[code], 'etf'
        
        if not item:
            print(f"  - Warning: Code '{code}' not found in any fetched market data.")
            continue
        
        # 构建标准化的数据结构
        common_info = {
            "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'), 
            "trade_date": trade_date
        }

        if security_type == 'stock':
            security_info = {
                '代码': code, 
                '名称': item.get('名称'), 
                'Price': safe_round(item.get('最新价'), 2), 
                'Percent': safe_round(item.get('涨跌幅'), 2), 
                'Amount': safe_round(item.get('成交额'), 2, 100_000_000), 
                'PE_TTM': safe_round(item.get('市盈率-动态'), 2), 
                'PB': safe_round(item.get('市净率'), 2)
            }
        elif security_type == 'hk_stock':
            security_info = {
                '代码': code, 
                '名称': item.get('名称'), 
                'Price': safe_round(item.get('最新价'), 3), 
                'Percent': safe_round(item.get('涨跌幅'), 2), 
                'Amount': safe_round(item.get('成交额'), 2, 100_000_000)
            }
        elif security_type == 'etf':
            security_info = {
                '代码': code, 
                '名称': item.get('名称'), 
                'Price': safe_round(item.get('最新价'), 3), 
                'Percent': safe_round(item.get('涨跌幅'), 2), 
                'Amount': safe_round(item.get('成交额'), 2, 100_000_000)
            }
        
        # 合并通用信息和特定证券信息
        security_info.update(common_info)
        result_list.append(security_info)
        
    print(f"Successfully processed {len(result_list)} securities from the dynamic list.")
    return result_list

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
        # --- 2. 获取所有市场的实时行情数据 ---
        print("\n--- Starting Data Acquisition Phase ---")
        base_trade_date = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
        df_stock_raw, df_etf_raw, df_hk_stock_raw = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        try:
            df_stock_raw = ak.stock_zh_a_spot_em()
            df_stock_raw['代码'] = df_stock_raw['代码'].astype(str) # 转换为字符串是好习惯
            print(f"Successfully fetched {len(df_stock_raw)} A-share stocks.")
        except Exception as e:
            print(f"Could not fetch A-share stock data: {e}")

        try:
            df_hk_stock_raw = ak.stock_hk_main_board_spot_em()
            # 港股逻辑是正确的，因为前端发送的就是 'HK' + code
            df_hk_stock_raw['代码'] = 'HK' + df_hk_stock_raw['代码'].astype(str)
            print(f"Successfully fetched {len(df_hk_stock_raw)} HK stocks.")
        except Exception as e:
            print(f"Could not fetch HK stock data: {e}")
        
        try:
            df_etf_raw = ak.fund_etf_spot_em()
            # 【关键修改】移除给ETF代码添加 "ETF" 前缀的错误操作
            # df_etf_raw['代码'] = 'ETF' + df_etf_raw['代码'].astype(str)
            
            # 仅将代码列转换为字符串类型，以确保匹配的稳定性
            df_etf_raw['代码'] = df_etf_raw['代码'].astype(str)
            
            print(f"Successfully fetched {len(df_etf_raw)} ETFs.")
            if not df_etf_raw.empty and '数据日期' in df_etf_raw.columns and pd.to_datetime(df_etf_raw['数据日期'].iloc[0], errors='coerce') is not pd.NaT:
                base_trade_date = pd.to_datetime(df_etf_raw['数据日期'].iloc[0]).strftime('%Y-%m-%d')
                print(f"Base trade date set to: {base_trade_date}")
        except Exception as e:
            print(f"Could not fetch ETF data or extract date: {e}. Using fallback date.")

        # --- 3. 处理数据并保存 ---
        final_data = process_dynamic_securities_report(
            df_stock_raw, df_etf_raw, df_hk_stock_raw, base_trade_date, all_dynamic_codes
        )
        
        output_dir = "data"
        output_filename = "stock_dynamic_data_portfolio.json"
        output_filepath = os.path.join(output_dir, output_filename)
        
        os.makedirs(output_dir, exist_ok=True)
        
        with open(output_filepath, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=4)
        
        print(f"\n[Finished] -> Dynamic portfolio data saved to {output_filepath}")
