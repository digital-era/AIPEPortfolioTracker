# api/index.py
# 版本：API 驱动的分布式数据处理
# 描述：此脚本设计为由 GitHub Action 工作流触发，用于处理特定类型的证券列表（A股、港股或ETF）。
# 它会根据传入的参数，获取相应的数据，并将其保存到一个独立的、临时的 JSON 文件中，等待后续的合并处理。

import os
import json
import pandas as pd
import akshare as ak
from datetime import datetime, timezone, timedelta

def process_dynamic_securities_report(df_stock_raw, df_etf_raw, df_hk_stock_raw, trade_date, all_codes):
    """
    处理一个包含A股、港股或ETF代码的列表，并返回一个包含其市场数据的列表。
    (此函数与您原始版本完全相同，功能完善，无需修改)
    """
    print(f"\n--- Processing a list of {len(all_codes)} dynamic securities ---")
    
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
            security_info = {'代码': code, '名称': item.get('名称'), 'Price': safe_round(item.get('最新价'), 2), 'Percent': safe_round(item.get('涨跌幅'), 2), 'Amount': safe_round(item.get('成交额'), 2, 100_000_000), 'PE_TTM': safe_round(item.get('市盈率-动态'), 2), 'PB': safe_round(item.get('市净率'), 2)}
        elif security_type == 'hk_stock':
            security_info = {'代码': code, '名称': item.get('名称'), 'Price': safe_round(item.get('最新价'), 3), 'Percent': safe_round(item.get('涨跌幅'), 2), 'Amount': safe_round(item.get('成交额'), 2, 100_000_000)}
        elif security_type == 'etf':
            security_info = {'代码': code, '名称': item.get('名称'), 'Price': safe_round(item.get('最新价'), 3), 'Percent': safe_round(item.get('涨跌幅'), 2), 'Amount': safe_round(item.get('成交额'), 2, 100_000_000)}
        
        security_info.update(common_info)
        result_list.append(security_info)
        
    print(f"Successfully processed {len(result_list)} securities from the dynamic list.")
    return result_list

if __name__ == "__main__":
    # --- 1. 确定本次运行的模式和输出文件 ---
    list_type = os.environ.get('INPUT_LISTTYPE')
    if not list_type:
        raise ValueError("FATAL: Environment variable 'INPUT_LISTTYPE' must be set. (e.g., 'a_shares', 'hk_shares', 'etf')")

    output_filename = f"partial_{list_type}_{datetime.now().timestamp()}.json"
    print(f"--- Running in '{list_type}' mode. Output will be '{output_filename}' ---")

    # --- 2. 解析对应列表的输入 ---
    dynamic_list_str, dynamic_codes = "[]", []
    if list_type == 'a_shares':
        dynamic_list_str = os.environ.get('INPUT_DYNAMICLIST', '[]')
    elif list_type == 'hk_shares':
        dynamic_list_str = os.environ.get('INPUT_DYNAMICHKLIST', '[]')
    elif list_type == 'etf':
        dynamic_list_str = os.environ.get('INPUT_DYNAMICETFLIST', '[]')
    
    try:
        dynamic_codes = json.loads(dynamic_list_str)
        if not isinstance(dynamic_codes, list):
            raise json.JSONDecodeError("Input is not a JSON array.", dynamic_list_str, 0)
        print(f"Found and parsed {len(dynamic_codes)} codes from input.")
    except json.JSONDecodeError as e:
        print(f"Error: Could not parse dynamic list: '{dynamic_list_str}'. Details: {e}")
        dynamic_codes = []

    if not dynamic_codes:
        print("\nNo dynamic codes to process. Exiting script gracefully.")
    else:
        # --- 3. 获取所有市场的实时行情数据 (逻辑不变) ---
        print("\n--- Starting Data Acquisition Phase ---")
        base_trade_date = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
        df_stock_raw, df_etf_raw, df_hk_stock_raw = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        try:
            df_stock_raw = ak.stock_zh_a_spot_em()
            df_stock_raw['代码'] = df_stock_raw['代码'].astype(str)
            print(f"Successfully fetched {len(df_stock_raw)} A-share stocks.")
        except Exception as e:
            print(f"Could not fetch A-share stock data: {e}")

        try:
            df_hk_stock_raw = ak.stock_hk_main_board_spot_em()
            df_hk_stock_raw['代码'] = 'HK' + df_hk_stock_raw['代码'].astype(str)
            print(f"Successfully fetched {len(df_hk_stock_raw)} HK stocks.")
        except Exception as e:
            print(f"Could not fetch HK stock data: {e}")
        
        try:
            df_etf_raw = ak.fund_etf_spot_em()
            df_etf_raw['代码'] = df_etf_raw['代码'].astype(str)
            print(f"Successfully fetched {len(df_etf_raw)} ETFs.")
            if not df_etf_raw.empty and '数据日期' in df_etf_raw.columns and pd.to_datetime(df_etf_raw['数据日期'].iloc[0], errors='coerce') is not pd.NaT:
                base_trade_date = pd.to_datetime(df_etf_raw['数据日期'].iloc[0]).strftime('%Y-%m-%d')
        except Exception as e:
            print(f"Could not fetch ETF data or extract date: {e}")

        # --- 4. 处理数据并保存到临时文件 ---
        final_data = process_dynamic_securities_report(
            df_stock_raw, df_etf_raw, df_hk_stock_raw, base_trade_date, dynamic_codes
        )
        
        output_dir = "data"
        output_filepath = os.path.join(output_dir, output_filename)
        
        os.makedirs(output_dir, exist_ok=True)
        
        with open(output_filepath, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=4)
        
        print(f"\n[Finished] -> Partial data for '{list_type}' saved to {output_filepath}")
