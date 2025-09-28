import os
import json
import pandas as pd
import akshare as ak
from datetime import datetime, timezone, timedelta
import time

def safe_akshare_fetch(func, *args, **kwargs):
    """
    安全获取akshare数据，带重试机制（修正版）
    """
    max_retries = 3
    base_delay = 2  # 秒
    
    for attempt in range(max_retries):
        try:
            # 移除timeout参数，因为akshare函数不接受
            result = func(*args, **kwargs)
            print(f"第 {attempt + 1} 次尝试成功获取数据")
            return result
        except Exception as e:
            print(f"第 {attempt + 1} 次尝试失败: {e}")
            if attempt < max_retries - 1:
                wait_time = base_delay * (2 ** attempt)  # 指数退避
                print(f"等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                print(f"所有 {max_retries} 次尝试都失败了")
                # 返回空DataFrame防止程序崩溃
                return pd.DataFrame()

def process_dynamic_securities_report(df_stock_raw, df_etf_raw, df_hk_stock_raw, trade_date, all_codes):
    """
    处理包含A股、港股和ETF代码的统一列表，返回市场数据列表
    """
    print(f"\n--- 正在处理 {len(all_codes)} 个动态证券的统一列表 ---")
    
    if not all_codes:
        print("没有提供动态代码，跳过处理。")
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
            print(f"  - 警告: 代码 '{code}' 在任何获取的市场数据中未找到。")
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
        
    print(f"成功从动态列表中处理了 {len(result_list)} 个证券。")
    return result_list

if __name__ == "__main__":
    # --- 1. 从环境变量读取和解析输入 ---
    print("--- 从环境变量读取动态列表 ---")
    
    dynamic_a_list, dynamic_hk_list, dynamic_etf_list = [], [], []
    
    # 解析A股列表
    dynamic_a_list_str = os.environ.get('INPUT_DYNAMICLIST')
    if dynamic_a_list_str:
        try:
            dynamic_a_list = json.loads(dynamic_a_list_str)
            print(f"从A股列表输入中找到并解析了 {len(dynamic_a_list)} 个代码。")
        except json.JSONDecodeError:
            print(f"错误: 无法解析动态A股列表: '{dynamic_a_list_str}'。")
    
    # 解析港股列表
    dynamic_hk_list_str = os.environ.get('INPUT_DYNAMICHKLIST')
    if dynamic_hk_list_str:
        try:
            dynamic_hk_list = json.loads(dynamic_hk_list_str)
            print(f"从港股列表输入中找到并解析了 {len(dynamic_hk_list)} 个代码。")
        except json.JSONDecodeError:
            print(f"错误: 无法解析动态港股列表: '{dynamic_hk_list_str}'。")
            
    # 解析ETF列表
    dynamic_etf_list_str = os.environ.get('INPUT_DYNAMICETFLIST')
    if dynamic_etf_list_str:
        try:
            dynamic_etf_list = json.loads(dynamic_etf_list_str)
            print(f"从ETF列表输入中找到并解析了 {len(dynamic_etf_list)} 个代码。")
        except json.JSONDecodeError:
            print(f"错误: 无法解析动态ETF列表: '{dynamic_etf_list_str}'。")

    # 合并所有列表
    all_dynamic_codes = dynamic_a_list + dynamic_hk_list + dynamic_etf_list
    
    if not all_dynamic_codes:
        print("\n没有动态代码需要处理，退出脚本。")
    else:
        # --- 2. 获取所有市场的实时行情数据 ---
        print("\n--- 开始数据获取阶段 ---")
        base_trade_date = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
        df_stock_raw, df_etf_raw, df_hk_stock_raw = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        try:
            df_stock_raw = safe_akshare_fetch(ak.stock_zh_a_spot_em)
            df_stock_raw['代码'] = df_stock_raw['代码'].astype(str)
            print(f"成功获取 {len(df_stock_raw)} 只A股数据。")
        except Exception as e:
            print(f"重试后仍无法获取A股数据: {e}")

        try:
            df_hk_stock_raw = safe_akshare_fetch(ak.stock_hk_main_board_spot_em)
            df_hk_stock_raw['代码'] = 'HK' + df_hk_stock_raw['代码'].astype(str)
            print(f"成功获取 {len(df_hk_stock_raw)} 只港股数据。")
        except Exception as e:
            print(f"重试后仍无法获取港股数据: {e}")
        
        try:
            df_etf_raw = safe_akshare_fetch(ak.fund_etf_spot_em)
            df_etf_raw['代码'] = df_etf_raw['代码'].astype(str)
            print(f"成功获取 {len(df_etf_raw)} 只ETF数据。")
            if not df_etf_raw.empty and '数据日期' in df_etf_raw.columns and pd.to_datetime(df_etf_raw['数据日期'].iloc[0], errors='coerce') is not pd.NaT:
                base_trade_date = pd.to_datetime(df_etf_raw['数据日期'].iloc[0]).strftime('%Y-%m-%d')
                print(f"基础交易日期设置为: {base_trade_date}")
        except Exception as e:
            print(f"重试后仍无法获取ETF数据或提取日期: {e}。使用备用日期。")

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
        
        print(f"\n[完成] -> 动态投资组合数据已保存到 {output_filepath}")
