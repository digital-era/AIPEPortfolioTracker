import pandas as pd
import os
import oss2

# 本地文件路径
ORIGINAL_FILE = 'data/AIPEPortfolio.xlsx'
NEW_FILE = 'data/AIPEPortfolio_new.xlsx'

# OSS 配置（从环境变量读取）
OSS_ACCESS_KEY_ID = os.environ.get('OSS_ACCESS_KEY_ID')
OSS_ACCESS_KEY_SECRET = os.environ.get('OSS_ACCESS_KEY_SECRET')
OSS_BUCKET = os.environ.get('OSS_BUCKET')
OSS_ENDPOINT = os.environ.get('OSS_ENDPOINT')


def merge_excel():
    print("Starting Excel merge process...")

    # 读取原始文件
    try:
        df_dazhi_orig = pd.read_excel(ORIGINAL_FILE, sheet_name='大智投资组合')
        df_dazhi_orig['股票代码'] = df_dazhi_orig['股票代码'].astype(str).str.zfill(6)
        df_dacheng_orig = pd.read_excel(ORIGINAL_FILE, sheet_name='大成投资组合')
        df_dacheng_orig['股票代码'] = df_dacheng_orig['股票代码'].astype(str).str.zfill(6)
        df_my_orig = pd.read_excel(ORIGINAL_FILE, sheet_name='我的投资组合')
        df_my_orig['股票代码'] = df_my_orig['股票代码'].astype(str).str.zfill(6)
        print(f"Successfully loaded existing portfolio from {ORIGINAL_FILE}")
    except FileNotFoundError:
        print(f"{ORIGINAL_FILE} not found. Creating new file.")
        df_dazhi_orig = pd.DataFrame()
        df_dacheng_orig = pd.DataFrame()
        df_my_orig = pd.DataFrame()

    # 读取新数据
    df_dazhi_new = pd.read_excel(NEW_FILE, sheet_name='大智投资组合')
    df_dazhi_new['股票代码'] = df_dazhi_new['股票代码'].astype(str).str.zfill(6)
    df_dacheng_new = pd.read_excel(NEW_FILE, sheet_name='大成投资组合')
    df_dacheng_new['股票代码'] = df_dacheng_new['股票代码'].astype(str).str.zfill(6)
    df_my_new = pd.read_excel(NEW_FILE, sheet_name='我的投资组合')
    df_my_new['股票代码'] = df_my_new['股票代码'].astype(str).str.zfill(6)
    print(f"Successfully loaded new data from {NEW_FILE}")

    # 合并
    df_dazhi_combined = pd.concat([df_dazhi_orig, df_dazhi_new], ignore_index=True)
    df_dazhi_combined['股票代码'] = df_dazhi_combined['股票代码'].astype(str).str.zfill(6)
    df_dazhi_combined['修改时间'] = df_dazhi_combined['修改时间'].astype(str)
    df_dacheng_combined = pd.concat([df_dacheng_orig, df_dacheng_new], ignore_index=True)
    df_dacheng_combined['股票代码'] = df_dacheng_combined['股票代码'].astype(str).str.zfill(6)
    df_dacheng_combined['修改时间'] = df_dacheng_combined['修改时间'].astype(str)
    df_my_combined = pd.concat([df_my_orig, df_my_new], ignore_index=True)
    df_my_combined['股票代码'] = df_my_combined['股票代码'].astype(str).str.zfill(6)
    df_my_combined['修改时间'] = df_my_combined['修改时间'].astype(str)
    print("Dataframes concatenated.")

    # 写回原始文件
    with pd.ExcelWriter(ORIGINAL_FILE, engine='openpyxl') as writer:
        df_dazhi_combined.to_excel(writer, sheet_name='大智投资组合', index=False)
        df_dacheng_combined.to_excel(writer, sheet_name='大成投资组合', index=False)
        df_my_combined.to_excel(writer, sheet_name='我的投资组合', index=False)
    
    print(f"Successfully wrote combined data back to {ORIGINAL_FILE}")

    ## 删除新文件，避免重复合并
    #if os.path.exists(NEW_FILE):
        #os.remove(NEW_FILE)
        #print(f"Deleted temporary file {NEW_FILE}")

def upload_to_oss():
    
    print("OSS_ACCESS_KEY_ID",OSS_ACCESS_KEY_ID)
    print("OSS_ACCESS_KEY_SECRET",OSS_ACCESS_KEY_SECRET)
    print("OSS_BUCKET",OSS_BUCKET)
    print("OSS_ENDPOINT",OSS_ENDPOINT)
    if not all([OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_BUCKET, OSS_ENDPOINT]):
        print("OSS credentials not fully configured. Skipping upload.")
        return

    try:
        auth = oss2.Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)
        bucket = oss2.Bucket(auth, OSS_ENDPOINT, OSS_BUCKET)
        bucket.put_object_from_file(ORIGINAL_FILE, ORIGINAL_FILE)
        print(f"Successfully uploaded '{ORIGINAL_FILE}' to OSS bucket '{OSS_BUCKET}'")
    except Exception as e:
        print(f"Error uploading to OSS: {e}")
        exit(1)

def main():
    merge_excel()
    upload_to_oss()

if __name__ == "__main__":
    main()
