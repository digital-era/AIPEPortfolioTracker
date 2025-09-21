import pandas as pd
import json
from pathlib import Path

# 输入文件（仓库内已有）
INPUT_FILE = Path("data/AIPEPortfolio_new.xlsx")
# 输出文件（生成/覆盖）
OUTPUT_FILE = Path("data/AIPEPortfolio.json")

def main():
    if not INPUT_FILE.exists():
        print(f"❌ 输入文件不存在: {INPUT_FILE}")
        return

    print(f"读取 Excel 文件: {INPUT_FILE}")
    df = pd.read_excel(INPUT_FILE)

    print("转换为 JSON...")
    data = df.to_dict(orient="records")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✅ 已写入: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
