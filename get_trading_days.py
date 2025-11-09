from pathlib import Path

import akshare as ak
import pandas as pd

OUTPUT_PATH = Path("a_share_trading_days.parquet")

# 全量交易日（上交所+深交所一致即可）
# 接口返回列名为 trade_date，先统一转换为 pandas 的 Timestamp
cal = ak.tool_trade_date_hist_sina()
cal["trade_date"] = pd.to_datetime(cal["trade_date"])
cal = cal.sort_values("trade_date").reset_index(drop=True)

# 截取区间（示例：2010-01-01 至官方已公布的最后一个交易日）
start = pd.Timestamp("2010-01-01")
end = cal["trade_date"].max()
mask = (cal["trade_date"] >= start) & (cal["trade_date"] <= end)
trade_days = cal.loc[mask, ["trade_date"]].reset_index(drop=True)

trade_days.to_parquet(OUTPUT_PATH, index=False)
print(
    f"保存交易日到 {OUTPUT_PATH}，共 {len(trade_days)} 天。"
    f"区间 {trade_days['trade_date'].min().date()} -> {trade_days['trade_date'].max().date()}"
)
