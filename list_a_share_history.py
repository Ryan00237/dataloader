#!/usr/bin/env python3
"""
List historical A-share securities (including delisted stocks) with rich metadata.

Examples:
    python list_a_share_history.py --output data/past_listings.parquet
    python list_a_share_history.py --status both --format csv --markets sh,sz,bj
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, List

import akshare as ak
import pandas as pd

MARKET_CHOICES = ("sh", "sz", "bj")
DEFAULT_OUTPUT = "past_a_share_listings.parquet"
DEFAULT_SH_BOARDS = ("主板A股", "科创板")
DEFAULT_SZ_BOARDS = ("A股列表",)
COLUMN_ORDER = [
    "code",
    "symbol",
    "name",
    "market",
    "board",
    "status",
    "list_date",
    "delist_date",
    "company_full_name",
    "industry",
    "region",
    "total_shares",
    "float_shares",
    "source",
    "source_indicator",
    "retrieved_at",
]
MARKET_PREFIX = {"sh": "sh", "sz": "sz", "bj": "bj"}


def csv_list(value: str, *, lowercase: bool = False) -> List[str]:
    if not value:
        return []
    items = []
    for item in value.split(","):
        stripped = item.strip()
        if not stripped:
            continue
        items.append(stripped.lower() if lowercase else stripped)
    return items


def parse_markets(raw: str) -> List[str]:
    if not raw:
        raise SystemExit("请至少指定一个市场，例如 --markets sh,sz")
    raw = raw.strip().lower()
    if raw in {"all", "*"}:
        return list(MARKET_CHOICES)
    markets = []
    for item in raw.split(","):
        key = item.strip().lower()
        if not key:
            continue
        if key not in MARKET_CHOICES:
            raise SystemExit(f"未知市场：{key}，可选值：{', '.join(MARKET_CHOICES)}")
        markets.append(key)
    return sorted(set(markets))


def parse_date(value: str | None, label: str) -> pd.Timestamp | None:
    if not value:
        return None
    try:
        return pd.Timestamp(value).tz_localize(None)
    except ValueError as exc:  # pragma: no cover - defensive
        raise SystemExit(f"无法解析 {label}: {value}") from exc


def coerce_numeric(series: pd.Series) -> pd.Series:
    text = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("--", "", regex=False)
    )
    return pd.to_numeric(text, errors="coerce")


def fetch_sh_active(boards: Iterable[str]) -> List[pd.DataFrame]:
    frames: List[pd.DataFrame] = []
    for board in boards:
        board = board.strip()
        if not board:
            continue
        df = ak.stock_info_sh_name_code(board)
        df = df.rename(
            columns={
                "证券代码": "code",
                "证券简称": "name",
                "公司全称": "company_full_name",
                "上市日期": "list_date",
            }
        )
        df["market"] = "sh"
        df["board"] = board
        df["status"] = "active"
        df["delist_date"] = pd.NaT
        df["industry"] = pd.NA
        df["region"] = pd.NA
        df["total_shares"] = pd.NA
        df["float_shares"] = pd.NA
        df["source"] = "stock_info_sh_name_code"
        df["source_indicator"] = board
        frames.append(df)
    return frames


def fetch_sz_active(indicators: Iterable[str]) -> List[pd.DataFrame]:
    frames: List[pd.DataFrame] = []
    for indicator in indicators:
        indicator = indicator.strip()
        if not indicator:
            continue
        df = ak.stock_info_sz_name_code(indicator)
        rename_map = {
            "A股代码": "code",
            "A股简称": "name",
            "A股上市日期": "list_date",
            "A股总股本": "total_shares",
            "A股流通股本": "float_shares",
            "所属行业": "industry",
            "板块": "board",
        }
        df = df.rename(columns=rename_map)
        for new_column in rename_map.values():
            if new_column not in df.columns:
                df[new_column] = pd.NA
        df["market"] = "sz"
        df["status"] = "active"
        df["delist_date"] = pd.NaT
        df["company_full_name"] = pd.NA
        df["region"] = pd.NA
        df["source"] = "stock_info_sz_name_code"
        df["source_indicator"] = indicator
        frames.append(df)
    return frames


def fetch_bj_active() -> pd.DataFrame:
    df = ak.stock_info_bj_name_code()
    df = df.rename(
        columns={
            "证券代码": "code",
            "证券简称": "name",
            "上市日期": "list_date",
            "总股本": "total_shares",
            "流通股本": "float_shares",
            "所属行业": "industry",
            "地区": "region",
        }
    )
    df["market"] = "bj"
    df["board"] = "北交所"
    df["status"] = "active"
    df["delist_date"] = pd.NaT
    df["company_full_name"] = pd.NA
    df["source"] = "stock_info_bj_name_code"
    df["source_indicator"] = "上市公司基本信息"
    return df


def fetch_sh_delisted() -> pd.DataFrame:
    df = ak.stock_info_sh_delist()
    df = df.rename(
        columns={
            "公司代码": "code",
            "公司简称": "name",
            "上市日期": "list_date",
            "暂停上市日期": "delist_date",
        }
    )
    df["market"] = "sh"
    df["board"] = "退市"
    df["status"] = "delisted"
    df["company_full_name"] = pd.NA
    df["industry"] = pd.NA
    df["region"] = pd.NA
    df["total_shares"] = pd.NA
    df["float_shares"] = pd.NA
    df["source"] = "stock_info_sh_delist"
    df["source_indicator"] = "delisted"
    return df


def fetch_sz_delisted() -> pd.DataFrame:
    df = ak.stock_info_sz_delist()
    df = df.rename(
        columns={
            "证券代码": "code",
            "证券简称": "name",
            "上市日期": "list_date",
            "终止上市日期": "delist_date",
        }
    )
    df["market"] = "sz"
    df["board"] = "退市"
    df["status"] = "delisted"
    df["company_full_name"] = pd.NA
    df["industry"] = pd.NA
    df["region"] = pd.NA
    df["total_shares"] = pd.NA
    df["float_shares"] = pd.NA
    df["source"] = "stock_info_sz_delist"
    df["source_indicator"] = "delisted"
    return df


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    for column in ("code", "name", "board", "status", "market"):
        if column in data.columns:
            data[column] = data[column].astype("string").str.strip()
    if "code" in data.columns:
        data = data[data["code"] != ""]
    if "list_date" in data.columns:
        data["list_date"] = pd.to_datetime(data["list_date"], errors="coerce")
    if "delist_date" in data.columns:
        data["delist_date"] = pd.to_datetime(data["delist_date"], errors="coerce")
    for column in ("total_shares", "float_shares"):
        if column in data.columns:
            data[column] = coerce_numeric(data[column])
    if "code" in data.columns and "market" in data.columns:
        needs_pad = data["market"].isin(["sh", "sz"])
        data.loc[needs_pad, "code"] = (
            data.loc[needs_pad, "code"].astype("string").str.zfill(6)
        )
    for column in COLUMN_ORDER:
        if column not in data.columns:
            data[column] = pd.NA
    data["retrieved_at"] = pd.Timestamp.utcnow().tz_localize(None)
    data = data[COLUMN_ORDER]
    prefix = data["market"].map(MARKET_PREFIX).fillna("")
    data["symbol"] = prefix + data["code"]
    return data


def apply_filters(
    df: pd.DataFrame,
    list_start: pd.Timestamp | None,
    list_end: pd.Timestamp | None,
    delist_start: pd.Timestamp | None,
    delist_end: pd.Timestamp | None,
) -> pd.DataFrame:
    data = df
    if list_start is not None:
        data = data[data["list_date"].notna() & (data["list_date"] >= list_start)]
    if list_end is not None:
        data = data[data["list_date"].notna() & (data["list_date"] <= list_end)]
    if delist_start is not None:
        data = data[
            data["delist_date"].notna() & (data["delist_date"] >= delist_start)
        ]
    if delist_end is not None:
        data = data[data["delist_date"].notna() & (data["delist_date"] <= delist_end)]
    return data


def write_output(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="导出包含上市/退市日期的 A 股股票清单。")
    parser.add_argument(
        "--markets",
        default="sh,sz",
        help="需要抓取的市场，逗号分隔，可选 {sh,sz,bj}，或使用 all。",
    )
    parser.add_argument(
        "--status",
        choices=("delisted", "active", "both"),
        default="delisted",
        help="导出退市、在市或全部股票，默认仅退市股票。",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="输出文件路径（Parquet 格式）。",
    )
    parser.add_argument(
        "--sh-boards",
        default=",".join(DEFAULT_SH_BOARDS),
        help="上交所板块列表，逗号分隔，对 status active/both 生效。",
    )
    parser.add_argument(
        "--sz-boards",
        default=",".join(DEFAULT_SZ_BOARDS),
        help="深交所列表选项，逗号分隔，对 status active/both 生效。",
    )
    parser.add_argument("--list-start", help="上市日期起始（YYYY-MM-DD）。")
    parser.add_argument("--list-end", help="上市日期结束。")
    parser.add_argument("--delist-start", help="退市日期起始（仅对退市股票生效）。")
    parser.add_argument("--delist-end", help="退市日期结束。")
    parser.add_argument(
        "--sort-by",
        default="list_date",
        help="排序列，默认 list_date，可输入任意字段名，例如 delist_date、code。",
    )
    parser.add_argument(
        "--descending",
        action="store_true",
        help="降序排序，默认升序。",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="仅输出前 N 行，用于调试，0 表示不限制。",
    )
    args = parser.parse_args()

    markets = parse_markets(args.markets)
    list_start = parse_date(args.list_start, "list-start")
    list_end = parse_date(args.list_end, "list-end")
    delist_start = parse_date(args.delist_start, "delist-start")
    delist_end = parse_date(args.delist_end, "delist-end")

    frames: List[pd.DataFrame] = []
    if args.status in {"active", "both"}:
        if "sh" in markets:
            frames.extend(fetch_sh_active(csv_list(args.sh_boards)))
        if "sz" in markets:
            frames.extend(fetch_sz_active(csv_list(args.sz_boards)))
        if "bj" in markets:
            frames.append(fetch_bj_active())
    if args.status in {"delisted", "both"}:
        if "sh" in markets:
            frames.append(fetch_sh_delisted())
        if "sz" in markets:
            frames.append(fetch_sz_delisted())
        if "bj" in markets:
            print("北交所暂未提供退市股票列表，跳过。")

    if not frames:
        raise SystemExit("未获取到任何数据，请检查参数。")

    df = pd.concat(frames, ignore_index=True)
    df = normalize(df)
    df = apply_filters(df, list_start, list_end, delist_start, delist_end)
    if args.limit > 0:
        df = df.head(args.limit)
    if args.sort_by:
        if args.sort_by not in df.columns:
            raise SystemExit(f"排序列 {args.sort_by} 不存在。可选：{', '.join(df.columns)}")
        df = df.sort_values(args.sort_by, ascending=not args.descending, kind="mergesort")

    output_path = Path(args.output).expanduser()
    write_output(df, output_path)
    list_span = (
        f"{df['list_date'].min():%Y-%m-%d} -> {df['list_date'].max():%Y-%m-%d}"
        if df["list_date"].notna().any()
        else "N/A"
    )
    delist_span = (
        f"{df['delist_date'].min():%Y-%m-%d} -> {df['delist_date'].max():%Y-%m-%d}"
        if df["delist_date"].notna().any()
        else "N/A"
    )
    print(
        f"导出 {len(df)} 条记录到 {output_path} （Parquet）。"
        f" 上市区间：{list_span}，退市区间：{delist_span}"
    )


if __name__ == "__main__":
    main()
