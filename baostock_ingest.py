#!/usr/bin/env python3
"""Baostock demo: download A-share daily data into partitioned Parquet files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

import baostock as bs
import duckdb
import pandas as pd


DEFAULT_CONFIG = "baostock_config.json"


def load_config(path: Path) -> Dict[str, object]:
    path = path.expanduser()
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def parse_args() -> argparse.Namespace:
    base = argparse.ArgumentParser(add_help=False)
    base.add_argument("--config", default=DEFAULT_CONFIG)
    early, remaining = base.parse_known_args()
    defaults = load_config(Path(early.config))

    parser = argparse.ArgumentParser(parents=[base], description="Baostock daily downloader")
    parser.add_argument("--start", default=defaults.get("start"))
    parser.add_argument("--end", default=defaults.get("end"))
    parser.add_argument("--symbols", nargs="*", default=defaults.get("symbols"))
    parser.add_argument("--symbols-file", default=defaults.get("symbols_file", ""))
    parser.add_argument(
        "--symbols-mode",
        choices=("list", "file", "all"),
        default=defaults.get("symbols_mode", "list"),
        help="Select symbol source: explicit list, file, or Baostock universe.",
    )
    parser.add_argument(
        "--include-bj",
        action="store_true",
        default=defaults.get("include_bj", False),
        help="Include Beijing exchange when symbols-mode=all.",
    )
    parser.add_argument("--output-root", default=defaults.get("output_root", "baostock_data"))
    parser.add_argument("--keep-temp", action="store_true", default=defaults.get("keep_temp", False))
    args = parser.parse_args(remaining)
    for key in ("start", "end"):
        if not getattr(args, key):
            parser.error(f"Missing required parameter: {key}")
    return args


def download_daily(symbol: str, start: str, end: str) -> pd.DataFrame:
    fields = "date,code,open,high,low,close,volume,amount,adjustflag"
    rs = bs.query_history_k_data_plus(symbol, fields, start_date=start, end_date=end, frequency="d", adjustflag="3")
    data = []
    while rs.error_code == "0" and rs.next():
        data.append(rs.get_row_data())
    df = pd.DataFrame(data, columns=fields.split(","))
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    numeric_cols = ["open", "high", "low", "close", "volume", "amount"]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    return df


def write_partition(df: pd.DataFrame, output_dir: Path, keep_temp: bool) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_path = output_dir / "tmp.parquet"
    df.to_parquet(temp_path, index=False)
    con = duckdb.connect()
    con.execute(
        f"""
        COPY (SELECT *, date AS partition_date FROM read_parquet('{temp_path}'))
        TO '{output_dir / 'part-0000.parquet'}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
        """
    )
    con.close()
    if not keep_temp:
        temp_path.unlink(missing_ok=True)


def load_symbols_from_file(path: str) -> List[str]:
    file_path = Path(path).expanduser()
    if not file_path.exists():
        raise SystemExit(f"symbols file not found: {file_path}")
    symbols = [line.strip() for line in file_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not symbols:
        raise SystemExit(f"symbols file is empty: {file_path}")
    return symbols


def fetch_all_symbols(trade_date: str, include_bj: bool) -> List[str]:
    trade_day = resolve_latest_trade_day(trade_date)
    rs = bs.query_all_stock(trade_day)
    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        raise SystemExit("Baostock query_all_stock returned no symbols.")
    df = pd.DataFrame(rows, columns=rs.fields)
    prefixes = ["sh.", "sz."]
    if include_bj:
        prefixes.append("bj.")
    symbols = [code for code in df["code"].tolist() if any(code.startswith(pref) for pref in prefixes)]
    if not symbols:
        raise SystemExit("No symbols matched the requested exchanges.")
    return sorted(set(symbols))


def resolve_latest_trade_day(date_str: str) -> str:
    """Ensure we query Baostock with a valid trading day (handles weekends/holidays)."""
    target = pd.Timestamp(date_str)
    for _ in range(60):
        day_str = target.strftime("%Y-%m-%d")
        rs = bs.query_trade_dates(start_date=day_str, end_date=day_str)
        rows = []
        while rs.error_code == "0" and rs.next():
            rows.append(rs.get_row_data())
        if rows:
            record = rows[0]
            if record[-1] == "1":  # is_trading_day field
                return day_str
        target -= pd.Timedelta(days=1)
    raise SystemExit(f"Could not find a trading day before {date_str}")


def main() -> None:
    args = parse_args()
    lg = bs.login()
    if lg.error_code != "0":
        raise SystemExit(f"Baostock login failed: {lg.error_msg}")
    try:
        if args.symbols_mode == "all":
            symbols = fetch_all_symbols(args.start, args.include_bj)
        elif args.symbols_mode == "file":
            symbols = load_symbols_from_file(args.symbols_file)
        else:
            symbols = args.symbols or ["sh.600000"]
        for symbol in symbols:
            df = download_daily(symbol, args.start, args.end)
            if df.empty:
                print(f"{symbol}: no data in range")
                continue
            output_dir = Path(args.output_root) / f"symbol={symbol.replace('.', '_')}" / f"range={args.start}_{args.end}"
            write_partition(df, output_dir, args.keep_temp)
            print(f"Saved {symbol} -> {output_dir}")
    finally:
        bs.logout()


if __name__ == "__main__":
    main()
