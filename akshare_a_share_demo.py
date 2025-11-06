#!/usr/bin/env python3
"""
Simple A-share data demo built with AkShare.

Defaults can be configured via akshare_config.json (JSON format).

Run this script after installing:
    pip install akshare pyarrow
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import List, Tuple

import akshare as ak
import pandas as pd


DEFAULT_CONFIG_PATH = "akshare_config.json"


def load_config(path: str) -> dict:
    config_path = Path(path).expanduser()
    if not config_path.exists():
        return {}
    try:
        with config_path.open("r", encoding="utf-8") as file:
            raw_config = json.load(file)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"配置文件解析失败：{exc}") from exc

    config: dict = {}
    if "symbol" in raw_config:
        config["symbol"] = str(raw_config["symbol"])
    if "frequency" in raw_config:
        config["frequency"] = str(raw_config["frequency"])
    if "period" in raw_config:
        config["period"] = str(raw_config["period"])
    if "start" in raw_config:
        config["start"] = str(raw_config["start"])
    if "end" in raw_config:
        config["end"] = str(raw_config["end"])
    if "adjust" in raw_config:
        config["adjust"] = str(raw_config["adjust"])
    if "limit" in raw_config:
        config["limit"] = int(raw_config["limit"])
    if "output_dir" in raw_config:
        config["output_dir"] = str(raw_config["output_dir"])
    return config


def parse_args() -> argparse.Namespace:
    base_parser = argparse.ArgumentParser(add_help=False)
    base_parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help="配置文件路径（JSON），用于覆盖命令行默认参数。",
    )
    config_args, remaining = base_parser.parse_known_args()
    config_defaults = load_config(config_args.config)

    parser = argparse.ArgumentParser(
        parents=[base_parser],
        description=(
            "Fetch A-share data with AkShare. Defaults cover the last 30 "
            "calendar days for the A-share symbol sh600000 (浦发银行). "
            "Add --frequency minute to request intraday bars."
        ),
    )
    default_end = dt.date.today()
    default_start = default_end - dt.timedelta(days=30)

    parser.add_argument(
        "--symbol",
        default=config_defaults.get("symbol", "sh600000"),
        help=(
            "目标标的（AkShare 格式，例如 sh600000、sz000001）。传入 all "
            "则批量处理全市场。"
        ),
    )
    parser.add_argument(
        "--frequency",
        default=config_defaults.get("frequency", "daily"),
        choices={"daily", "minute"},
        help="Data frequency to request.",
    )
    parser.add_argument(
        "--period",
        default=config_defaults.get("period", "1"),
        choices={"1", "5", "15", "30", "60"},
        help="Minute bar period when --frequency minute.",
    )
    parser.add_argument(
        "--start",
        default=config_defaults.get("start", default_start.strftime("%Y-%m-%d")),
        help=(
            "Inclusive start datetime. Accepts 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM'. "
            "Default: 30 calendar days ago at 00:00."
        ),
    )
    parser.add_argument(
        "--end",
        default=config_defaults.get("end", default_end.strftime("%Y-%m-%d")),
        help=(
            "Inclusive end datetime. Accepts 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM'. "
            "Default: today at 00:00."
        ),
    )
    parser.add_argument(
        "--adjust",
        default=config_defaults.get("adjust", "qfq"),
        choices={"", "qfq", "hfq"},
        help="Adjustment type for prices: '' (none), 'qfq', or 'hfq'.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=config_defaults.get("limit", 5),
        help="Number of latest rows to display.",
    )
    parser.add_argument(
        "--output-dir",
        default=config_defaults.get("output_dir", ""),
        help=(
            "若批量下载（--symbol all），建议指定一个目录保存各股票数据。"
            "未指定时，将在控制台上按股票输出截断数据。"
        ),
    )
    return parser.parse_args(remaining)


def parse_datetime(value: str) -> dt.datetime:
    formats = ("%Y-%m-%d %H:%M", "%Y-%m-%d")
    for fmt in formats:
        try:
            return dt.datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"{value!r} does not match YYYY-MM-DD or YYYY-MM-DD HH:MM.")


def parse_datetime_range(start: str, end: str) -> Tuple[dt.datetime, dt.datetime]:
    try:
        start_dt = parse_datetime(start)
        end_dt = parse_datetime(end)
    except ValueError as exc:
        raise SystemExit(f"Invalid datetime format: {exc}") from exc
    if start_dt > end_dt:
        raise SystemExit("Start must be on or before end.")
    return start_dt, end_dt


def fetch_daily_data(
    symbol: str, start_date: dt.date, end_date: dt.date, adjust: str
) -> pd.DataFrame:
    """Retrieve daily data for the specified A-share symbol using AkShare."""
    df = ak.stock_zh_a_daily(symbol=symbol, adjust=adjust)

    # AkShare may return the trading date either as an index or as a column.
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
    else:
        df.index = pd.to_datetime(df.index)

    df = df.sort_index()
    mask = (df.index.date >= start_date) & (df.index.date <= end_date)
    return df.loc[mask]


def fetch_minute_data(
    symbol: str,
    period: str,
    start_dt: dt.datetime,
    end_dt: dt.datetime,
    adjust: str,
) -> pd.DataFrame:
    """Retrieve minute-level data for the specified A-share symbol using AkShare."""
    df = ak.stock_zh_a_minute(symbol=symbol, period=period, adjust=adjust)

    datetime_series = None
    # Prefer built-in datetime index.
    if isinstance(df.index, pd.DatetimeIndex):
        datetime_series = pd.to_datetime(df.index)
    elif df.index.name in {"datetime", "date", "day", "timestamp", "时间", "日期"}:
        datetime_series = pd.to_datetime(df.index)
    elif pd.api.types.is_object_dtype(df.index):
        try:
            converted_index = pd.to_datetime(df.index)
            if not converted_index.isna().all():
                datetime_series = converted_index
        except Exception:
            pass

    # Try to coerce any known single-column datetime representations.
    candidate_columns = [
        "datetime",
        "date",
        "day",
        "timestamp",
        "time_stamp",
        "period",
        "时间",
        "日期",
        "交易时间",
        "交易日期",
    ]
    for col in candidate_columns:
        if datetime_series is not None:
            break
        if col not in df.columns:
            continue
        try:
            datetime_series = pd.to_datetime(df[col])
        except Exception:
            continue

    # Combine separate date/time columns if available, including raw time-only columns.
    if datetime_series is None:
        combined_candidates = [
            ("date", "time"),
            ("day", "time"),
            ("日期", "时间"),
            ("交易日期", "交易时间"),
        ]
        for first, second in combined_candidates:
            if {first, second}.issubset(df.columns):
                datetime_series = pd.to_datetime(
                    df[first].astype(str) + " " + df[second].astype(str)
                )
                break

    if datetime_series is None:
        for col in df.columns:
            try:
                candidate = pd.to_datetime(df[col], errors="coerce")
            except Exception:
                continue
            if candidate.notna().sum() >= len(df) * 0.5:
                datetime_series = candidate
                break

    if datetime_series is None:
        raise ValueError(
            "Unexpected response: could not locate a datetime field in the minute data."
        )

    df = (
        df.assign(datetime=datetime_series)
        .dropna(subset=["datetime"])
        .set_index("datetime")
        .sort_index()
    )

    mask = (df.index >= start_dt) & (df.index <= end_dt)
    return df.loc[mask]


def to_akshare_symbol(code: str) -> str:
    code = code.strip()
    if not code:
        raise ValueError("Empty stock code encountered.")
    if code.startswith(("6", "9")):
        return f"sh{code}"
    if code.startswith(("0", "2", "3")):
        return f"sz{code}"
    if code.startswith(("4", "8")):
        return f"bj{code}"
    raise ValueError(f"Unrecognized A-share code prefix: {code}")


def get_all_a_share_symbols() -> List[str]:
    """Fetch the entire A-share symbol list and convert to AkShare symbol format."""
    df = ak.stock_info_a_code_name()
    column_candidates = ["code", "代码"]
    code_column = next((col for col in column_candidates if col in df.columns), None)
    if code_column is None:
        raise ValueError("未能在 code list 中找到股票代码列。")

    codes = (
        df[code_column]
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )
    symbols: List[str] = []
    for code in codes:
        try:
            symbol = to_akshare_symbol(code)
        except ValueError:
            continue
        symbols.append(symbol)
    if not symbols:
        raise ValueError("未能解析出任何 A 股股票代码。")
    return symbols


def resolve_symbols(symbol: str) -> List[str]:
    if symbol.lower() != "all":
        return [symbol]
    symbols = get_all_a_share_symbols()
    symbols.sort()
    return symbols


def main() -> None:
    args = parse_args()
    start_dt, end_dt = parse_datetime_range(args.start, args.end)

    try:
        symbols = resolve_symbols(args.symbol)
    except Exception as exc:
        raise SystemExit(f"无法获取股票列表：{exc}") from exc

    results: dict[str, pd.DataFrame] = {}
    errors: list[tuple[str, str]] = []
    for idx, symbol in enumerate(symbols, start=1):
        try:
            if args.frequency == "daily":
                df_res = fetch_daily_data(symbol, start_dt.date(), end_dt.date(), args.adjust)
            else:
                df_res = fetch_minute_data(symbol, args.period, start_dt, end_dt, args.adjust)
            results[symbol] = df_res
        except Exception as exc:  # pragma: no cover - network responses vary
            errors.append((symbol, str(exc)))
            continue

        if idx % 100 == 0:
            print(f"[INFO] 已处理 {idx} 支股票…", flush=True)

    non_empty = {sym: df for sym, df in results.items() if not df.empty}
    if not non_empty:
        print("No data returned for the specified parameters.")
        if errors:
            print("Encountered errors:")
            for symbol, message in errors[:10]:
                print(f"  {symbol}: {message}")
            if len(errors) > 10:
                print(f"  ... 还有 {len(errors) - 10} 个错误未显示")
        sys.exit(0)

    output_dir = Path(args.output_dir).expanduser() if args.output_dir else None
    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)

    # pandas will truncate floats by default; widen the display for clarity.
    with pd.option_context("display.width", 120, "display.max_columns", None):
        for symbol, df in non_empty.items():
            if output_dir:
                filename = (
                    f"{symbol}_{args.frequency}_"
                    f"{args.period if args.frequency == 'minute' else '1d'}_"
                    f"{start_dt:%Y%m%d%H%M}_{end_dt:%Y%m%d%H%M}.parquet"
                )
                file_path = output_dir / filename
                try:
                    df.to_parquet(file_path)
                except ImportError as exc:
                    raise SystemExit(
                        "保存 Parquet 失败：请先安装 pyarrow 或 fastparquet。"
                    ) from exc
                except Exception as exc:  # pragma: no cover - propagate unexpected
                    raise SystemExit(f"保存 Parquet 失败：{exc}") from exc
                print(f"Saved {symbol} -> {file_path}")
                continue

            display_df = df.tail(args.limit)
            print(
                f"Symbol: {symbol}  Frequency: {args.frequency}  "
                f"Period: {args.period if args.frequency == 'minute' else '1d'}  "
                f"Adjust: {args.adjust or 'none'}"
            )
            print(f"Range: {start_dt} -> {end_dt}")
            print(display_df)
            print("-" * 60)

    if errors:
        print("Encountered errors during fetch:")
        for symbol, message in errors[:10]:
            print(f"  {symbol}: {message}")
        if len(errors) > 10:
            print(f"  ... 还有 {len(errors) - 10} 个错误未显示")


if __name__ == "__main__":
    main()
