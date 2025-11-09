#!/usr/bin/env python3
"""Eastmoney-based minute data downloader (independent from AkShare pipeline)."""

from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Tuple

import akshare as ak
import duckdb
import pandas as pd

DEFAULT_CONFIG = "eastmoney_config.json"
SUPPORTED_PERIODS = {"1", "5", "15", "30", "60"}
SUPPORTED_ADJUST = {"", "qfq", "hfq"}


def load_config(path: Path) -> Dict[str, object]:
    path = path.expanduser()
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as file:
            return json.load(file)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"配置文件解析失败：{path}: {exc}") from exc


def parse_args() -> argparse.Namespace:
    base = argparse.ArgumentParser(add_help=False)
    base.add_argument("--config", default=DEFAULT_CONFIG, help="配置文件路径（JSON）。")
    early, remaining = base.parse_known_args()
    defaults = load_config(Path(early.config))

    parser = argparse.ArgumentParser(
        parents=[base],
        description="下载东方财富分钟级数据并按日期分区保存为 Parquet。",
    )
    parser.add_argument("--start", default=defaults.get("start"), help="起始时间，格式 YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--end", default=defaults.get("end"), help="结束时间，格式 YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--period", default=defaults.get("period", "1"), help="分钟周期：1/5/15/30/60")
    parser.add_argument("--adjust", default=defaults.get("adjust", ""), help="复权方式：'', qfq, hfq")
    parser.add_argument("--symbols-file", default=defaults.get("symbols_file", ""), help="股票列表文件（可选）")
    parser.add_argument("--output-root", default=defaults.get("output_root", "data_em_minute"))
    parser.add_argument("--temp-root", default=defaults.get("temp_root", "tmp/ingest_em_minute"))
    parser.add_argument("--manifest", default=defaults.get("manifest", "data/em_minute_manifest.json"))
    parser.add_argument("--limit", type=int, default=int(defaults.get("limit", 0)))
    parser.add_argument("--resume", action="store_true", default=bool(defaults.get("resume", True)))
    parser.add_argument("--sleep", type=float, default=float(defaults.get("sleep", 0.0)))
    parser.add_argument("--max-workers", type=int, default=int(defaults.get("max_workers", 4)))
    parser.add_argument("--keep-temp", action="store_true", default=bool(defaults.get("keep_temp", False)))
    args = parser.parse_args(remaining)

    missing = [field for field in ("start", "end") if not getattr(args, field)]
    if missing:
        parser.error("缺少必要参数：" + ", ".join(missing))
    if args.period not in SUPPORTED_PERIODS:
        parser.error(f"period 仅支持 {', '.join(sorted(SUPPORTED_PERIODS))}")
    if args.adjust not in SUPPORTED_ADJUST:
        parser.error("adjust 仅支持 '', qfq, hfq")
    return args


def load_symbols(path: str | None) -> List[str]:
    if path:
        file_path = Path(path).expanduser()
        if not file_path.exists():
            raise SystemExit(f"symbols 文件不存在：{file_path}")
        raw = [line.strip() for line in file_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        return [normalize_symbol(line)[0] for line in raw]

    df = ak.stock_info_a_code_name()
    column = "code" if "code" in df.columns else "代码"
    codes = [normalize_symbol(code)[0] for code in df[column].astype(str)]
    return sorted({code for code in codes if code})


def normalize_symbol(symbol: str) -> Tuple[str, str]:
    """
    Returns (standard_symbol_with_prefix, eastmoney_code_without_prefix)
    """
    code = symbol.strip().lower()
    if not code:
        return "", ""
    if code.startswith(("sh", "sz", "bj")):
        prefix, digits = code[:2], code[2:]
    else:
        digits = code
        if digits.startswith(("6", "9")):
            prefix = "sh"
        elif digits.startswith(("0", "2", "3")):
            prefix = "sz"
        elif digits.startswith(("4", "8")):
            prefix = "bj"
        else:
            return "", ""
    standard = f"{prefix}{digits}"
    em_code = digits
    return standard, em_code


def fetch_minute_em(symbol: str, period: str, start: str, end: str, adjust: str) -> pd.DataFrame:
    df = ak.stock_zh_a_hist_min_em(symbol=symbol, period=period, start_date=start, end_date=end, adjust=adjust)
    if df is None or df.empty:
        return pd.DataFrame()
    rename_map = {
        "时间": "datetime",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "amount",
        "振幅": "amplitude",
        "涨跌幅": "pct_change",
        "涨跌额": "change",
        "换手率": "turnover",
    }
    df = df.rename(columns=rename_map)
    df["datetime"] = pd.to_datetime(df["datetime"])
    numeric_cols = ["open", "close", "high", "low", "volume", "amount", "amplitude", "pct_change", "change", "turnover"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.sort_values("datetime").drop_duplicates(subset=["datetime"])


def dump_temp_chunks(df: pd.DataFrame, symbol: str, temp_root: Path) -> None:
    df["partition_date"] = df["datetime"].dt.strftime("%Y-%m-%d")
    for date_str, chunk in df.groupby("partition_date"):
        date_dir = temp_root / date_str
        date_dir.mkdir(parents=True, exist_ok=True)
        chunk.drop(columns="partition_date").to_parquet(date_dir / f"{symbol}.parquet", index=False)


def process_symbols(
    symbols: List[str],
    start: str,
    end: str,
    period: str,
    adjust: str,
    temp_root: Path,
    limit: int,
    sleep: float,
    max_workers: int,
    errors: List[dict],
) -> None:
    target_symbols = symbols if limit <= 0 else symbols[:limit]
    if not target_symbols:
        return

    def worker(std_symbol: str) -> dict:
        log: Dict[str, object] = {"symbol": std_symbol}
        display, em_code = normalize_symbol(std_symbol)
        if not display:
            log.update(stage="normalize", error="invalid symbol")
            return log
        try:
            df = fetch_minute_em(em_code, period, start, end, adjust)
        except Exception as exc:  # pragma: no cover
            log.update(stage="fetch_minute_em", error=str(exc))
            return log
        if df.empty:
            log["status"] = "empty"
            return log
        df = df[(df["datetime"] >= pd.Timestamp(start)) & (df["datetime"] <= pd.Timestamp(end))]
        if df.empty:
            log["status"] = "no_range"
            return log

        df.insert(0, "symbol", display)
        df.insert(1, "market", display[:2])
        df["period"] = period
        df["adjust_type"] = adjust or "none"
        df["code_em"] = em_code
        if {"open", "high", "low", "close"}.issubset(df.columns):
            df["twap"] = df[["open", "high", "low", "close"]].mean(axis=1)
        if {"amount", "volume"}.issubset(df.columns):
            vol = df["volume"].replace(0, pd.NA)
            df["vwap"] = df["amount"] / vol
        dump_temp_chunks(df, display.replace(".", "_"), temp_root)
        if sleep:
            time.sleep(sleep)
        log["status"] = "ok"
        return log

    temp_root.mkdir(parents=True, exist_ok=True)
    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
        futures = {executor.submit(worker, symbol): symbol for symbol in target_symbols}
        for future in as_completed(futures):
            result = future.result()
            if result.get("status") not in {"ok", "empty", "no_range"}:
                errors.append(
                    {
                        "symbol": result.get("symbol"),
                        "stage": result.get("stage", "unknown"),
                        "error": result.get("error", "unknown"),
                        "time": pd.Timestamp.utcnow().isoformat(),
                    }
                )


def write_partition(temp_dir: Path, output_dir: Path, keep_temp: bool) -> Dict[str, object]:
    files = list(temp_dir.glob("*.parquet"))
    if not files:
        if not keep_temp and temp_dir.exists():
            temp_dir.rmdir()
        return {"rows": 0, "status": "empty"}
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "part-0000.parquet"
    pattern = str(temp_dir / "*.parquet")
    con = duckdb.connect()
    row_count = con.execute(f"SELECT count(*) FROM read_parquet('{pattern}')").fetchone()[0]
    summary_row = con.execute(
        f"""
        SELECT
            min(datetime) AS min_datetime,
            max(datetime) AS max_datetime,
            min(date_trunc('day', datetime)) AS min_date,
            max(date_trunc('day', datetime)) AS max_date,
            count(DISTINCT symbol) AS symbols
        FROM read_parquet('{pattern}')
        """
    ).fetchone()
    con.execute(
        f"""
        COPY (SELECT * FROM read_parquet('{pattern}'))
        TO '{output_path}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
        """
    )
    con.close()
    if not keep_temp:
        for file in files:
            file.unlink(missing_ok=True)
        temp_dir.rmdir()
    return {
        "rows": int(row_count),
        "path": str(output_path),
        "min_datetime": None if summary_row[0] is None else str(summary_row[0]),
        "max_datetime": None if summary_row[1] is None else str(summary_row[1]),
        "symbols": int(summary_row[4] or 0),
    }


def finalize_partitions(
    dates: List[pd.Timestamp],
    output_root: Path,
    temp_root: Path,
    manifest: Dict[str, Dict[str, dict]],
    manifest_path: Path,
    resume: bool,
    keep_temp: bool,
) -> None:
    section = manifest.setdefault("em_minute", {})
    for date in dates:
        date_str = date.strftime("%Y-%m-%d")
        if resume and section.get(date_str, {}).get("status") == "done":
            continue
        temp_dir = temp_root / date_str
        if not temp_dir.exists():
            section[date_str] = {"status": "empty"}
            save_manifest(manifest_path, manifest)
            continue
        partition_dir = output_root / f"period/partition_date={date_str}"
        summary = write_partition(temp_dir, partition_dir, keep_temp=keep_temp)
        summary.setdefault("status", "done")
        section[date_str] = summary
        save_manifest(manifest_path, manifest)


def save_manifest(path: Path, manifest: Dict[str, Dict[str, dict]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(manifest, file, ensure_ascii=False, indent=2)


def main() -> None:
    args = parse_args()
    symbols = load_symbols(args.symbols_file or None)
    if not symbols:
        raise SystemExit("无法解析任何股票代码。")

    start_ts = pd.to_datetime(args.start)
    end_ts = pd.to_datetime(args.end)
    if start_ts > end_ts:
        raise SystemExit("start 不能晚于 end。")
    dates = [pd.Timestamp(d) for d in pd.date_range(start_ts.normalize(), end_ts.normalize(), freq="D")]

    temp_root = Path(args.temp_root).expanduser()
    output_root = Path(args.output_root).expanduser()
    manifest_path = Path(args.manifest).expanduser()
    manifest = load_config(manifest_path)
    errors: List[dict] = []

    process_symbols(
        symbols,
        args.start,
        args.end,
        args.period,
        args.adjust,
        temp_root,
        args.limit,
        args.sleep,
        args.max_workers,
        errors,
    )

    finalize_partitions(
        dates,
        output_root,
        temp_root,
        manifest,
        manifest_path,
        args.resume,
        args.keep_temp,
    )
    if errors:
        manifest.setdefault("errors", []).extend(errors)
        save_manifest(manifest_path, manifest)


if __name__ == "__main__":
    main()

