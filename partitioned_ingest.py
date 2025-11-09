#!/usr/bin/env python3
"""Partitioned A-share data downloader."""

from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List

import akshare as ak
import duckdb
import pandas as pd

MAX_FILES_PER_MERGE = 200

DEFAULT_CONFIG_PATH = "partitioned_config.json"


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
    base_parser = argparse.ArgumentParser(add_help=False)
    base_parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help="配置文件路径（JSON），用于覆盖默认参数。",
    )
    early_args, remaining = base_parser.parse_known_args()
    config_defaults = load_config(Path(early_args.config))

    parser = argparse.ArgumentParser(
        parents=[base_parser],
        description="Download A-share daily or minute data into partitioned Parquet files.",
    )
    parser.add_argument(
        "--frequency",
        choices=("daily", "minute"),
        default=config_defaults.get("frequency"),
    )
    parser.add_argument(
        "--period",
        default=config_defaults.get("period", "5"),
        help="Minute period (1/5/15/30/60) when --frequency=minute.",
    )
    parser.add_argument(
        "--adjust",
        default=config_defaults.get("adjust", "qfq"),
        help="AkShare adjust parameter (e.g. qfq/hfq/'').",
    )
    parser.add_argument(
        "--start",
        default=config_defaults.get("start"),
        help="Start date YYYY-MM-DD.",
    )
    parser.add_argument(
        "--end",
        default=config_defaults.get("end"),
        help="End date YYYY-MM-DD.",
    )
    parser.add_argument(
        "--symbols-file",
        default=config_defaults.get("symbols_file", ""),
        help="Optional newline-delimited list of symbols (sh600000).",
    )
    parser.add_argument(
        "--output-root",
        default=config_defaults.get("output_root", "data"),
        help="Root folder for final partitions.",
    )
    parser.add_argument(
        "--temp-root",
        default=config_defaults.get("temp_root", "tmp/ingest"),
        help="Temporary folder for per-date chunks.",
    )
    parser.add_argument(
        "--manifest",
        default=config_defaults.get("manifest", "data/manifest.json"),
        help="Manifest JSON file path.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=int(config_defaults.get("limit", 0)),
        help="Process first N symbols only (debug).",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        default=bool(config_defaults.get("resume", False)),
        help="Skip dates already marked done in manifest.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=float(config_defaults.get("sleep", 0.0)),
        help="Seconds to sleep between symbol requests.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=int(config_defaults.get("max_workers", 4)),
        help="Number of concurrent download workers.",
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        default=bool(config_defaults.get("keep_temp", False)),
        help="Retain temporary symbol files for debugging.",
    )
    parser.add_argument(
        "--merge-workers",
        type=int,
        default=int(config_defaults.get("merge_workers", 2)),
        help="Number of threads to use when merging partitions.",
    )
    args = parser.parse_args(remaining)
    missing = [field for field in ("frequency", "start", "end") if not getattr(args, field)]
    if missing:
        parser.error(
            "Missing required parameter(s): "
            + ", ".join(missing)
            + ". Provide them via CLI flags or config file."
        )
    return args


def manifest_key(freq: str, period: str) -> str:
    return freq if freq == "daily" else f"minute:{period}"


def load_manifest(path: Path) -> Dict[str, Dict[str, dict]]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return {}


def save_manifest(path: Path, manifest: Dict[str, Dict[str, dict]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)


def normalize_symbol(code: str) -> str | None:
    code = code.strip()
    if not code:
        return None
    if code.startswith(("sh", "sz", "bj")):
        return code.lower()
    if code[0] in "69":
        return f"sh{code}"
    if code[0] in "023":
        return f"sz{code}"
    if code[0] in "48":
        return f"bj{code}"
    return None


def load_symbols(path: str | None) -> List[str]:
    if path:
        raw = Path(path).expanduser().read_text(encoding="utf-8").splitlines()
        symbols = [normalize_symbol(line) for line in raw]
        return sorted({sym for sym in symbols if sym})
    df = ak.stock_info_a_code_name()
    column = "code" if "code" in df.columns else "代码"
    symbols = [normalize_symbol(code) for code in df[column].astype(str)]
    return sorted({sym for sym in symbols if sym})


def fetch_daily(symbol: str, adjust: str) -> pd.DataFrame:
    df = ak.stock_zh_a_daily(symbol=symbol, adjust=adjust)
    if df.empty:
        return df
    if "date" in df.columns:
        df.index = pd.to_datetime(df["date"])
        df = df.drop(columns=["date"])
    else:
        df.index = pd.to_datetime(df.index)
    return df.sort_index()


def fetch_minute(symbol: str, period: str, adjust: str) -> pd.DataFrame:
    df = ak.stock_zh_a_minute(symbol=symbol, period=period, adjust=adjust)
    if df.empty:
        return df
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"])
    elif "date" in df.columns:
        df["datetime"] = pd.to_datetime(df["date"])
    else:
        df["datetime"] = pd.to_datetime(df.iloc[:, 0])
    return df.set_index("datetime").sort_index()


def dump_temp_chunks(
    df: pd.DataFrame,
    symbol: str,
    temp_root: Path,
    date_col: str,
) -> None:
    df["partition_date"] = df[date_col].dt.strftime("%Y-%m-%d")
    for date_str, chunk in df.groupby("partition_date"):
        date_dir = temp_root / date_str
        date_dir.mkdir(parents=True, exist_ok=True)
        chunk.to_parquet(date_dir / f"{symbol}.parquet", index=False)


def process_daily(
    symbols: List[str],
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    adjust: str,
    temp_root: Path,
    limit: int,
    sleep: float,
    max_workers: int,
    errors: List[dict],
) -> None:
    temp_root.mkdir(parents=True, exist_ok=True)
    target_symbols = symbols if limit <= 0 else symbols[:limit]
    if not target_symbols:
        return

    def worker(symbol: str) -> dict:
        log: Dict[str, object] = {"symbol": symbol}
        try:
            df = fetch_daily(symbol, adjust)
        except Exception as exc:  # pragma: no cover
            log.update(stage="fetch_daily", error=str(exc))
            return log
        if df.empty:
            log["status"] = "empty"
            return log
        df = df.loc[(df.index >= start_ts) & (df.index <= end_ts)]
        if df.empty:
            log["status"] = "no_range"
            return log

        if adjust:
            try:
                raw_df = fetch_daily(symbol, "")
                raw_df = raw_df.loc[(raw_df.index >= start_ts) & (raw_df.index <= end_ts)]
                df = df.join(raw_df.add_suffix("_raw"), how="left")
            except Exception as exc:  # pragma: no cover
                log.update(stage="fetch_daily_raw", error=str(exc))
                return log

        if {"open", "high", "low", "close"}.issubset(df.columns):
            df["twap"] = df[["open", "high", "low", "close"]].mean(axis=1)
        if {"amount", "volume"}.issubset(df.columns):
            vol = df["volume"].replace(0, pd.NA)
            df["vwap"] = df["amount"] / vol
        if adjust and "close_raw" in df.columns:
            raw_close = df["close_raw"].replace(0, pd.NA)
            df["adjust_factor"] = df["close"] / raw_close
        else:
            df["adjust_factor"] = 1.0

        df = df.reset_index()
        first_col = df.columns[0]
        df = df.rename(columns={first_col: "trade_date"})
        df.insert(0, "symbol", symbol)
        df.insert(1, "market", symbol[:2])
        df["adjust_type"] = adjust or "none"
        dump_temp_chunks(df, symbol, temp_root, "trade_date")
        if sleep:
            time.sleep(sleep)
        log["status"] = "ok"
        return log

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


def process_minute(
    symbols: List[str],
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    period: str,
    adjust: str,
    temp_root: Path,
    limit: int,
    sleep: float,
    max_workers: int,
    errors: List[dict],
) -> None:
    temp_root.mkdir(parents=True, exist_ok=True)
    target_symbols = symbols if limit <= 0 else symbols[:limit]
    if not target_symbols:
        return

    def worker(symbol: str) -> dict:
        log: Dict[str, object] = {"symbol": symbol}
        try:
            df = fetch_minute(symbol, period, adjust)
        except Exception as exc:  # pragma: no cover
            log.update(stage="fetch_minute", error=str(exc))
            return log
        if df.empty:
            log["status"] = "empty"
            return log
        df = df.loc[(df.index >= start_ts) & (df.index <= end_ts)]
        if df.empty:
            log["status"] = "no_range"
            return log

        if {"open", "high", "low", "close"}.issubset(df.columns):
            df["twap"] = df[["open", "high", "low", "close"]].mean(axis=1)
        if {"amount", "volume"}.issubset(df.columns):
            vol = df["volume"].replace(0, pd.NA)
            df["vwap"] = df["amount"] / vol

        df = df.reset_index()
        first_col = df.columns[0]
        df = df.rename(columns={first_col: "trade_time"})
        df["trade_date"] = df["trade_time"].dt.normalize()
        df.insert(0, "symbol", symbol)
        df.insert(1, "market", symbol[:2])
        df["period"] = period
        df["adjust_type"] = adjust or "none"
        dump_temp_chunks(df, symbol, temp_root, "trade_time")
        if sleep:
            time.sleep(sleep)
        log["status"] = "ok"
        return log

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


def write_partition(temp_dir: Path, output_dir: Path, freq: str, period: str, keep_temp: bool) -> Dict[str, object]:
    files = sorted(temp_dir.glob("*.parquet"))
    if not files:
        if not keep_temp and temp_dir.exists():
            temp_dir.rmdir()
        return {"rows": 0, "status": "empty"}

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "part-0000.parquet"

    working_files = files[:]
    generated_files: List[Path] = []
    batch_idx = 0

    while len(working_files) > MAX_FILES_PER_MERGE:
        new_files: List[Path] = []
        for i in range(0, len(working_files), MAX_FILES_PER_MERGE):
            chunk = working_files[i : i + MAX_FILES_PER_MERGE]
            batch_path = temp_dir / f"_merge_chunk_{batch_idx}.parquet"
            batch_idx += 1
            _merge_parquet_files(chunk, batch_path)
            new_files.append(batch_path)
            generated_files.append(batch_path)
        working_files = new_files

    _merge_parquet_files(working_files, output_path)

    con = duckdb.connect()
    if freq == "daily":
        rows, min_date, max_date, symbols = con.execute(
            """
            SELECT
                count(*) AS rows,
                min(trade_date) AS min_trade_date,
                max(trade_date) AS max_trade_date,
                count(DISTINCT symbol) AS symbols
            FROM read_parquet(?);
            """,
            [str(output_path)],
        ).fetchone()
        summary: Dict[str, object] = {
            "rows": int(rows or 0),
            "path": str(output_path),
            "min_trade_date": None if min_date is None else str(min_date),
            "max_trade_date": None if max_date is None else str(max_date),
            "symbols": int(symbols or 0),
        }
    else:
        rows, min_time, max_time, min_date, max_date, symbols = con.execute(
            """
            SELECT
                count(*) AS rows,
                min(trade_time) AS min_trade_time,
                max(trade_time) AS max_trade_time,
                min(trade_date) AS min_trade_date,
                max(trade_date) AS max_trade_date,
                count(DISTINCT symbol) AS symbols
            FROM read_parquet(?);
            """,
            [str(output_path)],
        ).fetchone()
        summary = {
            "rows": int(rows or 0),
            "path": str(output_path),
            "min_trade_time": None if min_time is None else str(min_time),
            "max_trade_time": None if max_time is None else str(max_time),
            "min_trade_date": None if min_date is None else str(min_date),
            "max_trade_date": None if max_date is None else str(max_date),
            "symbols": int(symbols or 0),
            "period": period,
        }
    con.close()

    if not keep_temp:
        for file in files + generated_files:
            file.unlink(missing_ok=True)
        temp_dir.rmdir()
    return summary


def _merge_parquet_files(input_files: List[Path], output_file: Path) -> None:
    if not input_files:
        raise ValueError("No input files provided for merge.")
    file_list = ", ".join("'" + str(f).replace("'", "''") + "'" for f in input_files)
    source = f"[{file_list}]"
    output_parent = output_file.parent
    output_parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(
        f"""
        COPY (SELECT * FROM read_parquet({source}))
        TO '{str(output_file).replace("'", "''")}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
        """
    )
    con.close()


def finalize_partitions(
    dates: List[pd.Timestamp],
    freq: str,
    period: str,
    output_root: Path,
    temp_root: Path,
    manifest: Dict[str, Dict[str, dict]],
    manifest_path: Path,
    resume: bool,
    keep_temp: bool,
    merge_workers: int,
) -> None:
    key = manifest_key(freq, period)
    section = manifest.setdefault(key, {})
    tasks = []
    for date in dates:
        date_str = date.strftime("%Y-%m-%d")
        if resume and section.get(date_str, {}).get("status") == "done":
            continue
        temp_dir = temp_root / date_str
        if not temp_dir.exists():
            section[date_str] = {"status": "empty"}
            continue
        if freq == "daily":
            partition_dir = output_root / "daily" / f"partition_date={date_str}"
        else:
            partition_dir = output_root / "minute" / f"period={period}m" / f"partition_date={date_str}"
        tasks.append((date_str, temp_dir, partition_dir))

    if not tasks:
        save_manifest(manifest_path, manifest)
        return

    def worker(date_str: str, temp_dir: Path, partition_dir: Path) -> Tuple[str, Dict[str, object]]:
        summary = write_partition(temp_dir, partition_dir, freq, period, keep_temp=keep_temp)
        summary.setdefault("status", "done")
        return date_str, summary

    with ThreadPoolExecutor(max_workers=max(1, merge_workers)) as executor:
        futures = {executor.submit(worker, date_str, temp_dir, partition_dir): date_str for date_str, temp_dir, partition_dir in tasks}
        for future in as_completed(futures):
            date_str, summary = future.result()
            section[date_str] = summary
            save_manifest(manifest_path, manifest)


def main() -> None:
    args = parse_args()
    start_ts = pd.Timestamp(args.start)
    end_ts = pd.Timestamp(args.end)
    if start_ts > end_ts:
        raise SystemExit("start date must be <= end date")

    dates = [pd.Timestamp(d) for d in pd.date_range(start_ts, end_ts, freq="D")]
    symbols = load_symbols(args.symbols_file or None)
    if not symbols:
        raise SystemExit("No symbols to process.")

    manifest_path = Path(args.manifest).expanduser()
    manifest = load_manifest(manifest_path)
    output_root = Path(args.output_root).expanduser()
    temp_root = (Path(args.temp_root).expanduser() / manifest_key(args.frequency, args.period))
    temp_root.mkdir(parents=True, exist_ok=True)
    error_log: List[dict] = []
    if args.frequency == "daily":
        process_daily(
            symbols,
            start_ts,
            end_ts,
            args.adjust,
            temp_root,
            args.limit,
            args.sleep,
            args.max_workers,
            error_log,
        )
    else:
        process_minute(
            symbols,
            start_ts,
            end_ts,
            args.period,
            args.adjust,
            temp_root,
            args.limit,
            args.sleep,
            args.max_workers,
            error_log,
        )

    finalize_partitions(
        dates,
        args.frequency,
        args.period,
        output_root,
        temp_root,
        manifest,
        manifest_path,
        args.resume,
        args.keep_temp,
        args.merge_workers,
    )
    if error_log:
        manifest.setdefault("errors", []).extend(error_log)
        save_manifest(manifest_path, manifest)


if __name__ == "__main__":
    main()
