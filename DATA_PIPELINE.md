# A-share Data Pipeline

This repository now includes `partitioned_ingest.py`, a CLI tool that downloads AkShare data
into industry-style partitioned Parquet files. Files are organized by frequency and
`partition_date`, compressed with Snappy, and tracked via a manifest for incremental updates.

## Dependencies

```bash
pip install akshare pandas duckdb pyarrow
```

## Daily data example

默认配置写在 `partitioned_config.json`，因此直接运行即可：

```bash
python partitioned_ingest.py
```

若使用其他配置文件，可通过 `--config custom_config.json` 指定。

Output structure:

```
data/
  daily/
    partition_date=2025-11-01/part-0000.parquet
    partition_date=2025-11-02/part-0000.parquet
```

Each Parquet file contains all symbols for that trading day with columns such as
`symbol`, `market`, `trade_date`, `open`, `high`, `low`, `close`, `volume`, `amount`,
derived metrics (`twap`, `vwap`, `adjust_factor`), and both adjusted/`*_raw` prices when available.

## Minute data example

```bash
python partitioned_ingest.py --config partitioned_config_minute.json
```

Minute partitions are written to `data/minute/period=5m/partition_date=YYYY-MM-DD/` and
contain one file per trading day with intraday bars for every symbol.

## Advanced usage

- `--symbols-file my_list.txt` to restrict the download universe.
- `--limit 100` to test on the first 100 symbols.
- `--manifest data/manifest.json` stores status (`done`, `empty`) per date so reruns can skip
  previously processed partitions when `--resume` is set.
- `--keep-temp` keeps the per-symbol intermediates under `tmp/ingest/<frequency>/` for debugging.
- Each partition manifest entry now includes row counts, covered date/time ranges, and symbol counts,
  plus an `errors` list recording fetch failures (e.g., API hiccups) so you can requeue bad symbols.
- Temporary per-date symbol files are written under `tmp/ingest/<frequency>/` during the run and
  deleted automatically after partitions are materialized unless `--keep-temp` is set.

### Configuration file fields

`partitioned_config.json` mirrors the CLI flags:

| key | meaning |
| --- | --- |
| `frequency` | `daily` or `minute` |
| `period` | minute interval when `frequency=minute` |
| `adjust` | AkShare adjust parameter (`qfq`, `hfq`, `""`) |
| `start` / `end` | inclusive date range |
| `symbols_file` | optional symbol list, empty for all A shares |
| `output_root` / `temp_root` | directories for final partitions and temp chunks |
| `manifest` | manifest JSON path |
| `limit` | cap number of symbols per run (0 = all) |
| `resume` | skip already processed dates |
| `sleep` | delay between symbol requests |
| `max_workers` | thread pool size for concurrent downloads |
| `keep_temp` | retain temporary symbol files |

The resulting Parquet tree can be queried efficiently via DuckDB, Spark, Trino, or any engine
that understands Hive-style partitions.
