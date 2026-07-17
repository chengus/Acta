#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "duckdb>=1.4",
#   "numpy>=2.0",
#   "pyarrow>=18.0",
#   "zstandard>=0.23",
# ]
# ///
"""Benchmark Acta's proposed streams against Parquet on TPC-H SF1 lineitem."""

from __future__ import annotations

import argparse
import gc
import importlib.util
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Callable

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd


SCRIPT_DIR = Path(__file__).resolve().parent
CORE_PATH = SCRIPT_DIR.parent / "0_taxi_data" / "encoding_study.py"
CORE_SPEC = importlib.util.spec_from_file_location("acta_encoding_probe", CORE_PATH)
assert CORE_SPEC and CORE_SPEC.loader
encoding_probe = importlib.util.module_from_spec(CORE_SPEC)
sys.modules[CORE_SPEC.name] = encoding_probe
CORE_SPEC.loader.exec_module(encoding_probe)

BLOCK_ROWS = 65_536
COMPATIBLE_SELECT = """
SELECT
    l_orderkey,
    l_partkey,
    l_suppkey,
    l_linenumber,
    CAST(l_quantity * 100 AS BIGINT) AS l_quantity,
    CAST(l_extendedprice * 100 AS BIGINT) AS l_extendedprice,
    CAST(l_discount * 100 AS BIGINT) AS l_discount,
    CAST(l_tax * 100 AS BIGINT) AS l_tax,
    l_returnflag,
    l_linestatus,
    CAST(l_shipdate AS TIMESTAMP) AS l_shipdate,
    CAST(l_commitdate AS TIMESTAMP) AS l_commitdate,
    CAST(l_receiptdate AS TIMESTAMP) AS l_receiptdate,
    l_shipinstruct,
    l_shipmode,
    l_comment
FROM lineitem
"""


def median_seconds(
    call: Callable[[], object], repeats: int = 5
) -> tuple[float, object]:
    call()
    samples: list[float] = []
    value: object = None
    for _ in range(repeats):
        gc.collect()
        started = time.perf_counter()
        value = call()
        samples.append(time.perf_counter() - started)
    return statistics.median(samples), value


def sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def prepare_parquet(
    database: Path, unsorted_path: Path, sorted_path: Path
) -> dict[str, float]:
    connection = duckdb.connect(str(database), read_only=True)
    started = time.perf_counter()
    connection.execute(
        f"COPY ({COMPATIBLE_SELECT}) TO '{sql_path(unsorted_path)}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 1, "
        f"ROW_GROUP_SIZE {BLOCK_ROWS})"
    )
    unsorted_seconds = time.perf_counter() - started
    connection.close()

    transient = duckdb.connect()
    started = time.perf_counter()
    transient.execute(
        f"COPY (SELECT * FROM read_parquet('{sql_path(unsorted_path)}') "
        "ORDER BY l_shipdate, l_orderkey, l_linenumber) "
        f"TO '{sql_path(sorted_path)}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 1, "
        f"ROW_GROUP_SIZE {BLOCK_ROWS})"
    )
    sorted_seconds = time.perf_counter() - started
    transient.close()
    return {
        "unsorted_write_seconds": unsorted_seconds,
        "sorted_write_seconds": sorted_seconds,
    }


def canonical_raw_bytes(database: Path) -> int:
    connection = duckdb.connect(str(database), read_only=True)
    # Eleven fixed-width values consume 88 bytes per row. Five variable-width
    # strings use one uint32 length each plus their UTF-8 bytes.
    value = connection.execute(
        """
        SELECT CAST(
            count(*) * 108
            + sum(length(l_returnflag))
            + sum(length(l_linestatus))
            + sum(length(l_shipinstruct))
            + sum(length(l_shipmode))
            + sum(length(l_comment))
            AS UBIGINT
        )
        FROM lineitem
        """
    ).fetchone()[0]
    connection.close()
    return int(value)


def sample_table(
    parquet: pq.ParquetFile, sample_blocks: int
) -> tuple[pa.Table, list[int]]:
    indices = [
        int(index)
        for index in np.linspace(0, parquet.num_row_groups - 1, sample_blocks)
    ]
    tables = [parquet.read_row_group(index) for index in indices]
    return pa.concat_tables(tables).combine_chunks(), indices


def byte_values(column: pa.ChunkedArray) -> list[bytes]:
    return [value.encode() for value in column.to_pylist()]


def column_cases(table: pa.Table) -> list[object]:
    cases: list[object] = []
    integer_names = ["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber"]
    decimal_names = ["l_quantity", "l_extendedprice", "l_discount", "l_tax"]
    timestamp_names = ["l_shipdate", "l_commitdate", "l_receiptdate"]
    categorical_names = [
        "l_returnflag",
        "l_linestatus",
        "l_shipinstruct",
        "l_shipmode",
    ]
    for name in integer_names:
        cases.append(
            encoding_probe.ColumnCase(
                name,
                "int64",
                table[name].to_numpy(zero_copy_only=False).astype(np.int64),
            )
        )
    for name in decimal_names:
        cases.append(
            encoding_probe.ColumnCase(
                name,
                "decimal64[scale=2]",
                table[name].to_numpy(zero_copy_only=False).astype(np.int64),
            )
        )
    for name in categorical_names:
        cases.append(
            encoding_probe.ColumnCase(name, "categorical", byte_values(table[name]))
        )
    for name in timestamp_names:
        values = (
            table[name]
            .to_numpy(zero_copy_only=False)
            .astype("datetime64[us]")
            .astype(np.int64)
        )
        cases.append(encoding_probe.ColumnCase(name, "timestamp64[us]", values))
    cases.append(
        encoding_probe.ColumnCase("l_comment", "utf8", byte_values(table["l_comment"]))
    )
    return cases


def stream_benchmark(table: pa.Table, sample_blocks: int) -> list[dict[str, object]]:
    compressor = zstd.ZstdCompressor(level=1)
    decompressor = zstd.ZstdDecompressor()
    return [
        encoding_probe.benchmark_case(
            case, BLOCK_ROWS, sample_blocks, compressor, decompressor
        )
        for case in column_cases(table)
    ]


def write_sample_parquet(table: pa.Table, path: Path, compression: str | None) -> float:
    started = time.perf_counter()
    pq.write_table(
        table,
        path,
        row_group_size=BLOCK_ROWS,
        compression=compression,
        compression_level=1 if compression == "zstd" else None,
        use_dictionary=True,
        data_page_version="2.0",
        write_statistics=True,
    )
    return time.perf_counter() - started


def selected_row_groups(
    path: Path, lower_us: int, upper_us: int
) -> tuple[list[int], int]:
    parquet = pq.ParquetFile(path)
    time_index = parquet.schema_arrow.names.index("l_shipdate")
    projection_indices = [
        parquet.schema_arrow.names.index(name)
        for name in ("l_shipdate", "l_extendedprice", "l_discount")
    ]
    groups: list[int] = []
    compressed_bytes = 0
    lower = np.datetime64(lower_us, "us").astype(object)
    upper = np.datetime64(upper_us, "us").astype(object)
    for group_index in range(parquet.num_row_groups):
        group = parquet.metadata.row_group(group_index)
        statistics = group.column(time_index).statistics
        if statistics is None or (statistics.max >= lower and statistics.min < upper):
            groups.append(group_index)
            compressed_bytes += sum(
                group.column(column_index).total_compressed_size
                for column_index in projection_indices
            )
    return groups, compressed_bytes


def parquet_search(
    path: Path, lower_us: int, upper_us: int
) -> tuple[float, tuple[int, int, float]]:
    connection = duckdb.connect()
    lower = str(np.datetime64(lower_us, "us"))
    upper = str(np.datetime64(upper_us, "us"))
    query = (
        "SELECT count(*), sum(l_extendedprice), avg(l_discount) "
        f"FROM read_parquet('{sql_path(path)}') "
        f"WHERE l_shipdate >= TIMESTAMP '{lower}' "
        f"AND l_shipdate < TIMESTAMP '{upper}'"
    )
    seconds, result = median_seconds(
        lambda: connection.execute(query).fetchone(), repeats=5
    )
    connection.close()
    assert isinstance(result, tuple)
    return seconds, result


def raw_search_baselines(
    unsorted_path: Path, sorted_path: Path, work_dir: Path, lower_us: int, upper_us: int
) -> dict[str, object]:
    columns = ["l_shipdate", "l_extendedprice", "l_discount"]
    unsorted = pq.read_table(unsorted_path, columns=columns).combine_chunks()
    sorted_table = pq.read_table(sorted_path, columns=columns).combine_chunks()

    def arrays(table: pa.Table) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        times = (
            table["l_shipdate"]
            .to_numpy(zero_copy_only=False)
            .astype("datetime64[us]")
            .astype(np.int64)
        )
        prices = (
            table["l_extendedprice"].to_numpy(zero_copy_only=False).astype(np.int64)
        )
        discounts = table["l_discount"].to_numpy(zero_copy_only=False).astype(np.int64)
        return times, prices, discounts

    unsorted_arrays = arrays(unsorted)
    sorted_arrays = arrays(sorted_table)
    raw_paths: list[Path] = []
    for prefix, values in (("unsorted", unsorted_arrays), ("sorted", sorted_arrays)):
        for suffix, array in zip(("time", "price", "discount"), values):
            path = work_dir / f"raw_{prefix}_{suffix}.i64"
            array.tofile(path)
            raw_paths.append(path)

    def map_arrays(prefix: str) -> tuple[np.memmap, np.memmap, np.memmap]:
        return tuple(
            np.memmap(work_dir / f"raw_{prefix}_{suffix}.i64", dtype=np.int64, mode="r")
            for suffix in ("time", "price", "discount")
        )  # type: ignore[return-value]

    raw_times, raw_prices, raw_discounts = map_arrays("unsorted")
    sorted_times, sorted_prices, sorted_discounts = map_arrays("sorted")

    def full_scan() -> tuple[int, int, float]:
        mask = (raw_times >= lower_us) & (raw_times < upper_us)
        return (
            int(np.count_nonzero(mask)),
            int(raw_prices[mask].sum()),
            float(raw_discounts[mask].mean()),
        )

    def sorted_bisect() -> tuple[int, int, float]:
        left = int(np.searchsorted(sorted_times, lower_us, side="left"))
        right = int(np.searchsorted(sorted_times, upper_us, side="left"))
        return (
            right - left,
            int(sorted_prices[left:right].sum()),
            float(sorted_discounts[left:right].mean()),
        )

    scan_seconds, scan_result = median_seconds(full_scan, repeats=5)
    bisect_seconds, bisect_result = median_seconds(sorted_bisect, repeats=5)
    if scan_result != bisect_result:
        raise AssertionError("raw sorted and unsorted query results differ")
    return {
        "raw_projection_bytes": sum(path.stat().st_size for path in raw_paths[:3]),
        "full_scan_ms": scan_seconds * 1000,
        "sorted_bisect_ms": bisect_seconds * 1000,
        "result": scan_result,
    }


def render_markdown(report: dict[str, object]) -> str:
    compression = report["compression"]
    search = report["search"]
    assert isinstance(compression, dict) and isinstance(search, dict)
    lines = [
        "# TPC-H SF1 lineitem benchmark",
        "",
        "> Generated by `benchmarks/1_tpch_lineitem/benchmark.py`.",
        "",
        "## Compression",
        "",
        "| Representation | Bytes | Raw ratio |",
        "| --- | ---: | ---: |",
        f"| Canonical raw | {compression['canonical_raw_bytes']:,} | 1.000 |",
        f"| Parquet + Zstd, source order | {compression['parquet_unsorted_bytes']:,} | {compression['parquet_unsorted_ratio']:.3f} |",
        f"| Parquet + Zstd, ship-date order | {compression['parquet_sorted_bytes']:,} | {compression['parquet_sorted_ratio']:.3f} |",
        f"| Estimated Acta streams | {compression['acta_estimated_full_bytes']:,} | {compression['acta_sample_ratio']:.3f} |",
        "",
        "The Acta total extrapolates the ratio from evenly sampled blocks of the",
        "ship-date-ordered layout. It assumes those blocks represent the complete",
        "table, excludes common frame metadata, and is not a written Acta file.",
        "",
        "### Apples-to-apples sampled blocks",
        "",
        "| Representation | Bytes | Raw ratio |",
        "| --- | ---: | ---: |",
        f"| Canonical raw streams | {compression['sample_raw_bytes']:,} | 1.000 |",
        f"| Parquet without compression codec | {compression['sample_parquet_none_bytes']:,} | {compression['sample_parquet_none_ratio']:.3f} |",
        f"| Parquet + Zstd | {compression['sample_parquet_zstd_bytes']:,} | {compression['sample_parquet_zstd_ratio']:.3f} |",
        f"| Acta selected streams + Zstd | {compression['sample_acta_bytes']:,} | {compression['acta_sample_ratio']:.3f} |",
        "",
        "## Thirty-day ship-date query",
        "",
        f"The query matched {search['matching_rows']:,} of {search['rows']:,} rows.",
        "Times are warm-cache medians on the local machine.",
        "",
        "| Layout | Row groups | Projected bytes | Time |",
        "| --- | ---: | ---: | ---: |",
        f"| Parquet, source order | {search['unsorted_groups_touched']} / {search['row_groups_total']} | {search['unsorted_projected_bytes']:,} | {search['parquet_unsorted_ms']:.2f} ms |",
        f"| Parquet, ship-date order | {search['sorted_groups_touched']} / {search['row_groups_total']} | {search['sorted_projected_bytes']:,} | {search['parquet_sorted_ms']:.2f} ms |",
        f"| Raw fixed-width full scan | all rows | {search['raw_projection_bytes']:,} | {search['raw_scan_ms']:.2f} ms |",
        f"| Raw globally sorted bisection | matching range | — | {search['raw_bisect_ms']:.2f} ms |",
        "",
        "Parquet bytes are compressed column-chunk metadata totals; the raw scan",
        "bytes are three complete uncompressed fixed-width arrays. Timings compare",
        "DuckDB Parquet execution with a minimal NumPy memory-map baseline, so they",
        "are end-to-end implementation measurements rather than format-only costs.",
        "",
        "An Acta reader is not implemented. With identical ship-date-local blocks",
        f"its mandatory time bounds would select the same {search['sorted_groups_touched']} blocks,",
        "but no Acta wall-clock claim is made.",
        "",
        "## Findings",
        "",
        f"- Ship-date-ordered Parquet stored the complete table at {compression['parquet_sorted_ratio']:.1%} of canonical raw, including all file metadata.",
        f"- Ordering reduced the thirty-day query from {search['unsorted_groups_touched']} row groups to {search['sorted_groups_touched']} and from {search['unsorted_projected_bytes']:,} to {search['sorted_projected_bytes']:,} projected compressed bytes.",
        f"- In these implementations, the ordered Parquet query was {search['parquet_unsorted_ms'] / search['parquet_sorted_ms']:.1f}× faster than source-order Parquet and {search['raw_scan_ms'] / search['parquet_sorted_ms']:.1f}× faster than the raw full scan.",
        f"- The Acta stream estimate was {1 - compression['acta_sample_ratio'] / compression['parquet_sorted_ratio']:.1%} smaller than ordered Parquet, but it omits common metadata and has no executable reader; this is not yet a runtime advantage.",
        "- Globally sorted raw bisection remained the latency floor, at the cost of",
        "  requiring fixed-width, globally ordered data without a general compressed",
        "  analytical container.",
        "- Source-order date ranges overlapped every row group. Data locality mattered",
        "  more to pruning than the choice between Acta-style blocks and Parquet row groups.",
        "",
        "## Per-column Acta stream probe",
        "",
        "| Column | Logical type | Selected/raw | Selected layouts |",
        "| --- | --- | ---: | --- |",
    ]
    for result in report["columns"]:
        selected = ", ".join(
            f"{name} ×{count}" for name, count in result["selected"].items()
        )
        lines.append(
            f"| {result['name']} | `{result['logical_type']}` | {result['ratio_to_raw']:.3f} | {selected} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation limits",
            "",
            "- TPC-H is generated analytical data, not a live producer stream.",
            "- Acta size is an exhaustive Python stream-selection estimate.",
            "- Parquet measurements use complete files and native implementations.",
            "- Raw and Parquet query times are warm-cache and include different",
            "  levels of parsing, decompression, and execution machinery.",
            "- Append throughput, recovery, and concurrent readers are not measured.",
        ]
    )
    return "\n".join(lines) + "\n"


def run(args: argparse.Namespace) -> dict[str, object]:
    args.work_dir.mkdir(parents=True, exist_ok=True)
    unsorted_path = args.work_dir / "lineitem_acta_compatible.parquet"
    sorted_path = args.work_dir / "lineitem_acta_compatible_sorted.parquet"
    write_times = prepare_parquet(args.database, unsorted_path, sorted_path)

    # Sample the same physical order used by the complete ordered-Parquet result,
    # so the Acta and Parquet compression comparison holds locality constant.
    parquet = pq.ParquetFile(sorted_path)
    sample, sample_indices = sample_table(parquet, args.sample_blocks)
    columns = stream_benchmark(sample, args.sample_blocks)
    sample_raw = sum(int(result["raw_bytes"]) for result in columns)
    sample_acta = sum(int(result["selected_bytes"]) for result in columns)

    sample_none = args.work_dir / "lineitem_sample_none.parquet"
    sample_zstd = args.work_dir / "lineitem_sample_zstd.parquet"
    sample_none_write = write_sample_parquet(sample, sample_none, None)
    sample_zstd_write = write_sample_parquet(sample, sample_zstd, "zstd")

    raw_full = canonical_raw_bytes(args.database)
    acta_ratio = sample_acta / sample_raw
    row_count = parquet.metadata.num_rows

    full_search_table = pq.read_table(
        sorted_path, columns=["l_shipdate"]
    ).combine_chunks()
    sorted_times = (
        full_search_table["l_shipdate"]
        .to_numpy(zero_copy_only=False)
        .astype("datetime64[us]")
        .astype(np.int64)
    )
    lower_us = int(sorted_times[len(sorted_times) // 2])
    upper_us = lower_us + 30 * 86_400_000_000
    raw_search = raw_search_baselines(
        unsorted_path, sorted_path, args.work_dir, lower_us, upper_us
    )
    unsorted_seconds, unsorted_result = parquet_search(
        unsorted_path, lower_us, upper_us
    )
    sorted_seconds, sorted_result = parquet_search(sorted_path, lower_us, upper_us)
    if unsorted_result != sorted_result or unsorted_result != raw_search["result"]:
        raise AssertionError("Parquet and raw search results differ")
    unsorted_groups, unsorted_bytes = selected_row_groups(
        unsorted_path, lower_us, upper_us
    )
    sorted_groups, sorted_bytes = selected_row_groups(sorted_path, lower_us, upper_us)

    report: dict[str, object] = {
        "configuration": {
            "database": str(args.database),
            "rows": row_count,
            "columns": len(parquet.schema_arrow.names),
            "row_groups": parquet.num_row_groups,
            "block_rows": BLOCK_ROWS,
            "sample_blocks": args.sample_blocks,
            "sample_row_group_indices": sample_indices,
            "duckdb": duckdb.__version__,
            "pyarrow": pa.__version__,
            "numpy": np.__version__,
        },
        "compression": {
            "canonical_raw_bytes": raw_full,
            "parquet_unsorted_bytes": unsorted_path.stat().st_size,
            "parquet_unsorted_ratio": unsorted_path.stat().st_size / raw_full,
            "parquet_sorted_bytes": sorted_path.stat().st_size,
            "parquet_sorted_ratio": sorted_path.stat().st_size / raw_full,
            "acta_estimated_full_bytes": round(raw_full * acta_ratio),
            "acta_sample_ratio": acta_ratio,
            "sample_raw_bytes": sample_raw,
            "sample_acta_bytes": sample_acta,
            "sample_parquet_none_bytes": sample_none.stat().st_size,
            "sample_parquet_none_ratio": sample_none.stat().st_size / sample_raw,
            "sample_parquet_zstd_bytes": sample_zstd.stat().st_size,
            "sample_parquet_zstd_ratio": sample_zstd.stat().st_size / sample_raw,
            "parquet_unsorted_write_ms": write_times["unsorted_write_seconds"] * 1000,
            "parquet_sorted_write_ms": write_times["sorted_write_seconds"] * 1000,
            "sample_none_write_ms": sample_none_write * 1000,
            "sample_zstd_write_ms": sample_zstd_write * 1000,
        },
        "search": {
            "rows": row_count,
            "query_lower_us": lower_us,
            "query_upper_us": upper_us,
            "matching_rows": int(sorted_result[0]),
            "row_groups_total": parquet.num_row_groups,
            "unsorted_groups_touched": len(unsorted_groups),
            "sorted_groups_touched": len(sorted_groups),
            "unsorted_projected_bytes": unsorted_bytes,
            "sorted_projected_bytes": sorted_bytes,
            "parquet_unsorted_ms": unsorted_seconds * 1000,
            "parquet_sorted_ms": sorted_seconds * 1000,
            "raw_projection_bytes": raw_search["raw_projection_bytes"],
            "raw_scan_ms": raw_search["full_scan_ms"],
            "raw_bisect_ms": raw_search["sorted_bisect_ms"],
        },
        "columns": columns,
    }
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("database", type=Path)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--sample-blocks", type=int, default=8)
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = run(args)
    rendered_json = json.dumps(report, indent=2) + "\n"
    rendered_markdown = render_markdown(report)
    if args.json_output:
        args.json_output.write_text(rendered_json)
    if args.markdown_output:
        args.markdown_output.write_text(rendered_markdown)
    if not args.json_output and not args.markdown_output:
        print(rendered_markdown, end="")


if __name__ == "__main__":
    main()
