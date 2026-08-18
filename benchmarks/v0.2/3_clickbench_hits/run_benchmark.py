#!/usr/bin/env python3
"""Run the full ClickBench hits target-format benchmark."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import psutil

MIB = 1024 * 1024
FORMATS = ("acta",)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * MIB), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_child(command: list[str]) -> tuple[dict, int]:
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    child = psutil.Process(process.pid)
    peak_rss = 0
    while process.poll() is None:
        try:
            peak_rss = max(peak_rss, child.memory_info().rss)
        except psutil.Error:
            pass
        time.sleep(0.05)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise RuntimeError(
            f"benchmark failed ({process.returncode}):\n"
            f"command: {' '.join(command)}\n"
            f"stderr:\n{stderr}\nstdout:\n{stdout}"
        )
    metrics = json.loads(stdout.strip().splitlines()[-1])
    return metrics, peak_rss


def enrich(metrics: dict, peak_rss: int, output_path: Path | None) -> dict:
    rows = metrics["rows"]
    logical = metrics["logical_bytes"]
    write_seconds = metrics["target_write_seconds"]
    read_seconds = metrics["read_seconds"]
    result = dict(metrics)
    result["peak_rss_bytes"] = peak_rss
    result["bytes_per_row"] = metrics["output_file_bytes"] / rows
    result["output_mib_per_second"] = metrics["output_file_bytes"] / MIB / write_seconds if write_seconds else None
    result["logical_mib_per_second"] = logical / MIB / write_seconds if write_seconds else None
    result["rows_per_second"] = rows / write_seconds if write_seconds else None
    result["read_rows_per_second"] = rows / read_seconds if read_seconds else None
    result["output_path"] = str(output_path) if output_path else None
    if output_path and output_path.exists():
        result["sha256"] = sha256(output_path)
    else:
        result["sha256"] = None
    return result


def parquet_metadata(path: Path) -> dict:
    parquet = pq.ParquetFile(path)
    metadata = parquet.metadata
    compressed = 0
    uncompressed = 0
    for row_group in range(metadata.num_row_groups):
        group = metadata.row_group(row_group)
        for column in range(metadata.num_columns):
            chunk = group.column(column)
            compressed += chunk.total_compressed_size
            uncompressed += chunk.total_uncompressed_size
    return {
        "rows": metadata.num_rows,
        "columns": metadata.num_columns,
        "row_groups": metadata.num_row_groups,
        "file_bytes": path.stat().st_size,
        "column_chunks_compressed_bytes": compressed,
        "column_chunks_uncompressed_bytes": uncompressed,
        "created_by": metadata.created_by,
    }


def estimate_csv_size(path: Path, rows: int) -> dict:
    sample_rows = 1_000_000
    parquet = pq.ParquetFile(path)
    batch = next(parquet.iter_batches(batch_size=sample_rows))
    with tempfile.NamedTemporaryFile(prefix="clickbench-csv-sample-", suffix=".csv") as sample:
        with pa.OSFile(sample.name, "wb") as sink:
            csv.write_csv(batch, sink)
        sample_bytes = Path(sample.name).stat().st_size
    return {
        "sample_rows": batch.num_rows,
        "sample_bytes": sample_bytes,
        "estimated_full_bytes": round(sample_bytes * rows / batch.num_rows),
    }


def blank_format(
    format_name: str,
    rows: int,
    columns: int,
    output_bytes: int,
    source_sha256: str,
    note: str,
) -> dict:
    return {
        "mode": "not_measured",
        "format": format_name,
        "rows": rows,
        "columns": columns,
        "input_file_bytes": output_bytes,
        "output_file_bytes": output_bytes,
        "logical_bytes": None,
        "source_decode_seconds": None,
        "target_write_seconds": None,
        "finish_seconds": None,
        "read_seconds": None,
        "blocks_or_row_groups": None,
        "blocks_considered": None,
        "blocks_pruned": None,
        "streams_decoded": None,
        "stream_bytes_decoded": None,
        "bytes_read": None,
        "csv_chunks": None,
        "peak_rss_bytes": None,
        "bytes_per_row": output_bytes / rows,
        "output_mib_per_second": None,
        "logical_mib_per_second": None,
        "rows_per_second": None,
        "read_rows_per_second": None,
        "output_path": None,
        "sha256": source_sha256,
        "source_decode_excluded": None,
        "measured": False,
        "note": note,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument(
        "--binary",
        type=Path,
        default=Path("target/release/examples/clickbench_hits_benchmark"),
    )
    parser.add_argument("--result", type=Path)
    args = parser.parse_args()

    if not args.input.exists():
        parser.error(f"input does not exist: {args.input}")
    if not args.binary.exists():
        parser.error(f"benchmark binary does not exist: {args.binary}")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    source = parquet_metadata(args.input)
    source["sha256"] = sha256(args.input)
    results = []

    for format_name in FORMATS:
        output = args.output_dir / f"clickbench_hits.{format_name}"
        write_metrics, write_peak_rss = run_child([
            str(args.binary),
            "--mode", "write",
            "--format", format_name,
            "--input", str(args.input),
            "--output", str(output),
        ])
        read_metrics, read_peak_rss = run_child([
            str(args.binary),
            "--mode", "read",
            "--format", format_name,
            "--input", str(args.input),
            "--output", str(output),
        ])
        combined = enrich(write_metrics, max(write_peak_rss, read_peak_rss), output)
        combined["read_seconds"] = read_metrics["read_seconds"]
        combined["read_rows_per_second"] = read_metrics["rows"] / read_metrics["read_seconds"]
        combined["read_blocks_or_row_groups"] = read_metrics["blocks_or_row_groups"]
        combined["read_blocks_considered"] = read_metrics["blocks_considered"]
        combined["read_blocks_pruned"] = read_metrics["blocks_pruned"]
        combined["read_streams_decoded"] = read_metrics["streams_decoded"]
        combined["read_stream_bytes_decoded"] = read_metrics["stream_bytes_decoded"]
        combined["read_bytes_read"] = read_metrics["bytes_read"]
        combined["source_decode_excluded"] = True
        results.append(combined)

    results.append(blank_format(
        "parquet",
        source["rows"],
        source["columns"],
        source["file_bytes"],
        source["sha256"],
        "Original uploaded Parquet is the size baseline; Parquet write/read throughput was not measured.",
    ))

    csv_estimate = estimate_csv_size(args.input, source["rows"])
    csv_result = blank_format(
        "csv",
        source["rows"],
        source["columns"],
        csv_estimate["estimated_full_bytes"],
        source["sha256"],
        "Full CSV write/read was not measured because the estimated plain CSV does not fit in available local space.",
    )
    csv_result["csv_size_estimate"] = csv_estimate
    results.append(csv_result)

    for result in results:
        result["size_ratio_vs_source_parquet"] = result["output_file_bytes"] / source["file_bytes"]
        result["size_change_percent_vs_source_parquet"] = (
            result["size_ratio_vs_source_parquet"] - 1
        ) * 100

    rustc = subprocess.run(["rustc", "--version"], check=True, capture_output=True, text=True).stdout.strip()
    report = {
        "benchmark": "clickbench_hits_target_formats",
        "recorded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "dataset": {
            "source": "ClickHouse ClickBench hits-compatible Parquet",
            "input_path": str(args.input),
            **source,
            "batch_rows": 65_536,
            "row_group_or_block_rows": 65_536,
        },
        "write_scope": (
            "Each Parquet batch is fully decoded before its target-write timer starts. "
            "Source Parquet decoding is reported separately and excluded from target "
            "write throughput. Acta timing includes Arrow-to-Acta typed batch conversion, "
            "Writer::append, and target compression; finish/sync is reported separately."
        ),
        "csv_scope": (
            "CSV size is extrapolated from a 1,000,000-row pyarrow.csv sample. "
            "Full CSV write/read throughput is not measured because the estimated "
            "plain CSV does not fit in available local space."
        ),
        "options": {
            "acta": "adaptive encoding + Zstandard level 1, no optional statistics",
            "parquet": "original uploaded Parquet source; no rewrite",
            "csv": "UTF-8 CSV size extrapolated from a 1,000,000-row pyarrow.csv sample",
        },
        "formats": results,
        "environment": {
            "python": platform.python_version(),
            "rustc": rustc,
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
        },
    }

    result_path = args.result or (args.output_dir / "clickbench_hits.json")
    result_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    lines = [
        "# ClickBench hits target-format benchmark",
        "",
        "> Target-write timing begins after each source Parquet batch is decoded.",
        "> Source decoding is excluded from write throughput and reported separately.",
        "",
        "| format | output bytes | bytes/row | write s | rows/s | logical MiB/s | finish s | read s | read rows/s | peak RSS MiB | source ratio |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in results:
        write_rows_per_second = (
            f"{row['rows_per_second']:,.0f}" if row["rows_per_second"] is not None else "—"
        )
        def display(value: object, suffix: str = "") -> str:
            return "—" if value is None else f"{value}{suffix}"

        lines.append(
            f"| {row['format']} | {row['output_file_bytes']:,} | {row['bytes_per_row']:.2f} | "
            f"{display(row['target_write_seconds'])} | {write_rows_per_second} | "
            f"{display(row['logical_mib_per_second'])} | {display(row['finish_seconds'])} | "
            f"{display(row['read_seconds'])} | {display(row['read_rows_per_second'])} | "
            f"{display(row['peak_rss_bytes'])} | {row['size_ratio_vs_source_parquet']:.3f}x |"
        )
    lines += [
        "",
        "CSV size is extrapolated from a 1,000,000-row sample; full CSV write/read throughput was not measured because the plain file does not fit locally.",
    ]
    (result_path.parent / "clickbench_hits.md").write_text("\n".join(lines) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
