#!/usr/bin/env python3
"""Run the TSBS IoT target-format benchmark and render its result files."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import time
from pathlib import Path

import psutil


FORMATS = ("acta", "parquet", "csv")
MIB = 1024 * 1024


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_child(binary: Path, mode: str, format_name: str, input_path: Path, output_path: Path) -> dict:
    command = [str(binary), "--mode", mode, "--format", format_name,
               "--input", str(input_path), "--output", str(output_path)]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
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
        raise RuntimeError(f"{format_name} {mode} failed:\n{stderr}\n{stdout}")
    metrics = json.loads(stdout.strip().splitlines()[-1])
    metrics["peak_rss_bytes"] = peak_rss
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("--binary", type=Path, default=Path("target/release/examples/tsbs_iot_benchmark"))
    parser.add_argument("--result", type=Path)
    args = parser.parse_args()
    if not args.binary.exists():
        parser.error(f"benchmark binary does not exist: {args.binary}")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    formats = []
    for format_name in FORMATS:
        output = args.output_dir / f"tsbs_iot_10m.{format_name}"
        if output.exists():
            raise RuntimeError(f"refusing to overwrite existing output: {output}")
        write = run_child(args.binary, "write", format_name, args.input, output)
        read = run_child(args.binary, "read", format_name, args.input, output)
        output_bytes = output.stat().st_size
        arrow_bytes = write["arrow_buffer_bytes"]
        elapsed = write["elapsed_seconds"]
        formats.append({
            "format": format_name,
            "rows": write["rows"],
            "output_bytes": output_bytes,
            "bytes_per_row": output_bytes / write["rows"],
            "arrow_buffer_bytes": arrow_bytes,
            "output_over_arrow_ratio": output_bytes / arrow_bytes,
            "write_seconds": elapsed,
            "write_rows_per_second": write["rows"] / elapsed,
            "write_logical_mib_per_second": arrow_bytes / MIB / elapsed,
            "write_output_mib_per_second": output_bytes / MIB / elapsed,
            "finish_seconds": write["finish_seconds"],
            "write_peak_rss_bytes": write["peak_rss_bytes"],
            "read_seconds": read["elapsed_seconds"],
            "read_rows_per_second": read["rows"] / read["elapsed_seconds"],
            "read_peak_rss_bytes": read["peak_rss_bytes"],
            "read_blocks_or_row_groups": read["blocks_or_row_groups"],
            "read_blocks_considered": read["blocks_considered"],
            "read_blocks_pruned": read["blocks_pruned"],
            "read_streams_decoded": read["streams_decoded"],
            "read_stream_bytes_decoded": read["stream_bytes_decoded"],
            "read_bytes_read": read["bytes_read"],
            "sha256": sha256(output),
        })

    result = {
        "benchmark": "tsbs_iot_10m_target_formats",
        "recorded_at": "2026-08-18",
        "dataset": {
            "source": "TSBS IoT use case",
            "input_parquet": str(args.input),
            "rows": 10_000_000,
            "columns": 20,
            "batch_rows": 65_536,
            "row_group_or_block_rows": 65_536,
            "input_parquet_bytes": args.input.stat().st_size,
            "input_parquet_sha256": sha256(args.input),
        },
        "write_scope": "Input Parquet is fully decoded and materialized before the write timer. The timed section excludes TSBS generation, source parsing, source Parquet decoding, and compilation; it covers target conversion/serialization and target finish/synchronization.",
        "options": {
            "acta": "adaptive + zstd level 1, no optional statistics",
            "parquet": "dictionary encoding + zstd level 1",
            "csv": "UTF-8 header, integer UTC microseconds, empty nullable fields",
        },
        "formats": formats,
        "environment": {"python": os.sys.version.split()[0], "platform": os.uname().sysname + " " + os.uname().release},
    }
    result_path = args.result or (args.output_dir / "tsbs_iot_10m.json")
    result_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    lines = [
        "# TSBS IoT 10M target-format benchmark", "",
        "> The write timer starts after the input Parquet is fully decoded and materialized.",
        "> It excludes TSBS generation, source parsing, source decoding, and compilation.", "",
        "| format | output bytes | bytes/row | write s | write rows/s | logical MiB/s | finish s | read s | read rows/s | peak RSS MiB | SHA-256 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in formats:
        lines.append(
            f"| {row['format']} | {row['output_bytes']:,} | {row['bytes_per_row']:.2f} | {row['write_seconds']:.3f} | "
            f"{row['write_rows_per_second']:,.0f} | {row['write_logical_mib_per_second']:.2f} | {row['finish_seconds']:.3f} | "
            f"{row['read_seconds']:.3f} | {row['read_rows_per_second']:,.0f} | "
            f"{max(row['write_peak_rss_bytes'], row['read_peak_rss_bytes']) / MIB:.1f} | `{row['sha256']}` |"
        )
    (result_path.parent / "tsbs_iot_10m.md").write_text("\n".join(lines) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
