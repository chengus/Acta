#!/usr/bin/env python3
"""Normalize TSBS's TimescaleDB IoT stream into one fixed Arrow schema.

The TSBS IoT generator emits a tag line followed by either a readings or
diagnostics measurement.  The two measurements have different fields, so the
benchmark keeps one nullable wide table.  The first 10,000,000 measurement
rows are retained exactly; generator metadata and tag lines are not counted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


TARGET_ROWS = 10_000_000
ROW_GROUP_ROWS = 65_536

TAG_COLUMNS = [
    "name",
    "fleet",
    "driver",
    "model",
    "device_version",
    "load_capacity",
    "fuel_capacity",
    "nominal_fuel_consumption",
]

COLUMNS = [
    "timestamp",
    *TAG_COLUMNS,
    "measurement",
    "latitude",
    "longitude",
    "elevation",
    "velocity",
    "heading",
    "grade",
    "fuel_consumption",
    "fuel_state",
    "current_load",
    "status",
]

STRING_COLUMNS = {
    "name",
    "fleet",
    "driver",
    "model",
    "device_version",
    "measurement",
}
FLOAT_COLUMNS = {
    "load_capacity",
    "fuel_capacity",
    "nominal_fuel_consumption",
    "latitude",
    "longitude",
    "fuel_consumption",
    "fuel_state",
    "current_load",
}
INT_COLUMNS = {"timestamp", "elevation", "velocity", "heading", "grade", "status"}

MEASUREMENT_FIELDS = {
    "readings": [
        "latitude",
        "longitude",
        "elevation",
        "velocity",
        "heading",
        "grade",
        "fuel_consumption",
    ],
    "diagnostics": ["fuel_state", "current_load", "status"],
}


def parse_number(value: str, column: str):
    if value == "":
        return None
    if column in FLOAT_COLUMNS:
        return float(value)
    if column in INT_COLUMNS:
        return int(value)
    return value


def schema() -> pa.Schema:
    fields = []
    for column in COLUMNS:
        if column == "timestamp":
            data_type = pa.timestamp("us", tz="UTC")
        elif column in STRING_COLUMNS:
            data_type = pa.string()
        elif column in FLOAT_COLUMNS:
            data_type = pa.float64()
        else:
            data_type = pa.int64()
        nullable = column not in {"timestamp", "measurement"}
        fields.append(pa.field(column, data_type, nullable=nullable))
    return pa.schema(fields)


def normalize(source: Path, parquet: Path, target_rows: int):
    values = {column: [] for column in COLUMNS}
    tag_values: dict[str, str] | None = None
    measurement_rows = 0
    source_lines = 0
    measurement_counts: dict[str, int] = {}
    arrow_buffer_bytes = 0
    timestamp_min_us: int | None = None
    timestamp_max_us: int | None = None
    output_schema = schema()
    writer = pq.ParquetWriter(
        parquet,
        output_schema,
        compression="zstd",
        compression_level=1,
        use_dictionary=True,
        write_statistics=True,
    )

    def flush_batch() -> None:
        nonlocal arrow_buffer_bytes
        if not values["timestamp"]:
            return
        arrays = []
        for column in COLUMNS:
            if column in STRING_COLUMNS:
                arrays.append(pa.array(values[column], type=pa.string()))
            elif column == "timestamp":
                arrays.append(pa.array(values[column], type=pa.timestamp("us", tz="UTC")))
            elif column in FLOAT_COLUMNS:
                arrays.append(pa.array(values[column], type=pa.float64()))
            else:
                arrays.append(pa.array(values[column], type=pa.int64()))
        batch = pa.RecordBatch.from_arrays(arrays, schema=output_schema)
        writer.write_batch(batch, row_group_size=ROW_GROUP_ROWS)
        arrow_buffer_bytes += batch.nbytes
        for column in COLUMNS:
            values[column].clear()

    with source.open("r", encoding="utf-8", newline="") as stream:
        for raw_line in stream:
            source_lines += 1
            line = raw_line.rstrip("\n\r")
            if not line:
                continue
            parts = line.split(",")
            kind = parts[0]

            if kind == "tags":
                if len(parts) > 1 and "=" not in parts[1]:
                    # TSBS emits one schema declaration before data.
                    continue
                tag_values = {}
                for pair in parts[1:]:
                    key, separator, value = pair.partition("=")
                    if not separator:
                        raise ValueError(f"invalid tag pair on source line {source_lines}: {pair!r}")
                    tag_values[key] = value
                continue

            if kind in MEASUREMENT_FIELDS and len(parts) > 1 and not parts[1].lstrip("-").isdigit():
                # TSBS emits one field declaration for each measurement.
                continue

            if kind not in MEASUREMENT_FIELDS:
                # The first three source lines declare field names.
                continue
            if tag_values is None:
                raise ValueError(f"measurement before tags on source line {source_lines}")
            if measurement_rows == target_rows:
                break

            fields = MEASUREMENT_FIELDS[kind]
            if len(parts) != len(fields) + 2:
                raise ValueError(
                    f"{kind} source line {source_lines} has {len(parts) - 1} values; "
                    f"expected timestamp plus {len(fields)} fields"
                )

            timestamp_ns = int(parts[1])
            row = {column: None for column in COLUMNS}
            row["timestamp"] = timestamp_ns // 1_000
            row["measurement"] = kind
            for column in TAG_COLUMNS:
                if column not in tag_values:
                    raise ValueError(f"missing tag {column!r} on source line {source_lines}")
                row[column] = parse_number(tag_values[column], column)
            for column, value in zip(fields, parts[2:]):
                row[column] = parse_number(value, column)
            for column in COLUMNS:
                values[column].append(row[column])

            measurement_rows += 1
            measurement_counts[kind] = measurement_counts.get(kind, 0) + 1
            timestamp_min_us = row["timestamp"] if timestamp_min_us is None else min(timestamp_min_us, row["timestamp"])
            timestamp_max_us = row["timestamp"] if timestamp_max_us is None else max(timestamp_max_us, row["timestamp"])
            if len(values["timestamp"]) == ROW_GROUP_ROWS:
                flush_batch()

    flush_batch()
    writer.close()
    if measurement_rows != target_rows:
        raise ValueError(
            f"source contained only {measurement_rows:,} measurement rows; "
            f"expected {target_rows:,}"
        )
    return {
        "source_lines_consumed": source_lines,
        "measurement_counts": measurement_counts,
        "arrow_buffer_bytes": arrow_buffer_bytes,
        "timestamp_min_us": timestamp_min_us,
        "timestamp_max_us": timestamp_max_us,
        "rows": measurement_rows,
        "columns": len(COLUMNS),
    }


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("parquet", type=Path)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--rows", type=int, default=TARGET_ROWS)
    args = parser.parse_args()
    if args.rows <= 0:
        parser.error("--rows must be positive")

    started = time.perf_counter()
    args.parquet.parent.mkdir(parents=True, exist_ok=True)
    normalized = normalize(args.source, args.parquet, args.rows)
    elapsed = time.perf_counter() - started
    manifest = {
        "dataset": "TSBS IoT",
        "tsbs_use_case": "iot",
        "tsbs_format": "timescaledb",
        "tsbs_seed": 123,
        "tsbs_scale": 550,
        "tsbs_log_interval": "10s",
        "target_rows": args.rows,
        **normalized,
        "source_bytes": args.source.stat().st_size,
        "source_sha256": sha256(args.source),
        "normalized_parquet_bytes": args.parquet.stat().st_size,
        "normalized_parquet_sha256": sha256(args.parquet),
        "parquet_row_groups": pq.ParquetFile(args.parquet).num_row_groups,
        "normalization_seconds": elapsed,
    }
    args.manifest.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
