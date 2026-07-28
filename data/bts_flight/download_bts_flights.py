#!/usr/bin/env python3
"""Download and prepare a 15.75M-row BTS on-time flight dataset.

The original monthly ZIP archives are retained under ``source/``. A curated,
strongly typed Parquet file is written next to this script for convenient Acta
demo ingestion. The final monthly file is truncated only in the Parquet output
when necessary to hit the requested row count exactly.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterator

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


BASE_URL = "https://transtats.bts.gov/PREZIP"
DEFAULT_TARGET_ROWS = 15_750_000
DEFAULT_START_YEAR = 2018
DEFAULT_START_MONTH = 1
CHUNK_ROWS = 100_000

SOURCE_COLUMNS = [
    "Year",
    "Quarter",
    "Month",
    "DayofMonth",
    "DayOfWeek",
    "FlightDate",
    "Reporting_Airline",
    "DOT_ID_Reporting_Airline",
    "IATA_CODE_Reporting_Airline",
    "Tail_Number",
    "Flight_Number_Reporting_Airline",
    "OriginAirportID",
    "Origin",
    "OriginCityName",
    "OriginState",
    "DestAirportID",
    "Dest",
    "DestCityName",
    "DestState",
    "CRSDepTime",
    "DepTime",
    "DepDelay",
    "DepDelayMinutes",
    "DepDel15",
    "DepartureDelayGroups",
    "DepTimeBlk",
    "TaxiOut",
    "WheelsOff",
    "WheelsOn",
    "TaxiIn",
    "CRSArrTime",
    "ArrTime",
    "ArrDelay",
    "ArrDelayMinutes",
    "ArrDel15",
    "ArrivalDelayGroups",
    "ArrTimeBlk",
    "Cancelled",
    "CancellationCode",
    "Diverted",
    "CRSElapsedTime",
    "ActualElapsedTime",
    "AirTime",
    "Flights",
    "Distance",
    "DistanceGroup",
    "CarrierDelay",
    "WeatherDelay",
    "NASDelay",
    "SecurityDelay",
    "LateAircraftDelay",
]

RENAME_COLUMNS = {
    "Year": "year",
    "Quarter": "quarter",
    "Month": "month",
    "DayofMonth": "day_of_month",
    "DayOfWeek": "day_of_week",
    "FlightDate": "flight_date",
    "Reporting_Airline": "reporting_airline",
    "DOT_ID_Reporting_Airline": "reporting_airline_dot_id",
    "IATA_CODE_Reporting_Airline": "reporting_airline_iata_code",
    "Tail_Number": "tail_number",
    "Flight_Number_Reporting_Airline": "flight_number",
    "OriginAirportID": "origin_airport_id",
    "Origin": "origin",
    "OriginCityName": "origin_city_name",
    "OriginState": "origin_state",
    "DestAirportID": "destination_airport_id",
    "Dest": "destination",
    "DestCityName": "destination_city_name",
    "DestState": "destination_state",
    "CRSDepTime": "scheduled_departure_time",
    "DepTime": "actual_departure_time",
    "DepDelay": "departure_delay",
    "DepDelayMinutes": "departure_delay_minutes",
    "DepDel15": "departure_delayed_15_minutes",
    "DepartureDelayGroups": "departure_delay_group",
    "DepTimeBlk": "departure_time_block",
    "TaxiOut": "taxi_out_minutes",
    "WheelsOff": "wheels_off_time",
    "WheelsOn": "wheels_on_time",
    "TaxiIn": "taxi_in_minutes",
    "CRSArrTime": "scheduled_arrival_time",
    "ArrTime": "actual_arrival_time",
    "ArrDelay": "arrival_delay",
    "ArrDelayMinutes": "arrival_delay_minutes",
    "ArrDel15": "arrival_delayed_15_minutes",
    "ArrivalDelayGroups": "arrival_delay_group",
    "ArrTimeBlk": "arrival_time_block",
    "Cancelled": "cancelled",
    "CancellationCode": "cancellation_code",
    "Diverted": "diverted",
    "CRSElapsedTime": "scheduled_elapsed_minutes",
    "ActualElapsedTime": "actual_elapsed_minutes",
    "AirTime": "air_time_minutes",
    "Flights": "flights",
    "Distance": "distance_miles",
    "DistanceGroup": "distance_group",
    "CarrierDelay": "carrier_delay_minutes",
    "WeatherDelay": "weather_delay_minutes",
    "NASDelay": "nas_delay_minutes",
    "SecurityDelay": "security_delay_minutes",
    "LateAircraftDelay": "late_aircraft_delay_minutes",
}

STRING_COLUMNS = [
    "reporting_airline",
    "reporting_airline_iata_code",
    "tail_number",
    "origin",
    "origin_city_name",
    "origin_state",
    "destination",
    "destination_city_name",
    "destination_state",
    "departure_time_block",
    "arrival_time_block",
    "cancellation_code",
]

INT8_COLUMNS = [
    "quarter",
    "month",
    "day_of_month",
    "day_of_week",
    "departure_delay_group",
    "arrival_delay_group",
    "distance_group",
]

INT16_COLUMNS = [
    "year",
    "scheduled_departure_time",
    "actual_departure_time",
    "wheels_off_time",
    "wheels_on_time",
    "scheduled_arrival_time",
    "actual_arrival_time",
]

INT32_COLUMNS = [
    "reporting_airline_dot_id",
    "flight_number",
    "origin_airport_id",
    "destination_airport_id",
]

FLOAT32_COLUMNS = [
    "departure_delay",
    "departure_delay_minutes",
    "taxi_out_minutes",
    "taxi_in_minutes",
    "arrival_delay",
    "arrival_delay_minutes",
    "scheduled_elapsed_minutes",
    "actual_elapsed_minutes",
    "air_time_minutes",
    "flights",
    "distance_miles",
    "carrier_delay_minutes",
    "weather_delay_minutes",
    "nas_delay_minutes",
    "security_delay_minutes",
    "late_aircraft_delay_minutes",
]

BOOL_COLUMNS = [
    "departure_delayed_15_minutes",
    "arrival_delayed_15_minutes",
    "cancelled",
    "diverted",
]

DICTIONARY_COLUMNS = [
    "reporting_airline",
    "reporting_airline_iata_code",
    "origin",
    "origin_city_name",
    "origin_state",
    "destination",
    "destination_city_name",
    "destination_state",
    "departure_time_block",
    "arrival_time_block",
    "cancellation_code",
]


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-rows", type=int, default=DEFAULT_TARGET_ROWS)
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR)
    parser.add_argument("--start-month", type=int, default=DEFAULT_START_MONTH)
    parser.add_argument("--output-dir", type=Path, default=script_dir)
    parser.add_argument(
        "--force",
        action="store_true",
        help="replace an existing Parquet output and manifest",
    )
    return parser.parse_args()


def month_sequence(year: int, month: int) -> Iterator[tuple[int, int]]:
    while True:
        yield year, month
        month += 1
        if month == 13:
            year += 1
            month = 1


def archive_filename(year: int, month: int) -> str:
    return (
        "On_Time_Reporting_Carrier_On_Time_Performance_"
        f"1987_present_{year}_{month}.zip"
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def archive_is_valid(path: Path) -> bool:
    if not path.is_file() or path.stat().st_size == 0:
        return False
    try:
        with zipfile.ZipFile(path) as archive:
            csv_members = [
                member
                for member in archive.infolist()
                if member.filename.lower().endswith(".csv")
            ]
            return bool(csv_members) and archive.testzip() is None
    except (OSError, zipfile.BadZipFile):
        return False


def download_archive(url: str, destination: Path) -> None:
    if archive_is_valid(destination):
        print(f"reuse {destination.name}", flush=True)
        return

    if destination.exists():
        print(f"replace invalid archive {destination.name}", flush=True)
        destination.unlink()

    temporary = destination.with_suffix(destination.suffix + ".part")
    temporary.unlink(missing_ok=True)
    print(f"download {url}", flush=True)

    try:
        subprocess.run(
            [
                "curl",
                "--fail",
                "--location",
                "--silent",
                "--show-error",
                "--retry",
                "4",
                "--retry-delay",
                "2",
                "--output",
                str(temporary),
                url,
            ],
            check=True,
        )
        if not archive_is_valid(temporary):
            raise RuntimeError(f"downloaded archive failed validation: {url}")
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def largest_csv_member(archive: zipfile.ZipFile) -> zipfile.ZipInfo:
    members = [
        member
        for member in archive.infolist()
        if member.filename.lower().endswith(".csv")
    ]
    if not members:
        raise RuntimeError("BTS archive contains no CSV file")
    return max(members, key=lambda member: member.file_size)


def normalize_chunk(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.rename(columns=RENAME_COLUMNS)
    frame["flight_date"] = pd.to_datetime(
        frame["flight_date"], format="%Y-%m-%d", errors="raise"
    ).dt.date

    for column in STRING_COLUMNS:
        frame[column] = frame[column].astype("string")
    for column in INT8_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="raise").astype("Int8")
    for column in INT16_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int16")
    for column in INT32_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="raise").astype("Int32")
    for column in FLOAT32_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Float32")
    for column in BOOL_COLUMNS:
        numeric = pd.to_numeric(frame[column], errors="coerce")
        frame[column] = numeric.map({0.0: False, 1.0: True}).astype("boolean")

    return frame


def csv_chunks(
    archive_path: Path, rows_remaining: int
) -> Iterator[pd.DataFrame]:
    with zipfile.ZipFile(archive_path) as archive:
        member = largest_csv_member(archive)
        with archive.open(member) as csv_stream:
            reader = pd.read_csv(
                csv_stream,
                usecols=SOURCE_COLUMNS,
                chunksize=CHUNK_ROWS,
                low_memory=False,
            )
            for frame in reader:
                if rows_remaining <= 0:
                    return
                if len(frame) > rows_remaining:
                    frame = frame.iloc[:rows_remaining].copy()
                normalized = normalize_chunk(frame)
                yield normalized
                rows_remaining -= len(normalized)


def main() -> int:
    args = parse_args()
    if args.target_rows <= 0:
        raise ValueError("--target-rows must be positive")
    if not 1 <= args.start_month <= 12:
        raise ValueError("--start-month must be between 1 and 12")

    output_dir = args.output_dir.resolve()
    source_dir = output_dir / "source"
    output_dir.mkdir(parents=True, exist_ok=True)
    source_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = output_dir / f"bts_flights_{args.target_rows}.parquet"
    manifest_path = output_dir / "manifest.json"
    temporary_parquet = parquet_path.with_suffix(".parquet.part")

    if parquet_path.exists() and not args.force:
        raise FileExistsError(
            f"{parquet_path} already exists; pass --force to regenerate it"
        )

    temporary_parquet.unlink(missing_ok=True)
    if args.force:
        parquet_path.unlink(missing_ok=True)
        manifest_path.unlink(missing_ok=True)

    writer: pq.ParquetWriter | None = None
    rows_written = 0
    source_records: list[dict[str, object]] = []

    try:
        for year, month in month_sequence(args.start_year, args.start_month):
            filename = archive_filename(year, month)
            archive_path = source_dir / filename
            url = f"{BASE_URL}/{filename}"
            download_archive(url, archive_path)

            archive_rows_used = 0
            for frame in csv_chunks(archive_path, args.target_rows - rows_written):
                table = pa.Table.from_pandas(frame, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(
                        temporary_parquet,
                        table.schema,
                        compression="zstd",
                        compression_level=6,
                        use_dictionary=DICTIONARY_COLUMNS,
                        write_statistics=True,
                    )
                writer.write_table(table, row_group_size=CHUNK_ROWS)
                chunk_rows = len(frame)
                rows_written += chunk_rows
                archive_rows_used += chunk_rows

            source_records.append(
                {
                    "year": year,
                    "month": month,
                    "url": url,
                    "archive": str(archive_path.relative_to(output_dir)),
                    "archive_bytes": archive_path.stat().st_size,
                    "archive_sha256": sha256_file(archive_path),
                    "rows_used": archive_rows_used,
                }
            )
            print(
                f"processed {year}-{month:02d}: "
                f"{archive_rows_used:,} rows; total {rows_written:,}",
                flush=True,
            )

            if rows_written >= args.target_rows:
                break
    finally:
        if writer is not None:
            writer.close()

    if rows_written != args.target_rows:
        temporary_parquet.unlink(missing_ok=True)
        raise RuntimeError(
            f"expected {args.target_rows:,} rows, wrote {rows_written:,}"
        )

    os.replace(temporary_parquet, parquet_path)
    parquet_file = pq.ParquetFile(parquet_path)
    if parquet_file.metadata.num_rows != args.target_rows:
        raise RuntimeError(
            "Parquet verification failed: "
            f"{parquet_file.metadata.num_rows:,} rows"
        )

    manifest = {
        "dataset": "BTS Reporting Carrier On-Time Performance (1987-present)",
        "source": "U.S. Bureau of Transportation Statistics",
        "generated_at": datetime.now(UTC).isoformat(),
        "target_rows": args.target_rows,
        "actual_rows": parquet_file.metadata.num_rows,
        "columns": parquet_file.metadata.num_columns,
        "row_groups": parquet_file.metadata.num_row_groups,
        "output": parquet_path.name,
        "output_bytes": parquet_path.stat().st_size,
        "output_sha256": sha256_file(parquet_path),
        "source_archives": source_records,
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    print(
        f"ready: {parquet_path} "
        f"({args.target_rows:,} rows, {parquet_path.stat().st_size:,} bytes)",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        raise SystemExit(130)
