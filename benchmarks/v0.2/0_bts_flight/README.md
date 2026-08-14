# BTS flight Parquet → Acta benchmark

This is an end-to-end conversion benchmark over a complete 15,750,000-row
subset of the U.S. Bureau of Transportation Statistics Reporting Carrier
On-Time Performance dataset. It uses the typed adapter in
[`../../parquet_to_acta.rs`](../../parquet_to_acta.rs), writes a
real Acta v0.2 file, and fully decodes the result for validation.

The recorded result is one release-mode run on an Apple Silicon Mac. It is a
reproducible reference point, not a cross-machine throughput claim. See the
[recorded result](results/summary.md) and
[structured result](results/bts_flights_15750000.json).

## 1. Reproduce the Parquet dataset

The exact recorded input and output are available from the
[Acta benchmark dataset on Hugging Face](https://huggingface.co/datasets/lu-chengass/acta-data).
Download them when you need to validate the recorded artifact or exercise the
reader without downloading the original BTS archives:

~~~bash
hf download lu-chengass/acta-data \
  --repo-type dataset \
  --revision main \
  --include 'bts_flight/v0.2/*' \
  --local-dir /tmp/acta-hf

cp /tmp/acta-hf/bts_flight/v0.2/bts_flights_15750000.parquet \
  benchmarks/v0.2/0_bts_flight/bts_flights_15750000.parquet
cp /tmp/acta-hf/bts_flight/v0.2/bts_flights_15750000.acta \
  benchmarks/v0.2/0_bts_flight/bts_flights_15750000.acta
cp /tmp/acta-hf/bts_flight/v0.2/manifest.json \
  benchmarks/v0.2/0_bts_flight/manifest.json
~~~

The dataset card records the artifact layout, source attribution, and
checksums. For a permanently reproducible experiment, replace `main` with the
immutable Hugging Face revision shown on the dataset page.

From the repository root, install Python 3.10+ with pandas and pyarrow, then
run:

~~~bash
python3 benchmarks/v0.2/0_bts_flight/download_bts_flights.py \
  --target-rows 15750000
~~~

The downloader obtains the monthly source ZIP archives from the BTS
[PREZIP archive](https://transtats.bts.gov/PREZIP/), retains their checksums in
benchmarks/v0.2/0_bts_flight/manifest.json, and writes:

~~~
benchmarks/v0.2/0_bts_flight/bts_flights_15750000.parquet
~~~

If the target already exists, the downloader intentionally refuses to replace
it; pass --force only when regenerating it. The recorded dataset is:

| Property | Value |
| --- | ---: |
| Rows | 15,750,000 |
| Columns | 51 |
| Parquet row groups | 173 |
| Bytes | 381,644,436 |
| SHA-256 | 33c7d1fd28faa95d002e05ebf778e6345c4eb1c9d39416224ee08967bad23f27 |

The manifest is part of the reproduction record. Its source archive URLs,
archive sizes, archive SHA-256 values, row contributions, and Parquet checksum
should remain available alongside the generated dataset.

## 2. Build and convert to Acta

The Parquet and Arrow crates are opt-in development dependencies. They are
compiled for this example only when the parquet-example feature is enabled;
they are not part of Acta's normal library dependency graph.

Build the converter:

~~~bash
cargo build --release \
  --features parquet-example \
  --example parquet_to_acta
~~~

Use a fresh output path for each run because Acta creation is deliberately
create-new:

~~~bash
mkdir -p /tmp/acta-benchmark/bts_flight
cargo run --release \
  --features parquet-example \
  --example parquet_to_acta -- \
  benchmarks/v0.2/0_bts_flight/bts_flights_15750000.parquet \
  /tmp/acta-benchmark/bts_flight/bts_flights_15750000.acta
~~~

The default reader batch size and Acta row-block target are both 65,536 rows.
The timer covers Parquet reader construction, Arrow batch decoding, typed
batch conversion, Acta adaptive encoding with Zstandard level 6, and
Writer::finish.
It starts after Parquet metadata/schema inspection and explicit schema mapping;
it excludes dataset download and compilation.

The recorded run uses adaptive selection.
`WriterEncoding::Fixed(WriterTransform::...)` is not a drop-in replacement for
it here: fixed mode applies one transform to every column and never falls back,
and this 51-column schema mixes integers, floats, strings, and timestamps, so
no single transform is offered for all of them. Comparing a fixed transform
against adaptive on this dataset means converting a subset of the schema whose
columns all offer that transform.

## 3. Schema mapping

The converter checks every Parquet field against an explicit mapping before it
opens the Acta writer. It preserves source nullability except that
flight_date is required as the Acta primary column.

| Parquet columns | Acta logical type |
| --- | --- |
| flight_date | date32 primary |
| year | int16 |
| quarter, month, day_of_month, day_of_week, departure_delay_group, arrival_delay_group, distance_group | int8 |
| reporting_airline_dot_id, flight_number, origin_airport_id, destination_airport_id | int32 |
| scheduled_departure_time, actual_departure_time, wheels_off_time, wheels_on_time, scheduled_arrival_time, actual_arrival_time | int16 |
| departure_delay, departure_delay_minutes, taxi_out_minutes, taxi_in_minutes, arrival_delay, arrival_delay_minutes, scheduled_elapsed_minutes, actual_elapsed_minutes, air_time_minutes, flights, distance_miles, carrier_delay_minutes, weather_delay_minutes, nas_delay_minutes, security_delay_minutes, late_aircraft_delay_minutes | float32 |
| departure_delayed_15_minutes, arrival_delayed_15_minutes, cancelled, diverted | bool |
| reporting_airline, reporting_airline_iata_code, origin, origin_city_name, origin_state, destination, destination_city_name, destination_state, departure_time_block, arrival_time_block, cancellation_code | categorical |
| tail_number | utf8 |

Categorical fields are encoded as unordered Acta categoricals; tail_number
remains UTF-8 because it is less suitable for a low-cardinality dictionary.

## 4. Verify the output

Check both artifacts against the recorded checksums:

~~~bash
shasum -a 256 \
  benchmarks/v0.2/0_bts_flight/bts_flights_15750000.parquet \
  /tmp/acta-benchmark/bts_flight/bts_flights_15750000.acta
~~~

Expected values:

~~~text
33c7d1fd28faa95d002e05ebf778e6345c4eb1c9d39416224ee08967bad23f27  benchmarks/v0.2/0_bts_flight/bts_flights_15750000.parquet
ee5c07e11197665895065d3b998f30d2a3984bd6920e9121d04db67e55821d29  /tmp/acta-benchmark/bts_flight/bts_flights_15750000.acta
~~~

The Parquet checksum is stored in `benchmarks/v0.2/0_bts_flight/manifest.json`;
the Acta checksum is recorded in the benchmark result and Hugging Face dataset
card.

Run the full Acta decoder/validator:

~~~bash
cargo run --release --example validate_acta -- \
  /tmp/acta-benchmark/bts_flight/bts_flights_15750000.acta
~~~

Expected validation summary:

~~~text
full validation: format=(0, 2), frames=242, rows=15750000, incomplete_tail=false
~~~

For a compact metadata check, use the CLI:

~~~bash
cargo run --release -- inspect \
  /tmp/acta-benchmark/bts_flight/bts_flights_15750000.acta
~~~

The expected Acta artifact has 241 data blocks, 15,750,000 rows, a complete
tail, and a date32 primary column named flight_date.

## 5. Recorded result

The committed result files record the exact options and environment:

~~~
results/summary.md
results/bts_flights_15750000.json
~~~

The recorded conversion uses Zstandard level 6 to match the Parquet input's
compression level. It produced a 325,340,952-byte Acta file in 129.824 s:

| Metric | Result |
| --- | ---: |
| Parquet input | 381,644,436 bytes (363.96 MiB) |
| Acta output | 325,340,952 bytes (310.27 MiB) |
| Acta / Parquet size ratio | 0.852× |
| Size change | 14.753% smaller |
| Conversion time | 129.824 s |
| Input throughput | 2.80 MiB/s |
| Row throughput | 121,318 rows/s |
| Parquet batches / Acta blocks | 241 / 241 |

The generated .acta file is ignored under this benchmark directory so large binary
artifacts do not enter source control. The checksum above is sufficient to
identify the exact output used for the recorded result.
