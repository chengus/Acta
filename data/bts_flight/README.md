# BTS flight demo data

`download_bts_flights.py` downloads official monthly **Reporting Carrier
On-Time Performance (1987-present)** archives from the U.S. Bureau of
Transportation Statistics. It retains the original full-schema ZIP archives
under `source/` and writes a curated, strongly typed Parquet file containing
exactly 15,750,000 rows by default.

The curated schema keeps 51 useful fields spanning:

- calendar dates and integer time values;
- airline, aircraft, airport, city, state, time-block, and cancellation strings;
- nullable delays, durations, distances, and taxi-time floating-point values;
- cancellation, diversion, and 15-minute-delay booleans.

Requirements: Python 3.10 or newer, `pandas`, `pyarrow`, and `curl`.

Run from the repository root:

```bash
python3 data/bts_flight/download_bts_flights.py
```

Use `--force` to rebuild an existing output. Change the target with
`--target-rows`, for example:

```bash
python3 data/bts_flight/download_bts_flights.py \
  --target-rows 15500000 \
  --force
```

Generated artifacts:

- `bts_flights_15750000.parquet` — the Acta demo input;
- `manifest.json` — exact source URLs, archive and output checksums, and counts;
- `source/*.zip` — original BTS monthly downloads.

Source documentation:

- <https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ>
- <https://transtats.bts.gov/PREZIP/>
