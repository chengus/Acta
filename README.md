# Acta

Acta is a proposed append-only, strongly typed file format for time-series data. It is designed for fast sequential ingestion, compact storage, concurrent readers, and efficient time-range queries—all in one file.

## Design

Producers collect rows in private buffers and encode them as immutable, compressed columnar blocks. Completed blocks are appended to the file with only brief coordination; their physical order does not need to match timestamp order. Readers use per-block metadata such as time bounds, row count, schema, and column statistics to skip irrelevant data, then merge matching blocks when ordered results are required.

The schema defines logical types such as integers, timestamps, decimals, categorical strings, and binary values. Each block may choose an appropriate physical encoding, including delta, bit-packing, run-length, dictionary, or raw encoding, followed by general-purpose compression.

Acta prioritizes:

1. Sequential append performance
2. Compression
3. Concurrent readers
4. Time-range query efficiency
5. Multiple concurrent producers

Acta is not intended to provide transactions, in-place updates, rollback, or database-style recovery. The initial concurrency model is parallel buffering and compression with serialized appends of completed blocks; more advanced extent reservation can be added if benchmarks justify it.

See [case studies](case_study/README.md) for comparisons with existing storage formats and databases.
