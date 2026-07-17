# Encoding study conclusions

The study used Zstandard level 1 and evenly spaced contiguous samples from the
3,724,889-row January 2026 NYC TLC yellow-taxi file. All transforms passed a
round-trip check. Controlled cases cover regular timestamps, monotonic
counters, smooth noisy floats, and sparse/bursty nulls.

## Block-size iteration

The aggregate below covers the twelve real or real-derived columns, including
the intentionally incompressible fixed 16-byte trip ID.

| Rows per block | Blocks sampled | Selected/raw bytes |
| ---: | ---: | ---: |
| 4,096 | 16 | 0.309 |
| 65,536 | 4 | 0.284 |
| 262,144 | 4 | 0.282 |

Moving from 4K to 65K rows improved the aggregate ratio by about 8%. Moving
from 65K to 262K improved it by less than 1% relative while quadrupling the
buffering, interrupted-tail, and minimum decode granularity. The initial writer
should therefore target 65,536 rows, subject to a separate encoded-byte cap.

## Encoding findings at 65,536 rows

| Data shape | Best observed choice | Result |
| --- | --- | ---: |
| mostly constant boolean flag | constant, RLE, or bitpack + Zstd by block | 0.017 of bitpacked raw |
| signed zone difference | dictionary or FOR | 0.293 of raw `int32` |
| low-cardinality unsigned zone | dictionary + Zstd | 0.190 of raw `uint32` |
| nullable passenger count values | delta, FOR, or empty all-null block | 0.022 of dense raw values |
| taxi distance float | dictionary + Zstd | 0.178 of raw `float64` |
| scaled fare cents | dictionary or FOR + Zstd | 0.163 of raw `int64` |
| unordered pickup timestamp | raw or delta + Zstd by block | 0.492 of raw `int64` |
| route UTF-8 | dictionary + Zstd | 0.101 of plain lengths + bytes |
| categorical payment label | constant or dictionary + Zstd | 0.006 of plain lengths + bytes |
| derived variable binary key | dictionary + Zstd | 0.168 of plain lengths + bytes |
| pseudorandom fixed 16-byte ID | raw, uncompressed | 1.000 of raw |
| regular sensor timestamp | delta + Zstd | 0.00019 of raw `int64` |
| monotonic counter | delta-of-delta + Zstd | 0.00011 of raw `uint64` |
| smooth noisy float | byte-stream split + Zstd | 0.764 of raw `float64` |
| sparse and bursty validity | boolean RLE + Zstd | 0.007 of bitpacked raw |

Ratios exclude common frame and stream-directory bytes. Exact measurements and
candidate tables are in [nyc_taxi_65536.md](nyc_taxi_65536.md).
The passenger-count ratio covers dense non-null values; its required validity
stream is the separately reported `passenger_validity` case. In this evenly
sampled run each block was all-valid or all-null, so the four validity streams
occupied four bytes in total.

## Decisions

1. Encoding remains a per-block choice. No transform wins for all blocks of a
   logical type.
2. The writer collects cheap statistics while buffering, estimates all legal
   candidates, then compresses raw plus no more than the two best estimates.
3. Raw remains the tie-breaker. A specialized transform must save at least the
   larger of 64 bytes or 1% after metadata.
4. Integer-like types retain constant, dictionary, RLE, FOR, delta, and
   delta-of-delta candidates. Real unordered timestamps demonstrate that raw
   must remain available.
5. Floats retain raw, dictionary, RLE, and byte-stream split. The prototype's
   simple XOR transform lost to raw or byte-stream split in both real and
   controlled cases, so XOR/Gorilla is deferred until a production-quality
   implementation proves worthwhile.
6. Variable-width values use plain lengths + bytes, dictionary, constant, or
   RLE layouts. Categorical strings strongly favor block-local dictionaries.
7. Validity is dense-value plus all-valid, all-null, bitpacked, or boolean-RLE
   representation. `NULL` is not a standalone logical type.
8. Compression is chosen independently per physical stream. Incompressible
   fixed IDs remain raw rather than paying Zstandard overhead.

## Limitations

- Only one public event dataset was measured.
- Dictionary construction and bit packing are clear Python prototypes, not
  representative C++ implementations.
- The selected-encode throughput column is estimated from candidate generation;
  exhaustive-search throughput is the measured cost of trying everything.
- The study measures column streams, not full Acta files, cache behavior, query
  pruning, or synchronization latency.
