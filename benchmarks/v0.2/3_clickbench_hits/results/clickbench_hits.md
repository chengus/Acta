# ClickBench hits target-format benchmark

The Acta write timer starts after each source Parquet batch has been decoded;
source decoding is excluded from the Acta write throughput.

| format | size | bytes/row | write time | write rows/s | logical MiB/s | finish/sync | full read | read rows/s | size vs Parquet |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Acta | 8,955,412,072 B | 89.56 | 1,324.585 s | 75,493 | 68.94 | 0.431 s | 203.313 s | 491,840 | 0.606× |
| Parquet | 14,779,976,446 B | 147.80 | — | — | — | — | — | — | 1.000× |
| CSV (estimated) | 80,163,339,161 B | 801.65 | — | — | — | — | — | — | 5.424× |

Acta output SHA-256:
`44a3aaf872cb6cd0318a6537b104ff26214d2738e8c9e7821a7f1bebd10ebad3`.

The retained 1M-row CSV sample is 801,653,457 bytes with SHA-256
`8355799f72ed09d03af458e4456ad8e2057533c809ceebfbb91f316b2d27bea5`.
Full CSV write/read throughput was not measured because the estimated plain CSV
is approximately 74.65 GiB and it does not fit on my computer.
