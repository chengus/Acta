# TSBS IoT 10M target-format benchmark

> The write timer starts after the input Parquet is fully decoded and materialized.
> It excludes TSBS generation, source parsing, source decoding, and compilation.
> Parquet reads use typed Arrow decoding; CSV reads count lines without parsing fields.

| format | output bytes | bytes/row | write s | write rows/s | logical MiB/s | finish s | read s | read rows/s | peak RSS MiB | SHA-256 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| acta | 118,508,440 | 11.85 | 22.459 | 445,253 | 101.31 | 0.078 | 5.635 | 1,774,506 | 1834.6 | `9ca97fbb121c21a4561f74a626538e0518e0d028bfcc55a19dc7dbff59807d23` |
| parquet | 153,331,317 | 15.33 | 4.507 | 2,218,980 | 504.90 | 0.016 | 1.136 | 8,801,581 | 1781.3 | `c062ec900c48e2554f61495d7106791298b0f943a39f6082f5a0063710cb38ee` |
| csv | 993,362,400 | 99.34 | 6.214 | 1,609,290 | 366.17 | 0.007 | 0.472 | 21,184,722 | 1725.2 | `bed124281d1fb22d69e202f6eeb778d6d731a61109fe09dc9d0ae0453a7a919a` |
