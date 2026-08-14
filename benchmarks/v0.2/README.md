# Acta v0.2 benchmarks

These benchmarks exercise the current Rust implementation against complete,
typed datasets and write actual Acta v0.2 files. They complement the historical
[v0.1 encoding studies](../v0.1/README.md) and the focused stage benchmarks.

| ID | Dataset | Focus |
| --- | --- | --- |
| [0](0_bts_flight/README.md) | BTS flight records | Typed Parquet-to-Acta conversion, compression, throughput, checksums, and full-file validation |
| [1](1_market_deltas/README.md) | Market data deltas | Fixed-schema, non-adaptive Parquet-to-Acta conversion at 239 million rows |

Each entry contains the exact acquisition, conversion, verification, and
recorded-result instructions.

The canonical large inputs and outputs are published in the
[Acta benchmark dataset on Hugging Face](https://huggingface.co/datasets/lu-chengass/acta-data).
The source for its combined dataset card is [HF_DATASET_CARD.md](HF_DATASET_CARD.md).
