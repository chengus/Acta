# Market deltas benchmark data

The canonical benchmark artifacts are published in the
[Acta benchmark dataset on Hugging Face](https://huggingface.co/datasets/lu-chengass/acta-data):

- `market_deltas/v0.2/deltas.parquet` — original 239,246,024-row input;
- `market_deltas/v0.2/deltas.acta` — recorded Acta v0.2 output;
- `market_deltas/v0.2/benchmark.json` — structured benchmark record;
- `market_deltas/v0.2/conversion.stats.txt` — converter-emitted statistics.

Download the complete benchmark collection with:

```bash
hf download lu-chengass/acta-data \
  --repo-type dataset \
  --revision main \
  --include 'market_deltas/v0.2/*' \
  --local-dir /tmp/acta-hf
```

The Parquet file was supplied directly for this experiment. Its embedded
metadata reports `ClickHouse version 26.4.1` as the writer. It has 238 row
groups, 13 columns, and 239,246,024 rows. Its fields describe market-data
updates; no external provenance or license is embedded in the artifact.

The exact conversion, schema mapping, results, checksums, and validation steps
are documented in [README.md](README.md).
