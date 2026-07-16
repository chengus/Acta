# Format comparisons

Acta occupies the space between an append-friendly log and an analytical columnar file. These comparisons describe design trade-offs, not benchmark results.

| Format | Advantages over Acta | Disadvantages relative to Acta |
| --- | --- | --- |
| [Parquet](parquet.md) | Mature ecosystem; excellent analytical compression; broad query-engine support | Appending usually creates new files or rewrites metadata; awkward as one continuously growing file |
| [CSV](csv.md) | Universal, human-readable, trivial to produce and stream | Weak typing, poor compression and pruning, expensive parsing |
| [SQLite](sqlite.md) | Transactions, indexes, updates, recovery, and a mature SQL engine | Single-writer serialization and row/page-oriented storage are a weaker fit for compressed analytical time series |
| [Time-series database](timeseries_database.md) | Full ingestion, indexing, retention, query, replication, and operational tooling | Requires a running system and carries substantially more operational and storage complexity |
| [JSON Lines](json_lines.md) | Flexible schema, easy streaming, readable records | Repeats field names, permits mixed types, and offers little native range pruning |
| [Arrow IPC](arrow_ipc.md) | Extremely fast interchange and near-zero-copy analytical reads | Primarily an interchange/memory format; compression and durable incremental append are not its central model |

Choose Acta when a portable, single append-only file with typed compression and time-range pruning matters more than transactions, mutation, or a complete database service.
