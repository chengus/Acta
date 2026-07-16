# Time-series databases vs Acta

This category includes server-based systems built specifically for time-series ingestion and queries. Individual products differ substantially.

## Time-series database advantages

- Query languages, indexes, retention policies, downsampling, and continuous aggregates.
- Replication, access control, monitoring, and distributed ingestion may be built in.
- Better suited to shared production services and complex operational queries.

## Acta advantages

- A portable file instead of a running service.
- Simple deployment, copying, archiving, and embedding.
- Predictable append-only layout with no background database maintenance requirement.
- Lower conceptual and operational overhead for local capture or interchange.

## Trade-off

A time-series database is a complete data system; Acta is only a storage format. Acta fits embedded capture and portable archives, while a database fits managed, multi-user workloads.
