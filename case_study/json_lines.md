# JSON Lines vs Acta

JSON Lines stores one self-contained JSON value per line and is naturally appendable.

## JSON Lines advantages

- Human-readable and easy to stream or recover record by record.
- Flexible records tolerate evolving or sparse fields.
- Supported by most programming environments.

## Acta advantages

- Fixed schemas avoid repeating field names and type tags.
- Columnar blocks compress repeated values efficiently.
- Invalid types are rejected during ingestion rather than discovered during queries.
- Block metadata supports column and time-range pruning.

## Trade-off

JSON Lines is better for flexible event logs. Acta is better when the schema is known and compact, predictable analytical reads are more important than flexibility.
