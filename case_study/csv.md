# CSV vs Acta

CSV is a simple appendable text representation.

## CSV advantages

- Universally supported, human-readable, and easy to debug.
- Rows can be appended with almost no format machinery.
- Suitable for interchange and small datasets.

## Acta advantages

- Declared types eliminate repeated parsing and prevent mixed-type columns.
- Column encodings and compression reduce storage size.
- Per-block time bounds and statistics allow range pruning.
- Readers can decode only requested columns.

## Trade-off

CSV favors simplicity and interoperability. Acta trades readability for smaller files and faster typed analytical access.
