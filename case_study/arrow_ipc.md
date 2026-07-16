# Arrow IPC vs Acta

Arrow IPC serializes typed columnar record batches using the Apache Arrow memory model.

## Arrow IPC advantages

- Broad analytical ecosystem and efficient transfer between processes and tools.
- Near-zero-copy reads where the representation and platform permit them.
- Rich, standardized type system.

## Acta advantages

- Durable append behavior and time-range discovery are primary design concerns.
- Blocks can select storage-oriented encodings and compression independently.
- File metadata is specialized for pruning time-series blocks written out of timestamp order.

## Trade-off

Arrow IPC is preferable for interoperable, high-speed data exchange. Acta targets long-lived append-only storage, potentially using similar typed columnar ideas with a more storage-specific layout.
