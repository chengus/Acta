# Arrow IPC vs Acta

Arrow IPC serializes typed columnar record batches using the Apache Arrow memory model.

## Arrow IPC advantages

- Broad analytical ecosystem and efficient transfer between processes and tools.
- Near-zero-copy reads where the representation and platform permit them.
- Rich, standardized type system.
- Two variants cover complementary needs: the **stream** format accepts an arbitrary number of record batches written sequentially, and the **file** format adds a footer of batch offsets for random access.

## Arrow IPC disadvantages

- Neither variant covers both needs at once: the stream format has no random access, and the file format requires a finalized footer over a fixed set of batches, so it isn't naturally appendable after the fact.
- No native time-series index — pruning is limited to record-batch offsets, not timestamp ranges or column statistics.
- Not designed for concurrent producers writing into the same file over time.
- Because it prioritizes a layout usable directly by CPUs (wide, aligned buffers for near-zero-copy reads), it doesn't apply storage-oriented encodings like delta-of-delta timestamps, bit-packing, or run-length encoding, so on-disk density is lower than a format built for storage.

## Acta advantages

- Durable append behavior and time-range discovery are primary design concerns.
- Blocks can select storage-oriented encodings and compression independently.
- File metadata is specialized for pruning time-series blocks written out of timestamp order.
- Combines the appendability of the Arrow stream format with the random access of the Arrow file format, plus time-series-specific indexing and compression neither variant offers.

## Trade-off

Arrow IPC is preferable for interoperable, high-speed data exchange. Acta targets long-lived, concurrently-appended time-series storage — combining stream-like appendability, file-like random access, and Parquet-like compression, accepting extra decode cost in place of Arrow's near-zero-copy layout. Arrow could still serve as an in-memory intermediate: producers build an Arrow `RecordBatch`, which Acta then encodes into its own compressed on-disk blocks, avoiding the need to design an entire in-memory column representation while keeping the on-disk format distinct.
