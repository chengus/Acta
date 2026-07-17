# Acta file format v0.1

Status: initial experimental design. Files written against this document are
not promised forward compatibility until the format reaches v1.

## 1. Scope

Acta is an append-only, strongly typed, block-columnar file for time-series
data. Version 0.1 defines:

- one fixed schema per file;
- one required primary timestamp column;
- independently encoded and compressed column streams;
- sequential discovery without a mutable file footer;
- recovery to the last complete frame after an interrupted append; and
- checksums suitable for projected reads and full-file validation.

Transactions, schema evolution, updates, deletion, encryption, parity repair,
and multi-writer coordination are outside this version. A writer may buffer or
synchronize frames according to its durability policy; that policy does not
change their representation.

## 2. Conventions

- All integers are little-endian.
- Signed integers use two's-complement representation.
- Floating-point values use IEEE 754 binary32 or binary64 representation.
- Sizes and offsets are unsigned and measured in bytes unless stated otherwise.
- Offsets within a frame header are relative to the first byte of that header.
- Stream payload offsets are relative to the first byte of the frame payload.
- Reserved fields and alignment padding MUST be zero when written and ignored
  when read unless a later compatible version assigns them meaning.
- CRC fields use CRC32C (Castagnoli), not the IEEE CRC-32 polynomial.
- A CRC covers the bytes exactly as stored, including zero padding when the
  covered region includes it.
- `UINT64_MAX` means an unavailable optional `uint64` value.

The reference block target is 65,536 rows. Writers SHOULD also impose a byte
target so a block containing large strings or binary values does not become
unreasonably large. Readers MUST validate all length arithmetic for overflow
and SHOULD apply configurable resource limits before allocating memory.

## 3. Logical type system

Type IDs are stable within format major version 0.

| ID | Logical type | Canonical value representation | Parameters |
| ---: | --- | --- | --- |
| 1 | `bool` | one logical bit | none |
| 2 | `int8` | signed 8-bit integer | none |
| 3 | `int16` | signed 16-bit integer | none |
| 4 | `int32` | signed 32-bit integer | none |
| 5 | `int64` | signed 64-bit integer | none |
| 6 | `uint8` | unsigned 8-bit integer | none |
| 7 | `uint16` | unsigned 16-bit integer | none |
| 8 | `uint32` | unsigned 32-bit integer | none |
| 9 | `uint64` | unsigned 64-bit integer | none |
| 10 | `float32` | IEEE 754 binary32 bits | none |
| 11 | `float64` | IEEE 754 binary64 bits | none |
| 12 | `decimal64` | signed 64-bit unscaled integer | precision and scale |
| 13 | `timestamp64` | signed 64-bit count from Unix epoch | unit and timezone |
| 14 | `utf8` | UTF-8 byte sequence | none |
| 15 | `categorical` | UTF-8 byte sequence | ordered flag |
| 16 | `binary` | arbitrary byte sequence | none |
| 17 | `fixed_binary` | fixed-width byte sequence | byte width |

`NULL` is not a separate stored type. Nullability is a column property, and a
nullable block carries a validity stream. Values are dense: the values streams
contain only non-null values, in row order. A validity bit of one means present;
zero means null. This makes an all-null block require no values stream.

### 3.1 Type parameters

- `decimal64`: `precision` is `uint16`; `scale` is `int16`. A logical value is
  `unscaled × 10^(-scale)`. Precision MUST be between 1 and 18.
- `timestamp64`: unit is `0=second`, `1=millisecond`, `2=microsecond`, or
  `3=nanosecond`. Timezone mode is `0=naive`, `1=UTC`, or `2=IANA name`.
  Zoned values are stored as UTC epoch counts. An IANA name is descriptive
  schema metadata and does not change the stored count.
- `categorical`: `ordered` is `0` or `1`. Dictionaries remain block-local;
  dictionary indices have no meaning outside their block.
- `fixed_binary`: width is an unsigned 32-bit integer greater than zero.

UTF-8 column names, timezone names, and `utf8` or `categorical` values MUST be
well-formed UTF-8. Readers MUST preserve exact floating-point bits, including
signed zero and NaN payloads. Numeric min/max statistics ignore NaNs; an all-NaN
column has no min/max statistic.

## 4. Top-level grammar

```text
file := prologue schema_frame data_frame*
```

There is no required file footer. Every frame is self-delimiting and ends in a
commit trailer. A reader discovers new blocks by continuing from the byte after
the last complete frame.

Frame type IDs are:

| ID | Frame |
| ---: | --- |
| 1 | schema |
| 2 | data block |
| 3 | checkpoint, reserved for a later minor version |

Frame sequence number zero belongs to the schema frame. Data frames use
contiguous sequence numbers beginning at one. Missing or repeated sequence
numbers are corruption in strict mode.

## 5. File prologue

The prologue is exactly 64 bytes.

| Offset | Size | Field | v0.1 value |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `41 43 54 41 0d 0a 1a 0a` |
| 8 | 2 | format major | `0` |
| 10 | 2 | format minor | `1` |
| 12 | 4 | prologue size | `64` |
| 16 | 8 | feature flags | bit 0 enables implicit row IDs |
| 24 | 16 | file ID | opaque writer-generated bytes |
| 40 | 8 | schema frame offset | `64` |
| 48 | 12 | reserved | zero |
| 60 | 4 | prologue CRC32C | bytes `[0, 60)` |

Feature flag bit 0 is `ROW_IDS`; all other bits are zero in v0.1. Readers MUST
reject an unknown major version, unknown feature bits, a bad CRC, or a schema
offset other than 64. The file ID is identity metadata, not a cryptographic
content hash.

## 6. Generic frame

```text
frame := prefix header payload trailer
```

Header and payload lengths MUST be multiples of eight. Frame-specific header
content and every payload stream are padded with zeros to the next eight-byte
boundary. The stored stream length excludes padding; the frame payload length
includes it.

### 6.1 Prefix

The prefix is exactly 48 bytes.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | magic: ASCII `ACTAFRM\n` |
| 8 | 2 | frame type |
| 10 | 2 | frame version, `0` |
| 12 | 4 | frame flags, `0` in v0.1 |
| 16 | 4 | header length |
| 20 | 4 | reserved |
| 24 | 8 | payload length |
| 32 | 8 | sequence number |
| 40 | 4 | header CRC32C |
| 44 | 4 | prefix CRC32C over bytes `[0, 44)` |

The prefix CRC MUST be validated before trusting either length. The header CRC
covers the complete padded header.

### 6.2 Trailer

The trailer is exactly 32 bytes and is written after the frame body.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | total frame length |
| 8 | 8 | repeated sequence number |
| 16 | 4 | body CRC32C |
| 20 | 4 | trailer CRC32C |
| 24 | 8 | commit magic: ASCII `ACTAEND\n` |

The total length is `48 + header_length + payload_length + 32`. The body CRC
covers the stored prefix, header, and payload. The trailer CRC covers trailer
bytes `[0, 20)` followed by bytes `[24, 32)`, excluding only itself.

A frame is structurally complete when its prefix, lengths, header CRC, matching
trailer fields, commit magic, and trailer CRC are valid. Strict validation also
checks the body CRC. A metadata-only open MAY defer the body CRC and instead
validate individual stream CRCs when those streams are read.

“Complete” and “durable” are deliberately distinct. Durability begins only
after a writer synchronization covering the complete frame has returned.

## 7. Schema frame

The schema frame has sequence zero and a 24-byte frame-specific header:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | schema ID, nonzero |
| 8 | 4 | column count |
| 12 | 4 | primary timestamp column ID |
| 16 | 4 | schema flags, zero in v0.1 |
| 20 | 4 | reserved |

Its payload is a sequence of column descriptors. Each descriptor begins on an
eight-byte boundary.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | descriptor length including padding |
| 4 | 4 | column ID |
| 8 | 2 | logical type ID |
| 10 | 2 | column flags |
| 12 | 4 | UTF-8 name length |
| 16 | 4 | type-parameter length |
| 20 | 4 | reserved |
| 24 | variable | name, then type parameters, then zero padding |

Column IDs MUST be unique and nonzero. Names MUST be unique. Column flag bit 0
means nullable; all other bits are zero. Exactly one column ID MUST match the
primary timestamp ID, and that column MUST have type `timestamp64`.

Type-parameter records are:

- `decimal64`: `<uint16 precision, int16 scale, uint32 reserved>`.
- `timestamp64`: `<uint8 unit, uint8 timezone_mode, uint16 reserved,
  uint32 timezone_name_length>`, followed by the name and padding.
- `categorical`: `<uint8 ordered, 7 reserved bytes>`.
- `fixed_binary`: `<uint32 byte_width, uint32 reserved>`.
- Types without parameters have a zero parameter length.

## 8. Data frame header

The first 64 header bytes are the block header.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | schema ID |
| 8 | 8 | base row ID or `UINT64_MAX` |
| 16 | 4 | row count |
| 20 | 4 | column count |
| 24 | 8 | primary timestamp minimum |
| 32 | 8 | primary timestamp maximum |
| 40 | 4 | column table offset |
| 44 | 4 | stream table offset |
| 48 | 4 | statistics area offset |
| 52 | 4 | statistics area length |
| 56 | 4 | block flags |
| 60 | 4 | reserved |

The schema ID and column count MUST match the schema frame. Timestamp bounds
are calculated over non-null values. The primary timestamp column MUST be
non-nullable, so every nonempty block has valid bounds. An empty data block is
invalid. Block flag bit 0 means row IDs are enabled; all other bits are zero.
The block flag MUST match the file's `ROW_IDS` feature. When enabled, the first
data block has base row ID zero and every subsequent base equals the preceding
base plus its row count. When disabled, every base row ID is `UINT64_MAX`.

The column table offset MUST be 64. The stream table immediately follows the
column table at `64 + 32 × column_count`. The statistics offset MUST be at or
after the stream table, and the difference MUST be a multiple of 48; this
quotient is the total stream count. The statistics area MUST lie entirely in
the padded frame header. These equalities deliberately leave no untyped gaps
between header tables.

### 8.1 Column descriptor

There is one 32-byte descriptor per schema column, sorted by column ID.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | column ID |
| 4 | 2 | column layout |
| 6 | 2 | column flags |
| 8 | 4 | null count |
| 12 | 4 | dense value count |
| 16 | 4 | first stream-table index |
| 20 | 2 | stream count |
| 22 | 2 | statistics kind |
| 24 | 4 | statistics offset |
| 28 | 4 | statistics length |

Dense value count MUST equal `row_count - null_count`. Column flag bit 0 means
the validity representation is implicit (`all_valid` or `all_null`); bit 1
means min/max statistics are present. Statistics offsets are relative to the
start of the frame header and must lie in the statistics area.
Non-nullable columns MUST have null count zero and no validity stream. For an
implicit nullable column, null count zero means all-valid and null count equal
to row count means all-null; other null counts require an explicit validity
stream.

Column layouts are:

| ID | Layout | Required logical streams |
| ---: | --- | --- |
| 0 | plain | values; variable-width types also have lengths |
| 1 | constant | one value; variable-width types also have one length |
| 2 | dictionary | dictionary values, optional dictionary lengths, indices |
| 3 | run length | run values, optional run-value lengths, run lengths |

A nullable column that is neither all-valid nor all-null additionally has one
validity stream. An all-null column has no value streams and uses plain layout.
Boolean values or validity may alternatively use one boolean-RLE stream under
plain layout; generic run-length layout uses separate run-values and
run-lengths streams.

### 8.2 Stream descriptor

Each physical stream has a 48-byte descriptor.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 2 | stream kind |
| 2 | 2 | transform |
| 4 | 2 | compression codec |
| 6 | 2 | stream flags, zero in v0.1 |
| 8 | 8 | payload offset |
| 16 | 8 | stored length |
| 24 | 8 | transformed length before compression |
| 32 | 8 | element count |
| 40 | 4 | CRC32C of stored bytes |
| 44 | 4 | reserved |

Stream kinds are `1=validity`, `2=values`, `3=lengths`,
`4=dictionary_values`, `5=dictionary_lengths`, `6=indices`,
`7=run_values`, and `8=run_lengths`.

Every stream begins at an eight-byte-aligned payload offset. Stream ranges MUST
not overlap. A reader performing projection reads and validates only required
streams. The body CRC remains available for a full integrity scan.

## 9. Stream transforms

Transform IDs are:

| ID | Transform | Applicable data |
| ---: | --- | --- |
| 0 | raw | every physical type |
| 1 | bit packed | booleans and nonnegative integers |
| 2 | frame of reference | integers, decimals, timestamps |
| 3 | delta | integers, decimals, timestamps |
| 4 | delta of delta | integers and timestamps |
| 5 | byte-stream split | floats and fixed binary |
| 6 | boolean RLE | validity and boolean values |

### 9.1 Raw

Fixed-width values are concatenated in canonical little-endian representation.
Booleans are never byte-per-value: raw boolean data uses LSB-first bit packing.
Variable-width values use an unsigned 32-bit lengths stream and a concatenated
byte-values stream. Individual values therefore have a v0.1 maximum length of
`UINT32_MAX`.

### 9.2 Bit packing

The transformed stream begins with one `uint8` bit width followed by values
concatenated LSB-first. Unused high bits in the final byte are zero. Width zero
represents an all-zero stream and is still encoded as the single width byte
`00`, with no following bit bytes. Dictionary indices and run lengths use this
transform.

### 9.3 Frame of reference

The transformed stream stores one canonical-width base value followed by a
bit-packed stream of `value - base`. The base is the block minimum. This
transform is valid only when every difference is representable as `uint64`.

### 9.4 Delta

The transformed stream stores the first canonical-width value followed by
bit-packed ZigZag deltas. For signed `x`, ZigZag is
`(uint64(x) << 1) XOR uint64(x >> 63)`. Every adjacent difference MUST fit in
`int64`.

### 9.5 Delta of delta

The stream stores the first canonical-width value, the first delta as `int64`,
then bit-packed ZigZag differences between consecutive deltas. It requires at
least two elements and all intermediate differences to fit in `int64`.

### 9.6 Byte-stream split

For width `W` and `N` values, output contains all byte-zero values, then all
byte-one values, through byte `W-1`. Bytes retain canonical little-endian order
within each value before transposition.

### 9.7 Boolean RLE

The stream contains `uint32 run_count`, `ceil(run_count / 8)` LSB-first run
value bits, then bit-packed positive run lengths. Run lengths MUST sum to the
stream element count.

## 10. Compression

Codec IDs are `0=none` and `1=Zstandard`. Zstandard streams are independent
standard frames without a preset dictionary. Compression is applied after the
transform and before CRC calculation.

The codec decision is local to each stream. Writers SHOULD use `none` unless
compression saves enough bytes to cover its CPU and eight bytes of descriptor
bookkeeping. The initial reference heuristic requires at least eight stored
bytes saved; production tuning may use a larger percentage threshold without
changing the format.

## 11. Statistics

Statistics kind zero means none; kind one means canonical min followed by max.
Fixed-width statistics use the logical type's canonical width. Variable-width
min/max statistics are not written in v0.1. Nulls and floating NaNs are ignored.
Boolean min and max are each stored as one byte with value zero or one.

The primary timestamp min/max in the block header is the complete mandatory
pruning statistic; it need not be repeated in that column's optional statistics
area. Implementations MAY write kind-one statistics for other numeric, decimal,
timestamp, boolean, or fixed-binary columns.

## 12. Encoding selection

Encoding selection is per column per block. It does not alter the schema.

| Logical type | v0.1 candidates | Expected benefit |
| --- | --- | --- |
| `bool` | constant, bit packed, boolean RLE | flags, long runs |
| signed/unsigned integer | constant, raw, FOR, delta, delta-of-delta, dictionary, RLE | small ranges, counters, repeated codes |
| `decimal64` | integer candidates | repeated currency values and bounded ranges |
| `timestamp64` | raw, FOR, delta, delta-of-delta | ordered or regular event time |
| float | constant, raw, dictionary, RLE, byte-stream split | repeated values or smooth signals before Zstandard |
| `utf8`/`binary` | constant, plain, dictionary, RLE | repeated messages, routes, identifiers |
| `categorical` | constant, dictionary, RLE; plain fallback | low-cardinality labels |
| `fixed_binary` | constant, raw, dictionary, RLE, byte-stream split | structured fixed fields; random IDs remain raw |
| validity | all-valid, all-null, bit packed, boolean RLE | sparse or bursty nulls |

Writers SHOULD collect min/max, run count, monotonicity, delta widths, and a
capped distinct-value table while buffering. They SHOULD estimate every cheap
candidate, compress raw plus at most the two best estimates, and retain the
smallest stored result. This avoids the brute-force cost of fully materializing
and compressing every candidate.

A specialized encoding SHOULD beat raw plus its codec by at least the larger of
64 bytes or 1% after stream metadata; otherwise raw is preferred. This keeps
decode complexity from changing for insignificant savings.

XOR/Gorilla float encoding is intentionally excluded from v0.1. The initial
probe found dictionary or raw best for taxi floats and byte-stream split best
for a smooth noisy signal. A production-quality Gorilla implementation can be
added under a new transform ID after it demonstrates a consistent advantage.

## 13. Reader and recovery behavior

### 13.1 Normal discovery

1. Validate the prologue and schema frame.
2. At the next frame offset, read and validate the 48-byte prefix.
3. Check sizes, limits, overflow, and file bounds.
4. Read and validate the frame header and trailer.
5. Add data-block metadata to the in-memory index.
6. Advance by total frame length and repeat.
7. If EOF occurs inside the next frame, retain the current offset and retry when
   the file grows.

### 13.2 Recovery open

Recovery mode validates each body CRC. A missing or incomplete final frame is
an interrupted tail and is ignored. The byte after the last valid frame is the
`last_good_offset`; a repair tool may truncate the file there.

A checksum failure in the final frame may also be discarded as a corrupt tail
after explicit user approval. A checksum failure before a later valid frame is
mid-file corruption and MUST be reported by strict readers.

Salvage mode may search on eight-byte boundaries for `ACTAFRM\n`, but a candidate
is accepted only after its prefix CRC, bounded lengths, header CRC, trailer,
sequence number, and body CRC all validate. Salvage reports the skipped byte
range and never silently presents a gap as a complete dataset.

Checksums detect corruption; they do not reconstruct lost values. Repair by
parity, replication, or backups is outside the file format.

## 14. Deferred extensions

The following are reserved for later versions rather than partially specified:

- checkpoint frames for faster opening of files with very many blocks;
- schema evolution and additional logical types;
- cryptographic authentication or encryption;
- global dictionaries;
- XOR/Gorilla float encoding;
- deletion and update generations; and
- writer-locking or extent-reservation protocols.

Checkpoint frames must remain optional acceleration structures. Blocks and
their checksums remain authoritative so a missing checkpoint never prevents a
full forward scan.
