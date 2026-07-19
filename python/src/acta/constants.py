"""Binary constants and struct layouts for the Acta v0.1 format.

Struct field orders mirror spec/v0.1/format_v0.1.md and are shared with the
executable framing probe.
"""

from __future__ import annotations

import struct

import google_crc32c

FILE_MAGIC = b"ACTA\r\n\x1a\n"
FRAME_MAGIC = b"ACTAFRM\n"
COMMIT_MAGIC = b"ACTAEND\n"

FORMAT_MAJOR = 0
FORMAT_MINOR = 1

UINT64_MAX = (1 << 64) - 1

# magic, major, minor, prologue size, feature flags, file ID, schema offset,
# reserved, CRC32C
PROLOGUE = struct.Struct("<8sHHIQ16sQ12sI")
# magic, frame type, frame version, frame flags, header length, reserved,
# payload length, sequence, header CRC32C, prefix CRC32C
PREFIX = struct.Struct("<8sHHIIIQQII")
# total length, repeated sequence, body CRC32C, trailer CRC32C, commit magic
TRAILER = struct.Struct("<QQII8s")
# schema ID, column count, primary timestamp column ID, schema flags, reserved
SCHEMA_HEADER = struct.Struct("<QIIII")
# descriptor length, column ID, logical type, column flags, name length,
# parameter length, reserved
COLUMN_SCHEMA = struct.Struct("<IIHHIII")
# schema ID, base row ID, row count, column count, timestamp min, timestamp
# max, column table offset, stream table offset, statistics offset,
# statistics length, block flags, reserved
BLOCK_HEADER = struct.Struct("<QQIIqqIIIIII")
# column ID, layout, flags, null count, dense count, first stream index,
# stream count, statistics kind, statistics offset, statistics length
COLUMN_DESCRIPTOR = struct.Struct("<IHHIIIHHII")
# kind, transform, codec, flags, payload offset, stored length, transformed
# length, element count, CRC32C, reserved
STREAM_DESCRIPTOR = struct.Struct("<HHHHQQQQII")


def crc32c(data: bytes) -> int:
    return google_crc32c.value(data)


def pad8(data: bytes) -> bytes:
    return data + bytes((-len(data)) % 8)
