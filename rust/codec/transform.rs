//! Checked implementations of the v0.2 stream transforms.
//!
//! Fixed-width streams decode straight into the column's own value type. The
//! wide accumulator a transform needs to reconstruct a value is a scalar, not a
//! buffer, so decoding a stream costs one vector of the logical type rather
//! than one vector per intermediate representation.

use crate::error::{Error, ErrorContext, Result};

fn invalid(what: &str, message: impl Into<String>) -> Error {
    Error::corruption(format!("{what}: {}", message.into()), None)
        .with_context(ErrorContext::Payload)
}

fn count_to_usize(count: u64, what: &str) -> Result<usize> {
    usize::try_from(count).map_err(|_| invalid(what, "element count does not fit this platform"))
}

fn checked_byte_count(count: usize, width: usize, what: &str) -> Result<usize> {
    count
        .checked_mul(width)
        .ok_or_else(|| invalid(what, "element byte count overflow"))
}

fn allocate<T>(count: usize, what: &str) -> Result<Vec<T>> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| invalid(what, "unable to allocate decoded values"))?;
    Ok(values)
}

/// Narrow one reconstructed value to the column's logical type.
///
/// A transform may legitimately compute in a wider range than the column can
/// hold; a stream whose values leave that range is describing a column it does
/// not match.
fn narrowed<T: TryFrom<i128>>(value: i128, what: &str) -> Result<T> {
    T::try_from(value)
        .ok()
        .ok_or_else(|| invalid(what, format!("value {value} does not fit the column type")))
}

/// The canonical byte widths of the v0.2 integer, decimal, timestamp, and date
/// types. Section 3 defines no other stored integer width.
const CANONICAL_INTEGER_WIDTHS: [usize; 4] = [1, 2, 4, 8];

/// Decode a fixed-width integer stream into the column's own value type.
pub(crate) fn integer<T: TryFrom<i128>>(
    payload: &[u8],
    count: u64,
    width: usize,
    signed: bool,
    transform: u16,
    what: &str,
) -> Result<Vec<T>> {
    let count = count_to_usize(count, what)?;
    if !CANONICAL_INTEGER_WIDTHS.contains(&width) {
        return Err(invalid(
            what,
            format!("{width} is not a canonical integer width"),
        ));
    }
    match transform {
        0 => raw_integers(payload, count, width, signed, what),
        1 => packed_integers(payload, count, what),
        2 => frame_of_reference(payload, count, width, signed, what),
        3 => delta(payload, count, width, signed, what),
        4 => delta_of_delta(payload, count, width, signed, what),
        _ => Err(invalid(what, "transform does not apply to integer values")),
    }
}

fn raw_integers<T: TryFrom<i128>>(
    payload: &[u8],
    count: usize,
    width: usize,
    signed: bool,
    what: &str,
) -> Result<Vec<T>> {
    let expected = checked_byte_count(count, width, what)?;
    if payload.len() != expected {
        return Err(invalid(
            what,
            format!(
                "raw stream has {} bytes, expected {expected}",
                payload.len()
            ),
        ));
    }
    let mut values = allocate(count, what)?;
    for chunk in payload.chunks_exact(width) {
        values.push(narrowed(read_integer(chunk, signed), what)?);
    }
    Ok(values)
}

fn packed_integers<T: TryFrom<i128>>(payload: &[u8], count: usize, what: &str) -> Result<Vec<T>> {
    let mut values = allocate(count, what)?;
    for value in BitUnpacker::new(payload, count, what)? {
        values.push(narrowed(i128::from(value), what)?);
    }
    Ok(values)
}

/// Section 9.3: one canonical-width block minimum, then packed differences.
fn frame_of_reference<T: TryFrom<i128>>(
    payload: &[u8],
    count: usize,
    width: usize,
    signed: bool,
    what: &str,
) -> Result<Vec<T>> {
    if count == 0 {
        return empty(payload, what);
    }
    let base = read_integer(
        payload
            .get(..width)
            .ok_or_else(|| invalid(what, "frame-of-reference stream has no base value"))?,
        signed,
    );
    let offsets = BitUnpacker::new(&payload[width..], count, what)?;

    let mut values = allocate(count, what)?;
    for offset in offsets {
        // Both terms are bounded by u64, so the sum cannot leave i128.
        values.push(narrowed(base + i128::from(offset), what)?);
    }
    Ok(values)
}

/// Section 9.4: the first canonical-width value, then packed ZigZag deltas.
fn delta<T: TryFrom<i128>>(
    payload: &[u8],
    count: usize,
    width: usize,
    signed: bool,
    what: &str,
) -> Result<Vec<T>> {
    if count == 0 {
        return empty(payload, what);
    }
    let first = read_integer(
        payload
            .get(..width)
            .ok_or_else(|| invalid(what, "delta stream has no first value"))?,
        signed,
    );
    let mut values = allocate(count, what)?;
    values.push(narrowed(first, what)?);
    if count == 1 {
        if payload.len() != width {
            return Err(invalid(what, "single-value delta stream has extra bytes"));
        }
        return Ok(values);
    }

    let mut current = first;
    for encoded in BitUnpacker::new(&payload[width..], count - 1, what)? {
        current = current
            .checked_add(unzigzag(encoded))
            .ok_or_else(|| invalid(what, "delta value overflow"))?;
        values.push(narrowed(current, what)?);
    }
    Ok(values)
}

/// Section 9.5: the first value, the first `int64` delta, then packed ZigZag
/// differences between consecutive deltas.
fn delta_of_delta<T: TryFrom<i128>>(
    payload: &[u8],
    count: usize,
    width: usize,
    signed: bool,
    what: &str,
) -> Result<Vec<T>> {
    if count < 2 {
        return Err(invalid(
            what,
            "delta-of-delta requires at least two elements",
        ));
    }
    let first = read_integer(
        payload
            .get(..width)
            .ok_or_else(|| invalid(what, "delta-of-delta has no first value"))?,
        signed,
    );
    let first_delta_end = width
        .checked_add(8)
        .ok_or_else(|| invalid(what, "offset overflow"))?;
    let first_delta = i64::from_le_bytes(
        payload
            .get(width..first_delta_end)
            .ok_or_else(|| invalid(what, "delta-of-delta has no first delta"))?
            .try_into()
            .map_err(|_| invalid(what, "invalid first delta"))?,
    );

    let mut values = allocate(count, what)?;
    values.push(narrowed(first, what)?);
    let mut delta = i128::from(first_delta);
    let mut current = first
        .checked_add(delta)
        .ok_or_else(|| invalid(what, "first delta overflows the value"))?;
    values.push(narrowed(current, what)?);

    for encoded in BitUnpacker::new(&payload[first_delta_end..], count - 2, what)? {
        delta = delta
            .checked_add(unzigzag(encoded))
            .ok_or_else(|| invalid(what, "delta-of-delta overflow"))?;
        // Section 9.5 requires every intermediate difference to fit in int64.
        i64::try_from(delta)
            .map_err(|_| invalid(what, "delta-of-delta difference does not fit int64"))?;
        current = current
            .checked_add(delta)
            .ok_or_else(|| invalid(what, "delta-of-delta value overflow"))?;
        values.push(narrowed(current, what)?);
    }
    Ok(values)
}

/// A stream with no elements carries no bytes either.
fn empty<T>(payload: &[u8], what: &str) -> Result<Vec<T>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    Err(invalid(what, "an empty stream has bytes"))
}

/// Read one canonical little-endian integer of at most eight stored bytes,
/// sign-extending it from its stored width when the logical type is signed.
fn read_integer(bytes: &[u8], signed: bool) -> i128 {
    let mut raw = [0_u8; 8];
    let width = bytes.len().min(raw.len());
    raw[..width].copy_from_slice(&bytes[..width]);
    if !signed {
        return i128::from(u64::from_le_bytes(raw));
    }
    let unused_bits = (raw.len() - width) * 8;
    if unused_bits == 0 || width == 0 {
        return i128::from(i64::from_le_bytes(raw));
    }
    i128::from((i64::from_le_bytes(raw) << unused_bits) >> unused_bits)
}

fn unzigzag(value: u64) -> i128 {
    let magnitude = i128::from(value >> 1);
    if value & 1 == 0 {
        magnitude
    } else {
        -magnitude - 1
    }
}

/// An LSB-first reader over a bit-packed stream.
///
/// Section 9.2 puts the bit width in the first byte, so the whole stream is
/// measurable before any of it is read: the constructor settles the width, the
/// stored length, and the zero padding of the final byte, and the iterator that
/// follows cannot then leave the payload. Values are yielded one at a time so a
/// caller can narrow them into its own destination rather than holding a buffer
/// of every unpacked word.
pub(crate) struct BitUnpacker<'a> {
    data: &'a [u8],
    width: usize,
    position: usize,
    remaining: usize,
}

impl<'a> BitUnpacker<'a> {
    pub(crate) fn new(payload: &'a [u8], count: usize, what: &str) -> Result<Self> {
        let width = usize::from(
            *payload
                .first()
                .ok_or_else(|| invalid(what, "bit-packed stream has no width byte"))?,
        );
        if width > 64 {
            return Err(invalid(what, format!("bit width {width} exceeds 64")));
        }
        let data = &payload[1..];

        // Section 9.2: width zero is the whole encoding of an all-zero stream.
        if width == 0 {
            if !data.is_empty() {
                return Err(invalid(what, "zero-width stream has data bytes"));
            }
            return Ok(Self {
                data,
                width,
                position: 0,
                remaining: count,
            });
        }

        let bit_count = count
            .checked_mul(width)
            .ok_or_else(|| invalid(what, "bit count overflow"))?;
        let byte_count = bit_count
            .checked_add(7)
            .ok_or_else(|| invalid(what, "packed byte count overflow"))?
            / 8;
        if data.len() != byte_count {
            return Err(invalid(
                what,
                format!(
                    "bit-packed stream has {} data bytes, expected {byte_count}",
                    data.len()
                ),
            ));
        }
        if bit_count % 8 != 0 && data[byte_count - 1] & (u8::MAX << (bit_count % 8)) != 0 {
            return Err(invalid(what, "unused high bits are not zero"));
        }

        Ok(Self {
            data,
            width,
            position: 0,
            remaining: count,
        })
    }
}

impl Iterator for BitUnpacker<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        self.remaining = self.remaining.checked_sub(1)?;
        if self.width == 0 {
            return Some(0);
        }
        let mut value = 0_u64;
        for bit in 0..self.width {
            let source = self.position + bit;
            if self.data[source / 8] & (1 << (source % 8)) != 0 {
                value |= 1_u64 << bit;
            }
        }
        self.position += self.width;
        Some(value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

/// Decode a bit-packed stream whose values are needed all at once, as
/// dictionary indices and run lengths are.
pub(crate) fn bit_unpack(payload: &[u8], count: usize, what: &str) -> Result<Vec<u64>> {
    let unpacker = BitUnpacker::new(payload, count, what)?;
    let mut values = allocate(count, what)?;
    values.extend(unpacker);
    Ok(values)
}

/// Decode raw, bit-packed, or boolean-RLE boolean values.
pub(crate) fn booleans(
    payload: &[u8],
    count: u64,
    transform: u16,
    what: &str,
) -> Result<Vec<bool>> {
    let count = count_to_usize(count, what)?;
    match transform {
        0 => raw_booleans(payload, count, what),
        1 => packed_booleans(payload, count, what),
        6 => boolean_rle(payload, count, what),
        _ => Err(invalid(what, "transform does not apply to booleans")),
    }
}

/// Section 9.1: raw boolean data is LSB-first bit packing without a width byte.
fn raw_booleans(payload: &[u8], count: usize, what: &str) -> Result<Vec<bool>> {
    let expected = count
        .checked_add(7)
        .ok_or_else(|| invalid(what, "boolean byte count overflow"))?
        / 8;
    if payload.len() != expected {
        return Err(invalid(what, "raw boolean stream has the wrong length"));
    }
    if count % 8 != 0 && payload[expected - 1] & (u8::MAX << (count % 8)) != 0 {
        return Err(invalid(what, "unused boolean bits are not zero"));
    }

    let mut values = allocate(count, what)?;
    for index in 0..count {
        values.push(payload[index / 8] & (1 << (index % 8)) != 0);
    }
    Ok(values)
}

fn packed_booleans(payload: &[u8], count: usize, what: &str) -> Result<Vec<bool>> {
    let mut values = allocate(count, what)?;
    for value in BitUnpacker::new(payload, count, what)? {
        values.push(match value {
            0 => false,
            1 => true,
            _ => return Err(invalid(what, "bit-packed boolean is not 0 or 1")),
        });
    }
    Ok(values)
}

/// Section 9.7: a run count, LSB-first run value bits, then packed run lengths.
fn boolean_rle(payload: &[u8], count: usize, what: &str) -> Result<Vec<bool>> {
    let run_count = u32::from_le_bytes(
        payload
            .get(..4)
            .ok_or_else(|| invalid(what, "boolean RLE has no run count"))?
            .try_into()
            .map_err(|_| invalid(what, "invalid boolean RLE run count"))?,
    );
    let run_count = usize::try_from(run_count)
        .map_err(|_| invalid(what, "boolean RLE run count does not fit this platform"))?;
    if run_count > count {
        return Err(invalid(what, "boolean RLE has more runs than elements"));
    }
    if run_count == 0 {
        if count != 0 || payload.len() != 4 {
            return Err(invalid(
                what,
                "empty boolean RLE does not match the element count",
            ));
        }
        return Ok(Vec::new());
    }

    let value_bytes = run_count
        .checked_add(7)
        .ok_or_else(|| invalid(what, "boolean RLE value count overflow"))?
        / 8;
    let value_end = 4usize
        .checked_add(value_bytes)
        .ok_or_else(|| invalid(what, "boolean RLE offset overflow"))?;
    let run_values = payload
        .get(4..value_end)
        .ok_or_else(|| invalid(what, "boolean RLE values are truncated"))?;
    let lengths = BitUnpacker::new(
        payload
            .get(value_end..)
            .ok_or_else(|| invalid(what, "boolean RLE lengths are missing"))?,
        run_count,
        what,
    )?;

    let mut values = allocate(count, what)?;
    let mut total = 0usize;
    for (index, length) in lengths.enumerate() {
        let length = usize::try_from(length)
            .map_err(|_| invalid(what, "boolean RLE run length does not fit this platform"))?;
        if length == 0 {
            return Err(invalid(what, "boolean RLE contains a zero-length run"));
        }
        total = total
            .checked_add(length)
            .filter(|total| *total <= count)
            .ok_or_else(|| invalid(what, "boolean RLE lengths exceed the element count"))?;
        let value = run_values[index / 8] & (1 << (index % 8)) != 0;
        values.extend(std::iter::repeat_n(value, length));
    }
    if total != count {
        return Err(invalid(
            what,
            "boolean RLE lengths do not sum to the element count",
        ));
    }
    Ok(values)
}

/// Restore canonical little-endian values from byte-stream split storage.
pub(crate) fn byte_stream_split(
    payload: &[u8],
    count: u64,
    width: usize,
    what: &str,
) -> Result<Vec<u8>> {
    let count = count_to_usize(count, what)?;
    let expected = checked_byte_count(count, width, what)?;
    if payload.len() != expected {
        return Err(invalid(
            what,
            "byte-stream-split payload has the wrong length",
        ));
    }
    let mut restored = Vec::new();
    restored
        .try_reserve_exact(expected)
        .map_err(|_| invalid(what, "unable to allocate byte-stream-split values"))?;
    restored.resize(expected, 0);
    for value in 0..count {
        for byte in 0..width {
            restored[value * width + byte] = payload[byte * count + value];
        }
    }
    Ok(restored)
}

/// Decode an unsigned 32-bit lengths stream.
pub(crate) fn lengths(payload: &[u8], count: u64, transform: u16, what: &str) -> Result<Vec<u32>> {
    integer(payload, count, 4, false, transform, what)
}

#[cfg(test)]
mod tests {
    use super::{BitUnpacker, booleans, byte_stream_split, integer};

    fn unpack(payload: &[u8], count: usize) -> Vec<u64> {
        BitUnpacker::new(payload, count, "test")
            .expect("valid packed stream")
            .collect()
    }

    #[test]
    fn bit_packed_values_are_lsb_first() {
        assert_eq!(unpack(&[3, 0x22], 2), vec![2, 4]);
    }

    #[test]
    fn integer_transforms_restore_values() {
        let mut delta = 10_i64.to_le_bytes().to_vec();
        delta.extend_from_slice(&[3, 0x22]);
        assert_eq!(
            integer::<i64>(&delta, 3, 8, true, 3, "test").expect("valid delta"),
            vec![10, 11, 13]
        );

        let mut frame_of_reference = 10_i64.to_le_bytes().to_vec();
        frame_of_reference.extend_from_slice(&[2, 0x34]);
        assert_eq!(
            integer::<i64>(&frame_of_reference, 3, 8, true, 2, "test")
                .expect("valid frame of reference"),
            vec![10, 11, 13]
        );
    }

    #[test]
    fn narrow_signed_values_are_sign_extended_from_their_stored_width() {
        assert_eq!(
            integer::<i8>(&[0xff, 0xfe], 2, 1, true, 0, "test").expect("valid int8 values"),
            vec![-1, -2]
        );
    }

    #[test]
    fn a_value_wider_than_its_column_is_rejected() {
        let error = integer::<i8>(&[8, 0x80], 1, 1, false, 1, "test")
            .expect_err("128 does not fit an int8");

        assert_eq!(error.kind(), crate::ErrorKind::Corruption);
    }

    #[test]
    fn a_width_the_type_system_does_not_have_is_rejected() {
        let error =
            integer::<i64>(&[0; 9], 3, 3, true, 0, "test").expect_err("width 3 is not canonical");

        assert_eq!(error.kind(), crate::ErrorKind::Corruption);
    }

    #[test]
    fn boolean_transforms_restore_values() {
        assert_eq!(
            booleans(&[0b0000_0101], 3, 0, "test").expect("valid raw booleans"),
            vec![true, false, true]
        );
        assert_eq!(
            booleans(&[2, 0, 0, 0, 0b0000_0001, 2, 0b0000_1001], 3, 6, "test",)
                .expect("valid boolean RLE"),
            vec![true, false, false]
        );
    }

    #[test]
    fn byte_stream_split_restores_canonical_order() {
        assert_eq!(
            byte_stream_split(&[1, 3, 2, 4], 2, 2, "test").expect("valid split"),
            vec![1, 2, 3, 4]
        );
    }
}
