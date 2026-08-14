//! Writer-side profiling and v0.2 candidate serialization.
//!
//! This module deliberately knows nothing about frames, offsets, checksums, or
//! codecs. It turns one dense logical value stream into deterministic physical
//! stream bytes. The writer prices and compresses the returned streams before
//! putting any of them in a frame.
//!
//! Dense values are held in flat buffers rather than one allocation per value,
//! and every fact a candidate needs is derived in one pass and kept as a small
//! scalar. Profiling a block therefore costs a small multiple of the block's
//! own bytes rather than a large one, which is what keeps
//! [`WriterOptions::byte_block_target`] a bound on writer memory.
//!
//! [`WriterOptions::byte_block_target`]: crate::WriterOptions::byte_block_target

use std::collections::HashMap;

use crate::array::ScalarValue;
use crate::error::{Error, Result};
use crate::format::constants::{
    COLUMN_LAYOUT_CONSTANT, COLUMN_LAYOUT_DICTIONARY, COLUMN_LAYOUT_PLAIN,
    COLUMN_LAYOUT_RUN_LENGTH, STREAM_KIND_DICTIONARY_LENGTHS, STREAM_KIND_DICTIONARY_VALUES,
    STREAM_KIND_INDICES, STREAM_KIND_LENGTHS, STREAM_KIND_RUN_LENGTHS, STREAM_KIND_RUN_VALUES,
    STREAM_KIND_VALUES, TRANSFORM_BIT_PACKED, TRANSFORM_BOOLEAN_RLE, TRANSFORM_BYTE_STREAM_SPLIT,
    TRANSFORM_DELTA, TRANSFORM_DELTA_OF_DELTA, TRANSFORM_FRAME_OF_REFERENCE, TRANSFORM_RAW,
};
use crate::schema::LogicalType;

use super::super::buffer::bitmap_bytes;

/// The profiling table is intentionally capped. It bounds the hash table, the
/// copied dictionary bytes, and the width of a stored index independently of
/// the block-size allowance.
const DISTINCT_VALUE_LIMIT: usize = 4_096;

/// The dictionary also stops once its copied values reach this many bytes, so a
/// column of few but large distinct values cannot pull a megabyte-scale table
/// into the profile before the cardinality cap would have noticed.
const DISTINCT_BYTE_LIMIT: usize = 1 << 20;

/// Indices are stored as `u16`, which the cardinality cap must keep valid.
const _: () = assert!(DISTINCT_VALUE_LIMIT <= u16::MAX as usize + 1);

/// A transform/layout choice, in stable tie-breaking order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum ValueEncoding {
    Raw,
    BitPacked,
    BooleanRle,
    FrameOfReference,
    Delta,
    DeltaOfDelta,
    ByteStreamSplit,
    Constant,
    Dictionary,
    RunLength,
}

impl ValueEncoding {
    /// The total order that breaks a tie between equally priced candidates.
    pub(super) fn rank(self) -> u8 {
        match self {
            Self::Raw => 0,
            Self::BitPacked => 1,
            Self::BooleanRle => 2,
            Self::FrameOfReference => 3,
            Self::Delta => 4,
            Self::DeltaOfDelta => 5,
            Self::ByteStreamSplit => 6,
            Self::Constant => 7,
            Self::Dictionary => 8,
            Self::RunLength => 9,
        }
    }
}

/// One uncompressed physical stream. Compression and frame placement happen in
/// the writer after candidate selection.
#[derive(Debug)]
pub(super) struct CandidateStream {
    pub(super) kind: u16,
    pub(super) transform: u16,
    pub(super) element_count: u64,
    pub(super) bytes: Vec<u8>,
}

/// One candidate's value streams and its column layout.
#[derive(Debug)]
pub(super) struct ValueCandidate {
    pub(super) layout: u16,
    pub(super) streams: Vec<CandidateStream>,
}

/// Dense values retained once while a column's candidates are estimated.
///
/// Values live in flat buffers: fixed-width values are concatenated at their
/// canonical width, and variable-width values are concatenated alongside one
/// `uint32` length each. Nothing here allocates per value.
#[derive(Debug)]
pub(super) enum PreparedValues {
    Bool(Vec<bool>),
    /// `signed` is `Some` for the integer-like types section 9 lets frame of
    /// reference, delta, and bit packing describe, and `None` for floats and
    /// fixed binary, whose bytes are never read as numbers.
    Fixed {
        width: usize,
        signed: Option<bool>,
        bytes: Vec<u8>,
    },
    Variable {
        bytes: Vec<u8>,
        lengths: Vec<u32>,
    },
}

/// The bounded facts shared by every candidate estimate and encoder.
///
/// Each transform is described by the bit width it would need, or by `None`
/// when some value puts the transform out of range. The flags are sticky: one
/// value that overflows disqualifies the transform for the whole stream.
#[derive(Debug)]
pub(super) struct ValueProfile {
    pub(super) count: usize,
    pub(super) constant: bool,
    pub(super) run_lengths: Vec<u32>,
    pub(super) max_run_length: u32,
    pub(super) dictionary: Option<DictionaryProfile>,
    pub(super) numeric: Option<NumericProfile>,
}

#[derive(Debug)]
pub(super) struct DictionaryProfile {
    pub(super) values: Vec<Vec<u8>>,
    pub(super) indices: Vec<u16>,
}

#[derive(Debug)]
pub(super) struct NumericProfile {
    /// The block minimum, which section 9.3 stores as the frame of reference.
    pub(super) minimum: i128,
    /// `Some(width)` when every value is a `uint64`, per section 9.2.
    pub(super) bit_packed_bits: Option<u8>,
    /// `Some(width)` when every value minus the minimum is a `uint64`, per 9.3.
    pub(super) frame_of_reference_bits: Option<u8>,
    /// `Some(width)` when every adjacent difference fits `int64`, per 9.4.
    pub(super) delta_bits: Option<u8>,
    /// `Some(width)` when there are at least two values and every difference
    /// between adjacent deltas also fits `int64`, per section 9.5.
    pub(super) delta_of_delta_bits: Option<u8>,
}

/// Collect dense values from the already validated logical stream and build its
/// bounded profile once. The iterator is consumed in block order, which makes
/// dictionaries and runs independent of append batch boundaries.
pub(super) fn prepare<'a, I>(
    logical_type: &LogicalType,
    values: I,
) -> Result<(PreparedValues, ValueProfile)>
where
    I: IntoIterator<Item = Result<ScalarValue<'a>>>,
{
    let mut prepared = empty_values(logical_type)?;
    for value in values {
        push_value(logical_type, &mut prepared, value?)?;
    }
    let profile = profile(&prepared)?;
    Ok((prepared, profile))
}

fn empty_values(logical_type: &LogicalType) -> Result<PreparedValues> {
    let (width, signed) = match logical_type {
        LogicalType::Bool => return Ok(PreparedValues::Bool(Vec::new())),
        LogicalType::Utf8 | LogicalType::Categorical { .. } | LogicalType::Binary => {
            return Ok(PreparedValues::Variable {
                bytes: Vec::new(),
                lengths: Vec::new(),
            });
        }
        LogicalType::Int8 => (1, Some(true)),
        LogicalType::Int16 => (2, Some(true)),
        LogicalType::Int32 | LogicalType::Date32 => (4, Some(true)),
        LogicalType::Int64 | LogicalType::Decimal { .. } | LogicalType::Timestamp { .. } => {
            (8, Some(true))
        }
        LogicalType::UInt8 => (1, Some(false)),
        LogicalType::UInt16 => (2, Some(false)),
        LogicalType::UInt32 => (4, Some(false)),
        LogicalType::UInt64 => (8, Some(false)),
        LogicalType::Float32 => (4, None),
        LogicalType::Float64 => (8, None),
        LogicalType::FixedBinary { byte_width } => (
            usize::try_from(*byte_width)
                .map_err(|_| resource("a fixed_binary width this platform can hold"))?,
            None,
        ),
    };
    if width == 0 {
        // The schema frame refuses a zero-width fixed_binary column, so this
        // states the invariant the flat buffer below depends on rather than
        // describing anything a caller can reach.
        return Err(invalid("a fixed-width column cannot have zero width"));
    }
    Ok(PreparedValues::Fixed {
        width,
        signed,
        bytes: Vec::new(),
    })
}

fn push_value(
    logical_type: &LogicalType,
    prepared: &mut PreparedValues,
    value: ScalarValue<'_>,
) -> Result<()> {
    match (logical_type, prepared, value) {
        (LogicalType::Bool, PreparedValues::Bool(values), ScalarValue::Bool(value)) => {
            values
                .try_reserve(1)
                .map_err(|_| resource("boolean values"))?;
            values.push(value);
        }
        (LogicalType::Int8, PreparedValues::Fixed { bytes, .. }, ScalarValue::Int8(value)) => {
            push_bytes(bytes, &value.to_le_bytes())?
        }
        (LogicalType::Int16, PreparedValues::Fixed { bytes, .. }, ScalarValue::Int16(value)) => {
            push_bytes(bytes, &value.to_le_bytes())?
        }
        (LogicalType::Int32, PreparedValues::Fixed { bytes, .. }, ScalarValue::Int32(value)) => {
            push_bytes(bytes, &value.to_le_bytes())?
        }
        (LogicalType::Int64, PreparedValues::Fixed { bytes, .. }, ScalarValue::Int64(value)) => {
            push_bytes(bytes, &value.to_le_bytes())?
        }
        (LogicalType::UInt8, PreparedValues::Fixed { bytes, .. }, ScalarValue::UInt8(value)) => {
            push_bytes(bytes, &value.to_le_bytes())?
        }
        (LogicalType::UInt16, PreparedValues::Fixed { bytes, .. }, ScalarValue::UInt16(value)) => {
            push_bytes(bytes, &value.to_le_bytes())?
        }
        (LogicalType::UInt32, PreparedValues::Fixed { bytes, .. }, ScalarValue::UInt32(value)) => {
            push_bytes(bytes, &value.to_le_bytes())?
        }
        (LogicalType::UInt64, PreparedValues::Fixed { bytes, .. }, ScalarValue::UInt64(value)) => {
            push_bytes(bytes, &value.to_le_bytes())?
        }
        (
            LogicalType::Float32,
            PreparedValues::Fixed { bytes, .. },
            ScalarValue::Float32(value),
        ) => push_bytes(bytes, &value.to_bits().to_le_bytes())?,
        (
            LogicalType::Float64,
            PreparedValues::Fixed { bytes, .. },
            ScalarValue::Float64(value),
        ) => push_bytes(bytes, &value.to_bits().to_le_bytes())?,
        (
            LogicalType::Decimal { .. },
            PreparedValues::Fixed { bytes, .. },
            ScalarValue::Decimal { unscaled, .. },
        ) => push_bytes(bytes, &unscaled.to_le_bytes())?,
        (
            LogicalType::Timestamp { .. },
            PreparedValues::Fixed { bytes, .. },
            ScalarValue::Timestamp { value, .. },
        ) => push_bytes(bytes, &value.to_le_bytes())?,
        (LogicalType::Date32, PreparedValues::Fixed { bytes, .. }, ScalarValue::Date32(value)) => {
            push_bytes(bytes, &value.to_le_bytes())?
        }
        (
            LogicalType::FixedBinary { byte_width },
            PreparedValues::Fixed { bytes, .. },
            ScalarValue::FixedBinary(value),
        ) => {
            if value.len() != *byte_width as usize {
                return Err(invalid("fixed_binary value has the wrong width"));
            }
            push_bytes(bytes, value)?
        }
        (
            LogicalType::Utf8,
            PreparedValues::Variable { bytes, lengths },
            ScalarValue::Utf8(value),
        )
        | (
            LogicalType::Categorical { .. },
            PreparedValues::Variable { bytes, lengths },
            ScalarValue::Categorical(value),
        ) => push_variable(bytes, lengths, value.as_bytes())?,
        (
            LogicalType::Binary,
            PreparedValues::Variable { bytes, lengths },
            ScalarValue::Binary(value),
        ) => push_variable(bytes, lengths, value)?,
        _ => return Err(invalid("a dense value does not match its logical type")),
    }
    Ok(())
}

fn push_bytes(buffer: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    buffer
        .try_reserve(bytes.len())
        .map_err(|_| resource("dense value bytes"))?;
    buffer.extend_from_slice(bytes);
    Ok(())
}

fn push_variable(bytes: &mut Vec<u8>, lengths: &mut Vec<u32>, value: &[u8]) -> Result<()> {
    let length =
        u32::try_from(value.len()).map_err(|_| invalid("a value length exceeds uint32"))?;
    lengths
        .try_reserve(1)
        .map_err(|_| resource("value lengths"))?;
    push_bytes(bytes, value)?;
    lengths.push(length);
    Ok(())
}

// ------------------------------------------------------------------ profiling

/// One sequential pass over the dense values as opaque comparison keys.
///
/// Runs, constancy, and the dictionary are all order-sensitive facts about the
/// same sequence, so they share this pass and borrow the values rather than
/// copying them.
fn profile(values: &PreparedValues) -> Result<ValueProfile> {
    let count = values.count();
    let mut dictionary = DictionaryBuilder::default();
    // A run cannot be longer than the block, and section 8 caps a block at
    // `UINT32_MAX` rows, so `u32` holds the longest run a block can contain.
    let mut run_lengths: Vec<u32> = Vec::new();
    let mut max_run_length = 0_u32;
    let mut previous: Option<&[u8]> = None;

    for key in values.keys() {
        dictionary.push(key)?;
        if previous == Some(key) {
            let run = run_lengths
                .last_mut()
                .expect("a run in progress has a length");
            *run += 1;
            max_run_length = max_run_length.max(*run);
        } else {
            run_lengths
                .try_reserve(1)
                .map_err(|_| resource("the run-length profile"))?;
            run_lengths.push(1);
            max_run_length = max_run_length.max(1);
            previous = Some(key);
        }
    }

    Ok(ValueProfile {
        count,
        // Every value equal to the first is exactly one run over a nonempty
        // stream, and an empty stream is trivially constant.
        constant: run_lengths.len() <= 1,
        run_lengths,
        max_run_length,
        dictionary: dictionary.finish(),
        numeric: values.numeric_profile(),
    })
}

/// The block-local dictionary, abandoned as soon as it crosses either cap.
#[derive(Default)]
struct DictionaryBuilder {
    map: HashMap<Vec<u8>, u16>,
    values: Vec<Vec<u8>>,
    indices: Vec<u16>,
    copied_bytes: usize,
    exceeded: bool,
}

impl DictionaryBuilder {
    fn push(&mut self, key: &[u8]) -> Result<()> {
        if self.exceeded {
            return Ok(());
        }
        if let Some(&index) = self.map.get(key) {
            self.indices
                .try_reserve(1)
                .map_err(|_| resource("dictionary indices"))?;
            self.indices.push(index);
            return Ok(());
        }
        if self.values.len() == DISTINCT_VALUE_LIMIT
            || self.copied_bytes.saturating_add(key.len()) > DISTINCT_BYTE_LIMIT
        {
            // Past either cap the dictionary can no longer be the answer, so
            // the partial table is released rather than carried to the end.
            self.exceeded = true;
            self.map = HashMap::new();
            self.values = Vec::new();
            self.indices = Vec::new();
            return Ok(());
        }
        let index = u16::try_from(self.values.len()).map_err(|_| resource("a dictionary index"))?;
        self.map
            .try_reserve(1)
            .map_err(|_| resource("the dictionary map"))?;
        self.values
            .try_reserve(1)
            .map_err(|_| resource("dictionary values"))?;
        self.indices
            .try_reserve(1)
            .map_err(|_| resource("dictionary indices"))?;
        self.map.insert(key.to_vec(), index);
        self.values.push(key.to_vec());
        self.copied_bytes = self.copied_bytes.saturating_add(key.len());
        self.indices.push(index);
        Ok(())
    }

    fn finish(self) -> Option<DictionaryProfile> {
        (!self.exceeded).then_some(DictionaryProfile {
            values: self.values,
            indices: self.indices,
        })
    }
}

/// One dense value as the byte string the profile compares.
const FALSE_KEY: [u8; 1] = [0];
const TRUE_KEY: [u8; 1] = [1];

/// A borrowing, sequential view of the dense values as comparison keys.
#[derive(Clone)]
enum Keys<'a> {
    Bool(std::slice::Iter<'a, bool>),
    Fixed(std::slice::ChunksExact<'a, u8>),
    Variable {
        bytes: &'a [u8],
        lengths: std::slice::Iter<'a, u32>,
        offset: usize,
    },
}

impl<'a> Iterator for Keys<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        match self {
            Self::Bool(values) => values.next().map(|value| {
                if *value {
                    &TRUE_KEY[..]
                } else {
                    &FALSE_KEY[..]
                }
            }),
            Self::Fixed(chunks) => chunks.next(),
            Self::Variable {
                bytes,
                lengths,
                offset,
            } => {
                let length = usize::try_from(*lengths.next()?).ok()?;
                let start = *offset;
                let end = start.checked_add(length)?;
                *offset = end;
                bytes.get(start..end)
            }
        }
    }
}

impl PreparedValues {
    fn as_bools(&self) -> Option<&[bool]> {
        match self {
            Self::Bool(values) => Some(values),
            _ => None,
        }
    }

    fn count(&self) -> usize {
        match self {
            Self::Bool(values) => values.len(),
            Self::Fixed { width, bytes, .. } => bytes.len() / *width,
            Self::Variable { lengths, .. } => lengths.len(),
        }
    }

    fn keys(&self) -> Keys<'_> {
        match self {
            Self::Bool(values) => Keys::Bool(values.iter()),
            Self::Fixed { width, bytes, .. } => Keys::Fixed(bytes.chunks_exact(*width)),
            Self::Variable { bytes, lengths } => Keys::Variable {
                bytes,
                lengths: lengths.iter(),
                offset: 0,
            },
        }
    }

    /// The dense values read back as numbers, for the integer-like types only.
    fn numbers(&self) -> Option<impl Iterator<Item = i128> + Clone + '_> {
        let Self::Fixed {
            width,
            signed: Some(signed),
            bytes,
        } = self
        else {
            return None;
        };
        let signed = *signed;
        Some(
            bytes
                .chunks_exact(*width)
                .map(move |chunk| read_number(chunk, signed)),
        )
    }

    /// Every numeric fact section 9 needs, in one pass, with sticky overflow.
    fn numeric_profile(&self) -> Option<NumericProfile> {
        let mut numbers = self.numbers()?;
        let Some(first) = numbers.next() else {
            return Some(NumericProfile {
                minimum: 0,
                bit_packed_bits: None,
                frame_of_reference_bits: None,
                delta_bits: None,
                delta_of_delta_bits: None,
            });
        };

        let mut minimum = first;
        let mut maximum = first;
        let mut nonnegative = first >= 0;
        let mut value_bits = bit_width_of(first);
        let mut delta_ok = true;
        let mut delta_bits = 0_u8;
        let mut delta_of_delta_ok = true;
        let mut delta_of_delta_bits = 0_u8;
        let mut previous = first;
        let mut previous_delta: Option<i64> = None;
        let mut count = 1_usize;

        for value in numbers {
            count += 1;
            minimum = minimum.min(value);
            maximum = maximum.max(value);
            nonnegative &= value >= 0;
            if nonnegative {
                value_bits = value_bits.max(bit_width_of(value));
            }
            match value
                .checked_sub(previous)
                .and_then(|delta| i64::try_from(delta).ok())
            {
                Some(delta) => {
                    delta_bits = delta_bits.max(bit_width(zigzag(delta)));
                    if let Some(earlier) = previous_delta {
                        match delta.checked_sub(earlier) {
                            Some(difference) => {
                                delta_of_delta_bits =
                                    delta_of_delta_bits.max(bit_width(zigzag(difference)));
                            }
                            // One overflow disqualifies the whole stream, so
                            // this is never cleared by a later value.
                            None => delta_of_delta_ok = false,
                        }
                    }
                    previous_delta = Some(delta);
                }
                None => {
                    delta_ok = false;
                    delta_of_delta_ok = false;
                    previous_delta = None;
                }
            }
            previous = value;
        }

        Some(NumericProfile {
            minimum,
            bit_packed_bits: nonnegative.then_some(value_bits),
            frame_of_reference_bits: maximum
                .checked_sub(minimum)
                .and_then(|span| u64::try_from(span).ok())
                .map(bit_width),
            delta_bits: delta_ok.then_some(delta_bits),
            delta_of_delta_bits: (delta_ok && delta_of_delta_ok && count >= 2)
                .then_some(delta_of_delta_bits),
        })
    }
}

/// Read one canonical little-endian value of at most eight stored bytes,
/// sign-extending it from its stored width when the logical type is signed.
fn read_number(bytes: &[u8], signed: bool) -> i128 {
    let mut raw = [0_u8; 16];
    raw[..bytes.len()].copy_from_slice(bytes);
    let value = i128::from_le_bytes(raw);
    if !signed || bytes.is_empty() {
        return value;
    }
    let unused = (raw.len() - bytes.len()) * 8;
    (value << unused) >> unused
}

// ----------------------------------------------------------------- candidates

/// Return candidates in deterministic order. The table follows section 12;
/// transforms which cannot apply to a logical type are never offered. The
/// caller re-sorts by price and [`ValueEncoding::rank`], so this order only has
/// to be stable, not preferential.
pub(super) fn candidates(
    logical_type: &LogicalType,
    values: &PreparedValues,
    profile: &ValueProfile,
) -> Vec<ValueEncoding> {
    supported_candidates(logical_type)
        .iter()
        .copied()
        .filter(|candidate| is_valid(*candidate, logical_type, values, profile))
        .collect()
}

/// Whether this writer offers a transform/layout for a logical type at all,
/// before any block values exist to profile.
///
/// This answers the section 12 question — is this a candidate worth pricing for
/// this type — rather than the section 9 question of whether the format permits
/// it. The two differ: a dictionary `timestamp64` column is legal v0.2 that
/// section 12 does not list, so neither policy writes one. Holding the fixed
/// policy to the same table is what keeps it from producing a shape the
/// adaptive policy never would.
pub(super) fn supports(logical_type: &LogicalType, encoding: ValueEncoding) -> bool {
    supported_candidates(logical_type).contains(&encoding)
}

/// The section 12 candidate table, in stable order. Both policies read it: the
/// adaptive policy filters it by [`is_valid`] and prices what survives, and the
/// fixed policy uses it to refuse a transform before the file exists.
fn supported_candidates(logical_type: &LogicalType) -> &'static [ValueEncoding] {
    match logical_type {
        LogicalType::Bool => &[
            ValueEncoding::Raw,
            ValueEncoding::Constant,
            ValueEncoding::BitPacked,
            ValueEncoding::BooleanRle,
        ],
        LogicalType::Int8
        | LogicalType::Int16
        | LogicalType::Int32
        | LogicalType::Int64
        | LogicalType::UInt8
        | LogicalType::UInt16
        | LogicalType::UInt32
        | LogicalType::UInt64
        | LogicalType::Decimal { .. }
        | LogicalType::Date32 => &[
            ValueEncoding::Raw,
            ValueEncoding::Constant,
            ValueEncoding::FrameOfReference,
            ValueEncoding::Delta,
            ValueEncoding::DeltaOfDelta,
            ValueEncoding::Dictionary,
            ValueEncoding::RunLength,
            ValueEncoding::BitPacked,
        ],
        LogicalType::Timestamp { .. } => &[
            ValueEncoding::Raw,
            ValueEncoding::FrameOfReference,
            ValueEncoding::Delta,
            ValueEncoding::DeltaOfDelta,
        ],
        LogicalType::Float32 | LogicalType::Float64 => &[
            ValueEncoding::Raw,
            ValueEncoding::Constant,
            ValueEncoding::Dictionary,
            ValueEncoding::RunLength,
            ValueEncoding::ByteStreamSplit,
        ],
        LogicalType::Utf8 | LogicalType::Categorical { .. } | LogicalType::Binary => &[
            ValueEncoding::Raw,
            ValueEncoding::Constant,
            ValueEncoding::Dictionary,
            ValueEncoding::RunLength,
        ],
        LogicalType::FixedBinary { .. } => &[
            ValueEncoding::Raw,
            ValueEncoding::Constant,
            ValueEncoding::Dictionary,
            ValueEncoding::RunLength,
            ValueEncoding::ByteStreamSplit,
        ],
    }
}

/// Whether one candidate can describe this stream at all.
///
/// Every numeric answer comes from [`NumericProfile`], which already recorded
/// the overflow the check would otherwise rediscover, so this is constant time.
fn is_valid(
    encoding: ValueEncoding,
    logical_type: &LogicalType,
    values: &PreparedValues,
    profile: &ValueProfile,
) -> bool {
    if profile.count == 0 {
        return false;
    }
    let numeric = profile.numeric.as_ref();
    match encoding {
        ValueEncoding::Raw => true,
        ValueEncoding::Constant => profile.constant,
        ValueEncoding::BitPacked => match values {
            PreparedValues::Bool(_) => true,
            _ => numeric.is_some_and(|numeric| numeric.bit_packed_bits.is_some()),
        },
        ValueEncoding::BooleanRle => matches!(values, PreparedValues::Bool(_)),
        ValueEncoding::FrameOfReference => {
            numeric.is_some_and(|numeric| numeric.frame_of_reference_bits.is_some())
        }
        ValueEncoding::Delta => numeric.is_some_and(|numeric| numeric.delta_bits.is_some()),
        ValueEncoding::DeltaOfDelta => {
            numeric.is_some_and(|numeric| numeric.delta_of_delta_bits.is_some())
        }
        ValueEncoding::ByteStreamSplit => matches!(values, PreparedValues::Fixed { .. }),
        ValueEncoding::Dictionary => profile.dictionary.is_some(),
        ValueEncoding::RunLength => {
            !matches!(logical_type, LogicalType::Bool) && !profile.run_lengths.is_empty()
        }
    }
}

/// Estimate the transformed length of every stream one value candidate needs.
///
/// Zstandard is intentionally priced by its transformed upper bound here; the
/// final choice always uses actual compressed bytes for raw and the two best
/// specialized estimates.
pub(super) fn estimate(
    logical_type: &LogicalType,
    values: &PreparedValues,
    profile: &ValueProfile,
    encoding: ValueEncoding,
) -> Option<Vec<u64>> {
    if !is_valid(encoding, logical_type, values, profile) {
        return None;
    }
    let count = u64::try_from(profile.count).ok()?;
    let numeric = profile.numeric.as_ref();
    let width = match values {
        PreparedValues::Fixed { width, .. } => Some(*width as u64),
        _ => None,
    };
    match encoding {
        ValueEncoding::Raw => match values {
            PreparedValues::Bool(_) => Some(vec![bitmap_bytes(count)]),
            PreparedValues::Fixed { bytes, .. } => Some(vec![bytes.len() as u64]),
            PreparedValues::Variable { bytes, .. } => {
                Some(vec![bytes.len() as u64, count.checked_mul(4)?])
            }
        },
        ValueEncoding::Constant => match values {
            PreparedValues::Bool(_) => Some(vec![1]),
            PreparedValues::Fixed { width, .. } => Some(vec![*width as u64]),
            PreparedValues::Variable { lengths, .. } => Some(vec![u64::from(*lengths.first()?), 4]),
        },
        // A boolean stream has no numeric profile, so this prices it at one
        // bit per value. That is exact unless every value is false, where
        // section 9.2's zero-width form makes the real stream one byte. The
        // gap can only over-price the candidate, and an all-false stream is
        // also constant, so `Constant` is always offered beside it at the
        // smaller price and wins the shortlist regardless.
        ValueEncoding::BitPacked => Some(vec![packed_length(
            profile.count,
            numeric
                .and_then(|numeric| numeric.bit_packed_bits)
                .unwrap_or(1),
        )]),
        ValueEncoding::BooleanRle => Some(vec![
            4 + bitmap_bytes(profile.run_lengths.len() as u64)
                + packed_length(
                    profile.run_lengths.len(),
                    bit_width(u64::from(profile.max_run_length)),
                ),
        ]),
        ValueEncoding::FrameOfReference => Some(vec![width?.checked_add(packed_length(
            profile.count,
            numeric?.frame_of_reference_bits?,
        ))?]),
        ValueEncoding::Delta => {
            let packed = if profile.count <= 1 {
                0
            } else {
                packed_length(profile.count - 1, numeric?.delta_bits?)
            };
            Some(vec![width?.checked_add(packed)?])
        }
        ValueEncoding::DeltaOfDelta => Some(vec![width?.checked_add(8)?.checked_add(
            packed_length(profile.count - 2, numeric?.delta_of_delta_bits?),
        )?]),
        ValueEncoding::ByteStreamSplit => Some(vec![count.checked_mul(width?)?]),
        ValueEncoding::Dictionary => {
            let dictionary = profile.dictionary.as_ref()?;
            let mut lengths = vec![dictionary_bytes(dictionary)?];
            if matches!(values, PreparedValues::Variable { .. }) {
                lengths.push(
                    u64::try_from(dictionary.values.len())
                        .ok()?
                        .checked_mul(4)?,
                );
            }
            lengths.push(packed_length(
                profile.count,
                bit_width(dictionary.values.len().saturating_sub(1) as u64),
            ));
            Some(lengths)
        }
        ValueEncoding::RunLength => {
            let runs = profile.run_lengths.len();
            let mut lengths = vec![run_value_bytes(values, profile)?];
            if matches!(values, PreparedValues::Variable { .. }) {
                lengths.push(u64::try_from(runs).ok()?.checked_mul(4)?);
            }
            lengths.push(packed_length(
                runs,
                bit_width(u64::from(profile.max_run_length)),
            ));
            Some(lengths)
        }
    }
}

fn dictionary_bytes(dictionary: &DictionaryProfile) -> Option<u64> {
    dictionary.values.iter().try_fold(0_u64, |total, value| {
        total.checked_add(u64::try_from(value.len()).ok()?)
    })
}

/// The bytes the first value of every run contributes to a run-values stream.
fn run_value_bytes(values: &PreparedValues, profile: &ValueProfile) -> Option<u64> {
    match values {
        PreparedValues::Bool(_) => None,
        PreparedValues::Fixed { width, .. } => u64::try_from(profile.run_lengths.len())
            .ok()?
            .checked_mul(*width as u64),
        PreparedValues::Variable { lengths, .. } => {
            let mut total = 0_u64;
            let mut start = 0_usize;
            for run in &profile.run_lengths {
                total = total.checked_add(u64::from(*lengths.get(start)?))?;
                start = start.checked_add(usize::try_from(*run).ok()?)?;
            }
            Some(total)
        }
    }
}

// ------------------------------------------------------------------- encoders

/// Materialize one candidate after its cheap estimate won a bounded shortlist.
///
/// A candidate that cannot describe this stream at all is rejected here rather
/// than part-way through serialization, so a caller cannot turn an inapplicable
/// encoding into a malformed one.
///
/// `values` and `profile` must be the pair one [`prepare`] call returned, which
/// is the only way to obtain either. The encoders below rely on that pairing:
/// where a width or a range came from the profile, they use it without
/// re-deriving it, and a profile describing some other column would be a bug
/// here rather than input this module can check.
pub(super) fn encode(
    logical_type: &LogicalType,
    values: &PreparedValues,
    profile: &ValueProfile,
    encoding: ValueEncoding,
) -> Result<ValueCandidate> {
    if !is_valid(encoding, logical_type, values, profile) {
        return Err(invalid("this candidate does not describe these values"));
    }
    let streams = match encoding {
        ValueEncoding::Raw => raw_streams(values),
        ValueEncoding::Constant => constant_streams(values),
        ValueEncoding::BitPacked => bit_packed_streams(values, profile),
        ValueEncoding::BooleanRle => boolean_rle_streams(values),
        ValueEncoding::FrameOfReference => frame_of_reference_streams(values, profile),
        ValueEncoding::Delta => delta_streams(values, profile),
        ValueEncoding::DeltaOfDelta => delta_of_delta_streams(values, profile),
        ValueEncoding::ByteStreamSplit => byte_stream_split_streams(values),
        ValueEncoding::Dictionary => dictionary_streams(values, profile),
        ValueEncoding::RunLength => run_length_streams(values, profile),
    }?;
    let layout = match encoding {
        ValueEncoding::Constant => COLUMN_LAYOUT_CONSTANT,
        ValueEncoding::Dictionary => COLUMN_LAYOUT_DICTIONARY,
        ValueEncoding::RunLength => COLUMN_LAYOUT_RUN_LENGTH,
        _ => COLUMN_LAYOUT_PLAIN,
    };
    Ok(ValueCandidate { layout, streams })
}

fn raw_streams(values: &PreparedValues) -> Result<Vec<CandidateStream>> {
    match values {
        PreparedValues::Bool(values) => Ok(vec![candidate_stream(
            STREAM_KIND_VALUES,
            TRANSFORM_RAW,
            values.len(),
            pack_booleans(values),
        )?]),
        PreparedValues::Fixed { width, bytes, .. } => Ok(vec![candidate_stream(
            STREAM_KIND_VALUES,
            TRANSFORM_RAW,
            bytes.len() / *width,
            copied(bytes)?,
        )?]),
        PreparedValues::Variable { bytes, lengths } => Ok(vec![
            candidate_stream(
                STREAM_KIND_VALUES,
                TRANSFORM_RAW,
                lengths.len(),
                copied(bytes)?,
            )?,
            candidate_stream(
                STREAM_KIND_LENGTHS,
                TRANSFORM_RAW,
                lengths.len(),
                length_bytes(lengths)?,
            )?,
        ]),
    }
}

fn constant_streams(values: &PreparedValues) -> Result<Vec<CandidateStream>> {
    let first = values
        .keys()
        .next()
        .ok_or_else(|| invalid("a constant column has no value"))?;
    match values {
        PreparedValues::Bool(values) => Ok(vec![candidate_stream(
            STREAM_KIND_VALUES,
            TRANSFORM_RAW,
            1,
            pack_booleans(&values[..1]),
        )?]),
        PreparedValues::Fixed { .. } => Ok(vec![candidate_stream(
            STREAM_KIND_VALUES,
            TRANSFORM_RAW,
            1,
            copied(first)?,
        )?]),
        PreparedValues::Variable { lengths, .. } => Ok(vec![
            candidate_stream(STREAM_KIND_VALUES, TRANSFORM_RAW, 1, copied(first)?)?,
            candidate_stream(
                STREAM_KIND_LENGTHS,
                TRANSFORM_RAW,
                1,
                length_bytes(&lengths[..1])?,
            )?,
        ]),
    }
}

fn bit_packed_streams(
    values: &PreparedValues,
    profile: &ValueProfile,
) -> Result<Vec<CandidateStream>> {
    let (count, bytes) = match values {
        PreparedValues::Bool(values) => {
            // Section 9.2 encodes an all-zero stream as the single width byte
            // `00`, so the width comes from the values rather than the type.
            let width = u8::from(values.iter().any(|value| *value));
            let bytes = pack_from(
                width,
                values.len(),
                values.iter().map(|value| u64::from(*value)),
            )?;
            (values.len(), bytes)
        }
        _ => {
            let numbers = values
                .numbers()
                .ok_or_else(|| invalid("bit packing does not apply to this value stream"))?;
            let width = profile
                .numeric
                .as_ref()
                .and_then(|numeric| numeric.bit_packed_bits)
                .ok_or_else(|| invalid("bit packing has no width for this value stream"))?;
            let bytes = pack_from(
                width,
                profile.count,
                // The profile records a width only when every value is a
                // `uint64`, so this conversion cannot fail for a live stream.
                numbers.map(|value| u64::try_from(value).expect("the profile checked this value")),
            )?;
            (profile.count, bytes)
        }
    };
    Ok(vec![candidate_stream(
        STREAM_KIND_VALUES,
        TRANSFORM_BIT_PACKED,
        count,
        bytes,
    )?])
}

fn boolean_rle_streams(values: &PreparedValues) -> Result<Vec<CandidateStream>> {
    let values = values
        .as_bools()
        .ok_or_else(|| invalid("boolean RLE does not apply to this value stream"))?;
    Ok(vec![candidate_stream(
        STREAM_KIND_VALUES,
        TRANSFORM_BOOLEAN_RLE,
        values.len(),
        boolean_rle(values)?,
    )?])
}

/// Section 9.3: one canonical-width block minimum, then packed differences.
fn frame_of_reference_streams(
    values: &PreparedValues,
    profile: &ValueProfile,
) -> Result<Vec<CandidateStream>> {
    let PreparedValues::Fixed { width, signed, .. } = values else {
        return Err(invalid(
            "frame of reference does not apply to this value stream",
        ));
    };
    let signed = signed.ok_or_else(|| invalid("frame of reference needs a numeric column"))?;
    let numeric = profile
        .numeric
        .as_ref()
        .ok_or_else(|| invalid("frame of reference has no numeric profile"))?;
    let bits = numeric
        .frame_of_reference_bits
        .ok_or_else(|| invalid("frame-of-reference difference does not fit uint64"))?;
    let numbers = values
        .numbers()
        .ok_or_else(|| invalid("frame of reference has no values"))?;
    let base = numeric.minimum;

    let mut output = canonical_number(base, *width, signed)?;
    let packed = pack_from(
        bits,
        profile.count,
        numbers.map(|value| {
            u64::try_from(value.saturating_sub(base)).expect("the profile checked this difference")
        }),
    )?;
    push_bytes(&mut output, &packed)?;
    Ok(vec![candidate_stream(
        STREAM_KIND_VALUES,
        TRANSFORM_FRAME_OF_REFERENCE,
        profile.count,
        output,
    )?])
}

/// Section 9.4: the first canonical-width value, then packed ZigZag deltas.
fn delta_streams(values: &PreparedValues, profile: &ValueProfile) -> Result<Vec<CandidateStream>> {
    let PreparedValues::Fixed { width, .. } = values else {
        return Err(invalid("delta does not apply to this value stream"));
    };
    let bits = profile
        .numeric
        .as_ref()
        .and_then(|numeric| numeric.delta_bits)
        .ok_or_else(|| invalid("a delta does not fit int64"))?;
    let mut output = copied(first_value(values, *width)?)?;
    if profile.count > 1 {
        let numbers = values
            .numbers()
            .ok_or_else(|| invalid("delta has no values"))?;
        let packed = pack_from(
            bits,
            profile.count - 1,
            adjacent_deltas(numbers).map(zigzag),
        )?;
        push_bytes(&mut output, &packed)?;
    }
    Ok(vec![candidate_stream(
        STREAM_KIND_VALUES,
        TRANSFORM_DELTA,
        profile.count,
        output,
    )?])
}

/// Section 9.5: the first value, the first `int64` delta, then packed ZigZag
/// differences between consecutive deltas.
fn delta_of_delta_streams(
    values: &PreparedValues,
    profile: &ValueProfile,
) -> Result<Vec<CandidateStream>> {
    let PreparedValues::Fixed { width, .. } = values else {
        return Err(invalid(
            "delta-of-delta does not apply to this value stream",
        ));
    };
    let bits = profile
        .numeric
        .as_ref()
        .and_then(|numeric| numeric.delta_of_delta_bits)
        .ok_or_else(|| invalid("delta-of-delta requires at least two in-range values"))?;
    let numbers = values
        .numbers()
        .ok_or_else(|| invalid("delta-of-delta has no values"))?;
    let first_delta = adjacent_deltas(numbers.clone())
        .next()
        .ok_or_else(|| invalid("delta-of-delta requires at least two values"))?;

    let mut output = copied(first_value(values, *width)?)?;
    push_bytes(&mut output, &first_delta.to_le_bytes())?;
    let packed = pack_from(
        bits,
        profile.count - 2,
        adjacent_deltas(adjacent_deltas(numbers).map(i128::from)).map(zigzag),
    )?;
    push_bytes(&mut output, &packed)?;
    Ok(vec![candidate_stream(
        STREAM_KIND_VALUES,
        TRANSFORM_DELTA_OF_DELTA,
        profile.count,
        output,
    )?])
}

/// Section 9.6: all byte-zero values, then all byte-one values, and so on.
fn byte_stream_split_streams(values: &PreparedValues) -> Result<Vec<CandidateStream>> {
    let PreparedValues::Fixed { width, bytes, .. } = values else {
        return Err(invalid(
            "byte-stream split does not apply to this value stream",
        ));
    };
    let count = bytes.len() / *width;
    let mut output = Vec::new();
    output
        .try_reserve_exact(bytes.len())
        .map_err(|_| resource("byte-stream-split values"))?;
    for byte in 0..*width {
        for value in 0..count {
            output.push(bytes[value * *width + byte]);
        }
    }
    Ok(vec![candidate_stream(
        STREAM_KIND_VALUES,
        TRANSFORM_BYTE_STREAM_SPLIT,
        count,
        output,
    )?])
}

fn dictionary_streams(
    values: &PreparedValues,
    profile: &ValueProfile,
) -> Result<Vec<CandidateStream>> {
    let dictionary = profile
        .dictionary
        .as_ref()
        .ok_or_else(|| invalid("dictionary cardinality exceeded the profiling cap"))?;
    let mut streams = vec![candidate_stream(
        STREAM_KIND_DICTIONARY_VALUES,
        TRANSFORM_RAW,
        dictionary.values.len(),
        concat(&dictionary.values)?,
    )?];
    if matches!(values, PreparedValues::Variable { .. }) {
        streams.push(candidate_stream(
            STREAM_KIND_DICTIONARY_LENGTHS,
            TRANSFORM_RAW,
            dictionary.values.len(),
            value_lengths(&dictionary.values)?,
        )?);
    }
    // Every dictionary entry is reached by at least one index, so the widest
    // index is exactly the width the estimate priced.
    let bits = bit_width(dictionary.values.len().saturating_sub(1) as u64);
    streams.push(candidate_stream(
        STREAM_KIND_INDICES,
        TRANSFORM_BIT_PACKED,
        dictionary.indices.len(),
        pack_from(
            bits,
            dictionary.indices.len(),
            dictionary.indices.iter().map(|index| u64::from(*index)),
        )?,
    )?);
    Ok(streams)
}

fn run_length_streams(
    values: &PreparedValues,
    profile: &ValueProfile,
) -> Result<Vec<CandidateStream>> {
    if matches!(values, PreparedValues::Bool(_)) {
        return Err(invalid("generic run length does not apply to booleans"));
    }
    let runs = profile.run_lengths.len();
    let mut run_values = Vec::new();
    let mut run_value_lengths: Vec<u32> = Vec::new();
    run_value_lengths
        .try_reserve_exact(runs)
        .map_err(|_| resource("run value lengths"))?;

    let mut keys = values.keys();
    let mut skipped = 0_usize;
    for run in &profile.run_lengths {
        // The run lengths came from this same key sequence, so the first key of
        // each run is reached by skipping the preceding run exactly.
        let key = keys
            .nth(skipped)
            .ok_or_else(|| invalid("the run lengths do not describe these values"))?;
        push_bytes(&mut run_values, key)?;
        run_value_lengths.push(
            u32::try_from(key.len()).map_err(|_| invalid("a run value exceeds uint32 bytes"))?,
        );
        skipped = usize::try_from(*run)
            .map_err(|_| resource("a run length this platform can hold"))?
            .checked_sub(1)
            .ok_or_else(|| invalid("a run length is zero"))?;
    }

    let mut streams = vec![candidate_stream(
        STREAM_KIND_RUN_VALUES,
        TRANSFORM_RAW,
        runs,
        run_values,
    )?];
    if matches!(values, PreparedValues::Variable { .. }) {
        streams.push(candidate_stream(
            STREAM_KIND_LENGTHS,
            TRANSFORM_RAW,
            runs,
            length_bytes(&run_value_lengths)?,
        )?);
    }
    streams.push(candidate_stream(
        STREAM_KIND_RUN_LENGTHS,
        TRANSFORM_BIT_PACKED,
        runs,
        pack_from(
            bit_width(u64::from(profile.max_run_length)),
            runs,
            profile.run_lengths.iter().map(|run| u64::from(*run)),
        )?,
    )?);
    Ok(streams)
}

// -------------------------------------------------------------------- helpers

/// The first stored value of a fixed-width stream, at its canonical width.
fn first_value(values: &PreparedValues, width: usize) -> Result<&[u8]> {
    match values {
        PreparedValues::Fixed { bytes, .. } => bytes
            .get(..width)
            .ok_or_else(|| invalid("a fixed-width stream has no first value")),
        _ => Err(invalid("this stream has no fixed-width first value")),
    }
}

/// Adjacent differences, as the `int64` section 9.4 requires them to be.
///
/// The profile has already refused every candidate whose differences leave
/// `int64`, so a saturating conversion here cannot reach a selected stream.
fn adjacent_deltas(values: impl Iterator<Item = i128>) -> impl Iterator<Item = i64> {
    let mut previous: Option<i128> = None;
    values.filter_map(move |value| {
        let delta = previous.map(|earlier| {
            i64::try_from(value.saturating_sub(earlier))
                .expect("the profile checked this difference")
        });
        previous = Some(value);
        delta
    })
}

fn zigzag(value: i64) -> u64 {
    ((value as u64) << 1) ^ ((value >> 63) as u64)
}

fn canonical_number(value: i128, width: usize, signed: bool) -> Result<Vec<u8>> {
    if !signed {
        let value =
            u64::try_from(value).map_err(|_| invalid("unsigned base is negative or too wide"))?;
        return Ok(value.to_le_bytes()[..width].to_vec());
    }
    match width {
        1 => Ok(narrow::<i8>(value)?.to_le_bytes().to_vec()),
        2 => Ok(narrow::<i16>(value)?.to_le_bytes().to_vec()),
        4 => Ok(narrow::<i32>(value)?.to_le_bytes().to_vec()),
        8 => Ok(narrow::<i64>(value)?.to_le_bytes().to_vec()),
        _ => Err(invalid("invalid canonical signed width")),
    }
}

fn narrow<T: TryFrom<i128>>(value: i128) -> Result<T> {
    T::try_from(value)
        .ok()
        .ok_or_else(|| invalid("signed base does not fit its canonical width"))
}

pub(super) fn candidate_stream(
    kind: u16,
    transform: u16,
    element_count: usize,
    bytes: Vec<u8>,
) -> Result<CandidateStream> {
    Ok(CandidateStream {
        kind,
        transform,
        element_count: u64::try_from(element_count)
            .map_err(|_| resource("a stream element count"))?,
        bytes,
    })
}

fn copied(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(bytes.len())
        .map_err(|_| resource("copied stream bytes"))?;
    output.extend_from_slice(bytes);
    Ok(output)
}

fn concat(values: &[Vec<u8>]) -> Result<Vec<u8>> {
    let length = values
        .iter()
        .try_fold(0_usize, |total, value| total.checked_add(value.len()))
        .ok_or_else(|| resource("concatenated values"))?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(length)
        .map_err(|_| resource("concatenated values"))?;
    for value in values {
        output.extend_from_slice(value);
    }
    Ok(output)
}

/// Section 9.1: an unsigned 32-bit length per variable-width value.
fn length_bytes(lengths: &[u32]) -> Result<Vec<u8>> {
    let byte_count = lengths
        .len()
        .checked_mul(4)
        .ok_or_else(|| resource("value lengths"))?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(byte_count)
        .map_err(|_| resource("value lengths"))?;
    for length in lengths {
        output.extend_from_slice(&length.to_le_bytes());
    }
    Ok(output)
}

fn value_lengths(values: &[Vec<u8>]) -> Result<Vec<u8>> {
    let mut lengths = Vec::new();
    lengths
        .try_reserve_exact(values.len())
        .map_err(|_| resource("value lengths"))?;
    for value in values {
        lengths.push(
            u32::try_from(value.len()).map_err(|_| invalid("a value length exceeds uint32"))?,
        );
    }
    length_bytes(&lengths)
}

/// Section 9.1: raw boolean data is LSB-first bit packing without a width byte.
pub(super) fn pack_booleans(values: &[bool]) -> Vec<u8> {
    let mut output = vec![0; bitmap_bytes(values.len() as u64) as usize];
    for (index, value) in values.iter().enumerate() {
        if *value {
            output[index / 8] |= 1 << (index % 8);
        }
    }
    output
}

/// Section 9.2: one `uint8` bit width, then values concatenated LSB-first.
///
/// The width is supplied rather than derived so that the values can be streamed
/// from wherever they are computed instead of being materialized first. Every
/// caller takes it from the profile that already measured the widest value, and
/// the buffer is sized from `count` before any of it is written, so a caller
/// whose iterator disagrees with its count is reported rather than left to
/// write past the end or to leave a short stream behind.
fn pack_from(width: u8, count: usize, values: impl Iterator<Item = u64>) -> Result<Vec<u8>> {
    let byte_count = 1_usize
        .checked_add(count.saturating_mul(usize::from(width)).div_ceil(8))
        .ok_or_else(|| resource("a bit-packed stream"))?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(byte_count)
        .map_err(|_| resource("a bit-packed stream"))?;
    output.push(width);
    output.resize(byte_count, 0);

    let mut written = 0_usize;
    let mut position = 0_usize;
    for value in values {
        if written == count {
            return Err(invalid(
                "a bit-packed stream has more values than it declares",
            ));
        }
        written += 1;
        if width == 0 {
            // Section 9.2: width zero is the whole encoding of an all-zero
            // stream, so there is nothing to place but the values still count.
            continue;
        }
        for bit in 0..usize::from(width) {
            if value & (1_u64 << bit) != 0 {
                let target = position + bit;
                output[1 + target / 8] |= 1 << (target % 8);
            }
        }
        position += usize::from(width);
    }
    if written != count {
        return Err(invalid(
            "a bit-packed stream has fewer values than it declares",
        ));
    }
    Ok(output)
}

/// Section 9.7: a `uint32` run count, LSB-first run value bits, then packed
/// positive run lengths.
pub(super) fn boolean_rle(values: &[bool]) -> Result<Vec<u8>> {
    let mut run_values: Vec<bool> = Vec::new();
    let mut run_lengths: Vec<u64> = Vec::new();
    for value in values {
        if run_values.last() == Some(value) {
            *run_lengths.last_mut().expect("a run value has a length") += 1;
            continue;
        }
        run_values
            .try_reserve(1)
            .map_err(|_| resource("boolean run values"))?;
        run_lengths
            .try_reserve(1)
            .map_err(|_| resource("boolean run lengths"))?;
        run_values.push(*value);
        run_lengths.push(1);
    }

    let count = u32::try_from(run_values.len()).map_err(|_| resource("a boolean run count"))?;
    let mut output = Vec::new();
    output
        .try_reserve(4)
        .map_err(|_| resource("a boolean RLE stream"))?;
    output.extend_from_slice(&count.to_le_bytes());
    if run_values.is_empty() {
        // A stream with no runs is the four count bytes and nothing else, which
        // is the only form a reader accepts for an empty stream.
        return Ok(output);
    }
    push_bytes(&mut output, &pack_booleans(&run_values))?;
    let width = bit_width(run_lengths.iter().copied().max().unwrap_or(0));
    push_bytes(
        &mut output,
        &pack_from(width, run_lengths.len(), run_lengths.iter().copied())?,
    )?;
    Ok(output)
}

/// The transformed length of a bit-packed stream, including its width byte.
fn packed_length(count: usize, width: u8) -> u64 {
    1 + u64::try_from(count.saturating_mul(usize::from(width)).div_ceil(8)).unwrap_or(u64::MAX)
}

fn bit_width(value: u64) -> u8 {
    (u64::BITS - value.leading_zeros()) as u8
}

/// The bit width of a value the caller has already established is nonnegative.
fn bit_width_of(value: i128) -> u8 {
    u64::try_from(value).map(bit_width).unwrap_or(64)
}

fn resource(what: &str) -> Error {
    Error::resource_limit(format!("unable to allocate {what}"), None)
}

fn invalid(what: &str) -> Error {
    Error::invalid_argument(what)
}

#[cfg(test)]
mod tests {
    use super::{
        DISTINCT_VALUE_LIMIT, PreparedValues, ValueEncoding, boolean_rle, candidates, encode,
        estimate, pack_from, prepare,
    };
    use crate::array::ScalarValue;
    use crate::codec::transform;
    use crate::schema::LogicalType;

    fn prepared(
        logical_type: &LogicalType,
        values: Vec<ScalarValue<'_>>,
    ) -> (PreparedValues, super::ValueProfile) {
        prepare(logical_type, values.into_iter().map(Ok)).expect("well-formed dense values")
    }

    #[test]
    fn packed_values_are_lsb_first() {
        assert_eq!(
            pack_from(3, 2, [2_u64, 4].into_iter()).expect("packs"),
            vec![3, 0x22]
        );
    }

    #[test]
    fn a_zero_width_packed_stream_is_only_its_width_byte() {
        assert_eq!(
            pack_from(0, 5, [0_u64; 5].into_iter()).expect("packs"),
            vec![0]
        );
    }

    #[test]
    fn boolean_rle_has_the_reader_order() {
        assert_eq!(
            boolean_rle(&[true, true, false]).expect("encodes"),
            vec![2, 0, 0, 0, 1, 2, 6]
        );
    }

    #[test]
    fn an_empty_boolean_rle_stream_is_only_its_run_count() {
        assert_eq!(boolean_rle(&[]).expect("encodes"), vec![0, 0, 0, 0]);
        assert_eq!(
            transform::booleans(&boolean_rle(&[]).expect("encodes"), 0, 6, "empty")
                .expect("the reader accepts an empty boolean RLE stream"),
            Vec::<bool>::new()
        );
    }

    #[test]
    fn writer_candidates_use_reader_transform_contracts() {
        let (integers, profile) = prepared(
            &LogicalType::Int64,
            vec![
                ScalarValue::Int64(-10),
                ScalarValue::Int64(-8),
                ScalarValue::Int64(-6),
                ScalarValue::Int64(-4),
            ],
        );
        for encoding in [
            ValueEncoding::Raw,
            ValueEncoding::BitPacked,
            ValueEncoding::FrameOfReference,
            ValueEncoding::Delta,
            ValueEncoding::DeltaOfDelta,
        ] {
            if let Ok(candidate) = encode(&LogicalType::Int64, &integers, &profile, encoding) {
                let stream = &candidate.streams[0];
                let decoded: Vec<i64> = transform::integer(
                    &stream.bytes,
                    stream.element_count,
                    8,
                    true,
                    stream.transform,
                    "writer candidate",
                )
                .expect("the reader accepts the writer's stream");
                assert_eq!(decoded, vec![-10_i64, -8, -6, -4]);
            }
        }

        let (floats, profile) = prepared(
            &LogicalType::Float64,
            vec![
                ScalarValue::Float64(f64::from_bits(0x8000_0000_0000_0000)),
                ScalarValue::Float64(f64::from_bits(0x7ff8_0000_0000_0042)),
                ScalarValue::Float64(1.25),
            ],
        );
        let candidate = encode(
            &LogicalType::Float64,
            &floats,
            &profile,
            ValueEncoding::ByteStreamSplit,
        )
        .expect("floats split");
        let stream = &candidate.streams[0];
        let restored = transform::byte_stream_split(
            &stream.bytes,
            stream.element_count,
            8,
            "writer byte-stream split",
        )
        .expect("the reader restores the split");
        let expected = [
            0x8000_0000_0000_0000_u64,
            0x7ff8_0000_0000_0042,
            1.25_f64.to_bits(),
        ]
        .into_iter()
        .flat_map(u64::to_le_bytes)
        .collect::<Vec<_>>();
        assert_eq!(restored, expected);

        let (booleans, profile) = prepared(
            &LogicalType::Bool,
            vec![
                ScalarValue::Bool(true),
                ScalarValue::Bool(true),
                ScalarValue::Bool(false),
                ScalarValue::Bool(false),
            ],
        );
        let candidate = encode(
            &LogicalType::Bool,
            &booleans,
            &profile,
            ValueEncoding::BooleanRle,
        )
        .expect("booleans run-length encode");
        let stream = &candidate.streams[0];
        assert_eq!(
            transform::booleans(
                &stream.bytes,
                stream.element_count,
                stream.transform,
                "writer boolean RLE",
            )
            .expect("the reader accepts the writer's runs"),
            vec![true, true, false, false]
        );
    }

    /// Every estimate is the exact transformed length of what `encode` emits,
    /// because selection compares estimates against each other before it
    /// compares materialized bytes against raw.
    #[test]
    fn every_estimate_matches_the_stream_it_prices() {
        let cases: Vec<(LogicalType, Vec<ScalarValue<'_>>)> = vec![
            (
                LogicalType::Int64,
                (0..9).map(|row| ScalarValue::Int64(row * 3)).collect(),
            ),
            (
                LogicalType::Int64,
                (0..9).map(|_| ScalarValue::Int64(-7)).collect(),
            ),
            (
                LogicalType::UInt32,
                (0..9).map(|row| ScalarValue::UInt32(row % 4)).collect(),
            ),
            (
                LogicalType::Int64,
                vec![ScalarValue::Int64(i64::MIN), ScalarValue::Int64(i64::MAX)],
            ),
            (
                LogicalType::Bool,
                (0..9).map(|row| ScalarValue::Bool(row % 3 == 0)).collect(),
            ),
            (
                LogicalType::Bool,
                (0..9).map(|_| ScalarValue::Bool(false)).collect(),
            ),
            (
                LogicalType::Float64,
                (0..9).map(|row| ScalarValue::Float64(row as f64)).collect(),
            ),
            (
                LogicalType::Utf8,
                (0..9).map(|_| ScalarValue::Utf8("abc")).collect(),
            ),
            (
                LogicalType::Utf8,
                (0..9).map(|_| ScalarValue::Utf8("")).collect(),
            ),
            (
                LogicalType::Binary,
                (0..9).map(|_| ScalarValue::Binary(b"xy")).collect(),
            ),
        ];

        for (logical_type, values) in cases {
            let (prepared, profile) = prepared(&logical_type, values);
            for candidate in candidates(&logical_type, &prepared, &profile) {
                let estimated = estimate(&logical_type, &prepared, &profile, candidate)
                    .expect("a listed candidate is priced");
                let encoded = encode(&logical_type, &prepared, &profile, candidate)
                    .expect("a listed candidate encodes");
                let actual: Vec<u64> = encoded
                    .streams
                    .iter()
                    .map(|stream| stream.bytes.len() as u64)
                    .collect();
                // Bit packing prices a boolean stream at one bit per value,
                // which an all-false stream beats; every other estimate is
                // exact, and an over-estimate can only cost a candidate its
                // place in the shortlist.
                if candidate == ValueEncoding::BitPacked
                    && matches!(prepared, PreparedValues::Bool(_))
                {
                    assert!(actual[0] <= estimated[0], "{logical_type:?} {candidate:?}");
                    continue;
                }
                assert_eq!(estimated, actual, "{logical_type:?} {candidate:?}");
            }
        }
    }

    #[test]
    fn a_stream_past_the_cardinality_cap_offers_no_dictionary() {
        let values: Vec<ScalarValue<'_>> = (0..=DISTINCT_VALUE_LIMIT as i64)
            .map(ScalarValue::Int64)
            .collect();
        let (prepared, profile) = prepared(&LogicalType::Int64, values);

        assert!(profile.dictionary.is_none());
        assert!(
            !candidates(&LogicalType::Int64, &prepared, &profile)
                .contains(&ValueEncoding::Dictionary)
        );
    }

    #[test]
    fn overflowing_differences_disqualify_only_the_transforms_they_reach() {
        let (prepared, profile) = prepared(
            &LogicalType::Int64,
            vec![
                ScalarValue::Int64(i64::MIN),
                ScalarValue::Int64(i64::MAX),
                ScalarValue::Int64(i64::MIN),
            ],
        );
        let offered = candidates(&LogicalType::Int64, &prepared, &profile);

        assert!(!offered.contains(&ValueEncoding::Delta));
        assert!(!offered.contains(&ValueEncoding::DeltaOfDelta));
        // The span still fits uint64, so section 9.3 still applies.
        assert!(offered.contains(&ValueEncoding::FrameOfReference));
    }

    /// A later in-range difference must not clear an earlier overflow.
    #[test]
    fn one_overflowing_difference_disqualifies_the_whole_stream() {
        let (prepared, profile) = prepared(
            &LogicalType::Int64,
            vec![
                ScalarValue::Int64(i64::MIN),
                ScalarValue::Int64(i64::MAX),
                ScalarValue::Int64(i64::MAX - 1),
                ScalarValue::Int64(i64::MAX - 2),
            ],
        );

        assert!(
            profile
                .numeric
                .as_ref()
                .expect("numeric")
                .delta_bits
                .is_none()
        );
        assert!(
            !candidates(&LogicalType::Int64, &prepared, &profile).contains(&ValueEncoding::Delta)
        );
    }

    #[test]
    fn an_encoding_that_does_not_describe_its_values_is_refused_rather_than_panicking() {
        let (prepared, profile) = prepared(&LogicalType::Utf8, vec![ScalarValue::Utf8("a")]);

        assert!(
            encode(
                &LogicalType::Utf8,
                &prepared,
                &profile,
                ValueEncoding::Delta
            )
            .is_err()
        );
        assert!(
            encode(
                &LogicalType::Utf8,
                &prepared,
                &profile,
                ValueEncoding::BooleanRle
            )
            .is_err()
        );
    }
}
