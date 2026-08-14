//! CRC32C (Castagnoli), the checksum behind every Acta v0.2 CRC field.

use ::crc32c as implementation;

/// A streaming CRC32C accumulator.
///
/// The state is one word, so frame validation can checksum arbitrarily large
/// file regions without buffering them. The dependency selects the hardware
/// implementation when the running CPU supports it and otherwise falls back
/// to software.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Crc32c {
    checksum: u32,
}

impl Crc32c {
    pub(crate) fn new() -> Self {
        Self { checksum: 0 }
    }

    pub(crate) fn update(&mut self, bytes: &[u8]) {
        self.checksum = implementation::crc32c_append(self.checksum, bytes);
    }

    pub(crate) fn finish(self) -> u32 {
        self.checksum
    }
}

pub(crate) fn checksum(bytes: &[u8]) -> u32 {
    implementation::crc32c(bytes)
}

#[cfg(test)]
mod tests {
    use super::{Crc32c, checksum};

    #[test]
    fn matches_the_standard_crc32c_check_value() {
        assert_eq!(checksum(b"123456789"), 0xe306_9283);
    }

    #[test]
    fn matches_the_rfc_3720_all_zero_vector() {
        assert_eq!(checksum(&[0x00; 32]), 0x8a91_36aa);
    }

    #[test]
    fn matches_the_rfc_3720_all_one_vector() {
        assert_eq!(checksum(&[0xff; 32]), 0x62a8_ab43);
    }

    #[test]
    fn matches_the_rfc_3720_incrementing_vector() {
        let mut bytes = [0_u8; 32];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = index as u8;
        }
        assert_eq!(checksum(&bytes), 0x46dd_794e);
    }

    #[test]
    fn matches_the_rfc_3720_decrementing_vector() {
        let mut bytes = [0_u8; 32];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = 31 - index as u8;
        }
        assert_eq!(checksum(&bytes), 0x113f_db5c);
    }

    #[test]
    fn the_empty_input_checksums_to_zero() {
        assert_eq!(checksum(&[]), 0);
    }

    #[test]
    fn streaming_in_chunks_matches_a_single_update() {
        let bytes: Vec<u8> = (0..=255_u8).cycle().take(1000).collect();

        let mut streamed = Crc32c::new();
        for chunk in bytes.chunks(7) {
            streamed.update(chunk);
        }

        assert_eq!(streamed.finish(), checksum(&bytes));
    }
}
