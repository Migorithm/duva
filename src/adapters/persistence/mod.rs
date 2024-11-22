//! A module providing variable-length size encoding followed by arbitrary data.
//!
//! This module implements an encoding scheme that can encode a size value using a variable
//! number of bytes based on the magnitude of the size, followed by arbitrary data bytes.
//! The size encoding uspub pub pub pub es a prefix to indicate how many bytes are used to represent the size:
//!
//! # Size Encoding Format
//!
//! The size value is encoded using one of three formats, chosen based on the value's magnitude:
//!
//! * 6-bit (1 byte total):  `00xxxxxx`
//!   - First 2 bits are `00`
//!   - Next 6 bits contain size value
//!   - Can encode sizes 0-63
//!
//! * 14-bit (2 bytes total): `01xxxxxx yyyyyyyy`
//!   - First 2 bits are `01`
//!   - Next 14 bits contain size value in big-endian order
//!   - Can encode sizes 64-16383
//!
//! * 32-bit (5 bytes total): `10000000 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx`
//!   - First 2 bits are `10`
//!   - Next 4 bytes contain size value in big-endian order
//!   - Can encode sizes 16384-4294967295
//!
//! After the size encoding, the arbitrary data bytes follow immediately.
//!
//! # Examples
//!
//! Basic usage:
//! ```rust
//! # fn main() -> Option<()> {
//! let data = "Hello".as_bytes();
//! let size = 42;
//!
//! // Encode size 42 (fits in 6 bits) followed by "Hello"
//! let encoded = size_encode(size, data)?;
//!
//! // First byte should be 00101010 (0b00 prefix + 42)
//! assert_eq!(encoded[0], 0b00101010);
//!
//! // Remaining bytes should be "Hello"
//! assert_eq!(&encoded[1..], b"Hello");
//! # Some(())
//! # }
//! ```
//!
//! The benefit is that size_encode allows you to encode any size value independently of the data length. This is useful when:
//!
//! 1. You're sending a header for a larger message:
//! ```rust,text
//! // First message part
//! let header = size_encode(1000, b"start").unwrap();
//! send(header);  // Sends: size=1000 + data="start"
//! // Later messages
//! send(b"more data");  // Just data
//! send(b"final part"); // Just data
//! ```
//!
//! It's primarily about communication/protocol rather than efficiency.\
use std::collections::HashMap;
use std::ops::RangeInclusive;

pub mod byte_encoder;

pub mod byte_decoder;

#[derive(Default)]
pub struct Init;
#[derive(Default, PartialEq, Eq, Debug)]
pub struct HeaderReady(String);
#[derive(Default)]
pub struct MetadataReady {
    metadata: HashMap<String, String>,
    header: String,
}

pub struct RdbFile {
    header: String,
    metadata: HashMap<String, String>,
    database: Vec<DatabaseSection>,
    checksum: Vec<u8>,
}

impl RdbFile {
    pub fn new(
        header: String,
        metadata: HashMap<String, String>,
        database: Vec<DatabaseSection>,
        checksum: Vec<u8>,
    ) -> Self {
        Self {
            header,
            metadata,
            database,
            checksum,
        }
    }
    pub fn key_values(self) -> Vec<KeyValueStorage> {
        self.database
            .into_iter()
            .flat_map(|section| section.storage.into_iter())
            .collect()
    }
}

pub struct DatabaseSection {
    pub index: usize,
    pub storage: Vec<KeyValueStorage>,
}

#[derive(Default)]
pub struct DatabaseSectionBuilder {
    index: usize,
    storage: Vec<KeyValueStorage>,
    key_value_table_size: usize,
    expires_table_size: usize,
}
impl DatabaseSectionBuilder {
    pub fn build(self) -> DatabaseSection {
        DatabaseSection {
            index: self.index,
            storage: self.storage,
        }
    }
}

/// # Extract Key-Value Pair Storage
/// Extract key-value pair from the data buffer and remove the extracted data from the buffer.
///
/// Each key-value pair is stored as follows:
///
/// 1. Optional Expire Information:
/// - **Timestamp in Seconds:**
/// ```
/// FD
/// Expire timestamp in seconds (4-byte unsigned integer)
/// ```
/// - **Timestamp in Milliseconds:**
/// ```
/// FC
/// Expire timestamp in milliseconds (8-byte unsigned long)
/// ```
/// 2. **Value Type:** 1-byte flag indicating the type and encoding of the value.
/// 3. **Key:** String encoded.
/// 4. **Value:** Encoding depends on the value type.
pub struct KeyValueStorage {
    pub key: String,
    pub value: String,
    pub expiry: Option<u64>,
}

// Safe conversion from a slice to an array of a specific size.
fn extract_range<const N: usize>(encoded: &[u8], range: RangeInclusive<usize>) -> Option<[u8; N]> {
    TryInto::<[u8; N]>::try_into(encoded.get(range)?).ok()
}
