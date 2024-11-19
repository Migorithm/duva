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

use crate::adapters::persistence::bytes_handler::BytesEndec;
use anyhow::Result;

use rdb_file_loader::RdbFileLoader;
use std::collections::HashMap;
use std::ops::RangeInclusive;
pub mod bytes_handler;
pub mod data_encoder;

mod rdb_file_loader;

pub struct RdbFile {
    header: String,
    metadata: HashMap<String, String>,
    database: Vec<DatabaseSection>,
    checksum: Vec<u8>,
}

impl RdbFile {
    pub fn new(data: Vec<u8>) -> Result<Self> {
        let loader = RdbFileLoader::new(data);
        loader.load_header()?.load_metadata()?.load_database()
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

pub struct DatabaseSectionBuilder<'a> {
    index: usize,
    storage: Vec<KeyValueStorage>,
    data: &'a mut BytesEndec,
    key_value_table_size: usize,
    expires_table_size: usize,
}

impl DatabaseSectionBuilder<'_> {
    pub fn new(data: &mut BytesEndec) -> DatabaseSectionBuilder {
        DatabaseSectionBuilder {
            data,
            index: Default::default(),
            storage: Default::default(),
            key_value_table_size: Default::default(),
            expires_table_size: Default::default(),
        }
    }
    pub fn extract_section(mut self) -> Result<DatabaseSection> {
        const SECTION_INDEX_IDENTIFIER: u8 = 0xFE; // 0b11111110
        const TABLE_SIZE_IDENTIFIER: u8 = 0xFB; //0b11111011

        while self.data.len() > 0 {
            match self.data[0] {
                SECTION_INDEX_IDENTIFIER => {
                    self.try_set_index()?;
                }

                TABLE_SIZE_IDENTIFIER => {
                    self.try_set_table_sizes()?;
                }
                _ => {
                    // ! as long as key_value_table_size is not 0 key value is extractable?
                    if self.key_value_table_size > 0 {
                        self.save_key_value_expiry_time_in_storage()?;
                        self.key_value_table_size -= 1;
                    } else {
                        break;
                    }
                }
            }
        }
        Ok(DatabaseSection {
            index: self.index,
            storage: self.storage,
        })
    }

    fn try_set_index(&mut self) -> Result<()> {
        self.data.remove_identifier();
        self.index = self.data.try_size_decode()?;
        Ok(())
    }

    fn try_set_table_sizes(&mut self) -> Result<()> {
        self.data.remove_identifier();
        self.key_value_table_size = self.data.try_size_decode()?;
        self.expires_table_size = self.data.try_size_decode()?;
        Ok(())
    }

    fn save_key_value_expiry_time_in_storage(&mut self) -> Result<()> {
        let key_value = KeyValueStorage::try_from(&mut *self.data)?;

        // ? How is expiry related to expired_table_size
        if key_value.expiry.is_some() {
            if let Some(existing_minus_one) = self.expires_table_size.checked_sub(1) {
                self.expires_table_size = existing_minus_one;
            } else {
                return Err(anyhow::anyhow!("expires_table_size is 0"));
            }
        }
        self.storage.push(key_value);

        Ok(())
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

impl TryFrom<&mut BytesEndec> for KeyValueStorage {
    type Error = anyhow::Error;
    fn try_from(data: &mut BytesEndec) -> Result<Self> {
        let mut expiry: Option<u64> = None;
        while data.len() > 0 {
            match data[0] {
                //0b11111100
                0xFC => {
                    expiry = Some(data.try_extract_expiry_time_in_milliseconds()?);
                }
                //0b11111101
                0xFD => {
                    expiry = Some(data.try_extract_expiry_time_in_seconds()?);
                }
                //0b11111110
                0x00 => {
                    let (key, value) = data.try_extract_key_value()?;
                    return Ok(KeyValueStorage { key, value, expiry });
                }
                _ => {
                    return Err(anyhow::anyhow!("Invalid key value pair"));
                }
            }
        }
        Err(anyhow::anyhow!("Invalid key value pair"))
    }
}

// Safe conversion from a slice to an array of a specific size.
fn extract_range<const N: usize>(encoded: &[u8], range: RangeInclusive<usize>) -> Option<[u8; N]> {
    TryInto::<[u8; N]>::try_into(encoded.get(range)?).ok()
}

#[test]
fn test_database_section_extractor() {
    let mut data = vec![
        0xFE, 0x00, 0xFB, 0x03, 0x02, 0x00, 0x06, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, 0x06, 0x62,
        0x61, 0x7A, 0x71, 0x75, 0x78, 0xFC, 0x15, 0x72, 0xE7, 0x07, 0x8F, 0x01, 0x00, 0x00, 0x00,
        0x03, 0x66, 0x6F, 0x6F, 0x03, 0x62, 0x61, 0x72, 0xFD, 0x52, 0xED, 0x2A, 0x66, 0x00, 0x03,
        0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78,
    ]
    .into();

    let db_section: DatabaseSection = DatabaseSectionBuilder::new(&mut data)
        .extract_section()
        .unwrap();
    assert_eq!(db_section.index, 0);
    assert_eq!(db_section.storage.len(), 3);

    assert_eq!(data.len(), 0);
}

#[test]
fn test_non_expiry_key_value_pair() {
    let mut data = vec![0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78].into();

    let key_value =
        KeyValueStorage::try_from(&mut data).expect("Failed to extract key value expiry");
    assert_eq!(key_value.key, "baz");
    assert_eq!(key_value.value, "qux");
    assert_eq!(key_value.expiry, None);
    assert_eq!(data.len(), 0);
}

#[test]
fn test_with_milliseconds_expiry_key_value_pair() {
    let mut data = vec![
        0xFC, 0x15, 0x72, 0xE7, 0x07, 0x8F, 0x01, 0x00, 0x00, 0x00, 0x03, 0x62, 0x61, 0x7A, 0x03,
        0x71, 0x75, 0x78,
    ]
    .into();

    let key_value = KeyValueStorage::try_from(&mut data).unwrap();

    assert_eq!(key_value.key, "baz");
    assert_eq!(key_value.value, "qux");
    assert!(key_value.expiry.is_some());
    assert_eq!(data.len(), 0);
}

#[test]
fn test_with_seconds_expiry_key_value_pair() {
    let mut data = vec![
        0xFD, 0x52, 0xED, 0x2A, 0x66, 0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78,
    ]
    .into();

    let key_value = KeyValueStorage::try_from(&mut data).unwrap();
    assert_eq!(key_value.key, "baz");
    assert_eq!(key_value.value, "qux");
    assert!(key_value.expiry.is_some());
    assert_eq!(data.len(), 0);
}

#[test]
fn test_invalid_expiry_key_value_pair() {
    let mut data = vec![
        0xFF, 0x52, 0xED, 0x2A, 0x66, 0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78,
    ]
    .into();

    let result = KeyValueStorage::try_from(&mut data);
    assert!(result.is_err());
    assert_eq!(data.len(), 14);
}
