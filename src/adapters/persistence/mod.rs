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
use key_value_storage_extractor::KeyValueStorage;
use rdb_file_loader::RdbFileLoader;
use std::collections::HashMap;
use std::ops::RangeInclusive;
pub mod bytes_handler;
pub mod data_encoder;
mod key_value_storage_extractor;
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

    // TODO : subject to refactor
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
        while self.data.len() > 0 {
            match self.data[0] {
                // 0b11111110
                0xFE => {
                    self.try_set_index()?;
                }
                //0b11111011
                0xFB => {
                    self.try_set_table_sizes()?;
                }
                _ => {
                    if self.is_key_value_extractable() {
                        self.save_key_value_expiry_time_in_storage()?;
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

    // ! as long as key_value_table_size is not 0 key value is extractable?
    fn is_key_value_extractable(&self) -> bool {
        self.key_value_table_size > 0
    }

    fn save_key_value_expiry_time_in_storage(&mut self) -> Result<()> {
        let key_value = KeyValueStorage::try_from(&mut *self.data)?;

        if key_value.expiry.is_some() {
            if let Some(existing_minus_one) = self.expires_table_size.checked_sub(1) {
                self.expires_table_size = existing_minus_one;
            } else {
                return Err(anyhow::anyhow!("expires_table_size is 0"));
            }
        }
        self.storage.push(key_value);
        self.key_value_table_size -= 1;
        Ok(())
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
