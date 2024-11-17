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
use crate::{from_to, make_smart_pointer};
use anyhow::{Error, Result};
use key_value_storage_extractor::KeyValueStorage;
use std::collections::HashMap;
use std::ops::RangeInclusive;
use bytes_handler::BytesHandler;
use crate::adapters::persistence::database_extractor::{DatabaseSection, DatabaseSectionBuilder};

mod database_extractor;
mod key_value_storage_extractor;
pub mod size_encoding;
mod bytes_handler;

const RDB_HEADER_MAGIC_STRING: &str = "REDIS";

pub struct RdbFile {
    header: String,
    metadata: HashMap<String, String>,
    database: Vec<DatabaseSection>,
}

#[derive(Default)]
struct RdbFileLoader<T=HeaderLoading> {
    data: BytesHandler,
    state: T,
    header: Option<String>,
    metadata: Option<HashMap<String, String>>,
    database: Option<Vec<DatabaseSection>>,
}
struct HeaderLoading;
struct MetadataSectionLoading;
struct DatabaseSectionLoading;

impl RdbFileLoader {
    fn new(data: Vec<u8>) -> Self {
        Self {
            data: BytesHandler(data),
            state: HeaderLoading,
            header: None,
            metadata: None,
            database: None,
        }
    }
    // read data and check first 5 ascii code convertable hex bytes are equal to "REDIS"
    // then read 4 digit Header version (like 0011) and return RdbFileLoader<MetadataSectionLoading> with header value as "REDIS" + 4 digit version
    fn load_header(mut self) -> Result<RdbFileLoader<MetadataSectionLoading>> {
        if self.data.len() < 9 {
            return Err(create_error_while_loading("header loading", "data length is less than 9"));
        }
        let header = String::from_utf8(self.data.drain(0..5).collect())?;
        if header != RDB_HEADER_MAGIC_STRING {
            return Err(create_error_while_loading("header loading", "header is not REDIS"));
        }
        let version = String::from_utf8(self.data.drain(0..4).collect())?;
        self.header = Some(format!("{}{}", header, version));
        Ok(RdbFileLoader {
            data: self.data,
            state: MetadataSectionLoading,
            header: self.header,
            metadata: None,
            database: None,
        })
    }
}

fn create_error_while_loading(state: &str, message:&str) -> Error {
    (anyhow::anyhow!("Error occurred while {}:{}", state, message))
}

#[test]
fn test_header_loading() {
    let data = vec![0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x31];
    let loader = RdbFileLoader::new(data);
    let loader = loader.load_header().unwrap();
    assert_eq!(loader.header, Some("REDIS0001".to_string()));
}

#[test]
fn test_header_loading_data_length_error() {
    let data = vec![0x52, 0x45, 0x44, 0x49, 0x53];
    let loader = RdbFileLoader::new(data);
    let result = loader.load_header();
    assert!(result.is_err());
}

#[test]
fn test_header_loading_header_error() {
    let data = vec![0x52, 0x45, 0x44, 0x49, 0x54, 0x30, 0x30, 0x30, 0x31];
    let loader = RdbFileLoader::new(data);
    let result = loader.load_header();
    assert!(result.is_err());
}

impl RdbFileLoader<MetadataSectionLoading>{
    fn is_metadata_section(&self) -> bool {
        let identifier = self.data.get(0);
        identifier == Some(&0xFA)
    }
    fn load_metadata(mut self) -> Result<RdbFileLoader<DatabaseSectionLoading>> {
        let mut metadata = HashMap::new();
        while self.is_metadata_section() {
            let (key,value) = self.data.try_extract_key_value()?;
            metadata.insert(key, value);
        }
        self.metadata = Some(metadata);
        Ok(RdbFileLoader {
            data: self.data,
            state: DatabaseSectionLoading,
            header: self.header,
            metadata: self.metadata,
            database: None,
        })
    }
}

#[test]
fn test_metadata_loading() {
    let data = vec![0xFA, 0x03, 0x61, 0x62, 0x63, 0x03, 0x64, 0x65, 0x66];
    let loader = RdbFileLoader {
        data: BytesHandler(data),
        state: MetadataSectionLoading,
        header: Some("REDIS0001".to_string()),
        metadata: None,
        database: None,
    };
    let loader = loader.load_metadata().unwrap();
    let metadata = loader.metadata.unwrap();
    assert_eq!(metadata.get("abc"), Some(&"def".to_string()));
}

#[test]
fn test_metadata_loading_multiple() {
    let data = vec![
        0xFA, 0x03, 0x61, 0x62, 0x63, 0x03, 0x64, 0x65, 0x66, 0xFA, 0x03, 0x67, 0x68, 0x69,
        0x03, 0x6A, 0x6B, 0x6C,
    ];
    let loader = RdbFileLoader {
        data: BytesHandler(data),
        state: MetadataSectionLoading,
        header: Some("REDIS0001".to_string()),
        metadata: None,
        database: None,
    };
    let loader = loader.load_metadata().unwrap();
    let metadata = loader.metadata.unwrap();
    assert_eq!(metadata.get("abc"), Some(&"def".to_string()));
    assert_eq!(metadata.get("ghi"), Some(&"jkl".to_string()));
}

#[test]
fn test_metadata_loading_no_metadata() {
    let data = vec![0xFE, 0x00, 0xFB, 0x03, 0x02, 0x00, 0x06, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, 0x06, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78];
    let loader = RdbFileLoader {
        data: BytesHandler(data),
        state: MetadataSectionLoading,
        header: Some("REDIS0001".to_string()),
        metadata: None,
        database: None,
    };
    let loader = loader.load_metadata().unwrap();
    assert_eq!(loader.metadata, Some(HashMap::new()));
}

impl RdbFileLoader<DatabaseSectionLoading> {
    fn is_database_section(&self) -> bool {
        let identifier = self.data.get(0);
        identifier == Some(&0xFE)
    }
    fn load_database(mut self) -> Result<RdbFile> {
        let mut database = Vec::new();
        while self.is_database_section() {
            let section = DatabaseSectionBuilder::new(&mut self.data);
            let section = section.extract_section()?;
            database.push(section);
        }
        Ok(RdbFile {
            header: self.header.unwrap(),
            metadata: self.metadata.unwrap(),
            database,
        })
    }
}

make_smart_pointer!(BytesHandler, Vec<u8>);
from_to!(Vec<u8>, BytesHandler);

// Safe conversion from a slice to an array of a specific size.
fn extract_range<const N: usize>(encoded: &[u8], range: RangeInclusive<usize>) -> Option<[u8; N]> {
    TryInto::<[u8; N]>::try_into(encoded.get(range)?).ok()
}