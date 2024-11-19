use super::{extract_range, DatabaseSection, Init, KeyValueStorage, MetadataReady, RdbFile};
use crate::adapters::persistence::{self, DatabaseSectionBuilder, HeaderReady};
use anyhow::{Context, Result};
use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
};

#[derive(Default)]
pub struct BytesEndec<T> {
    pub data: Vec<u8>,
    pub state: T,
}

/// General purpose BytesEndec implementation
impl<T> BytesEndec<T> {
    pub fn remove_identifier(&mut self) {
        self.data.remove(0);
    }

    // Decode a size-encoded value based on the first two bits and return the decoded value as a string.
    pub fn string_decode(&mut self) -> Option<String> {
        // Ensure we have at least one byte to read.
        if self.is_empty() {
            return None;
        }

        if let Some(size) = self.size_decode() {
            if size > self.len() {
                return None;
            }
            let data = String::from_utf8(self.drain(0..size).collect()).ok()?;
            Some(data)
        } else {
            self.integer_decode()
        }
    }
    pub fn size_decode(&mut self) -> Option<usize> {
        if let Some(first_byte) = self.get(0) {
            match first_byte >> 6 {
                0b00 => {
                    let size = (first_byte & 0x3F) as usize;
                    self.drain(0..1);
                    Some(size)
                }
                0b01 => {
                    if self.len() < 2 {
                        return None;
                    }
                    let size = (((first_byte & 0x3F) as usize) << 8) | (self[1] as usize);
                    self.drain(0..2);
                    Some(size)
                }
                0b10 => {
                    if self.len() < 5 {
                        return None;
                    }
                    let size = ((self[1] as usize) << 24)
                        | ((self[2] as usize) << 16)
                        | ((self[3] as usize) << 8)
                        | (self[4] as usize);
                    self.drain(0..5);
                    Some(size)
                }
                _ => None,
            }
        } else {
            None
        }
    }

    fn integer_decode(&mut self) -> Option<String> {
        if let Some(first_byte) = self.get(0) {
            match first_byte {
                // 0b11000000: 8-bit integer
                0xC0 => {
                    let value = u8::from_le_bytes([self[1]]).to_string();
                    self.drain(0..2);
                    return Some(value);
                }
                0xC1 => {
                    if self.len() == 3 {
                        let value = u16::from_le_bytes(persistence::extract_range(self, 1..=2)?)
                            .to_string();
                        self.drain(0..3);
                        return Some(value);
                    }
                }
                0xC2 => {
                    if self.len() == 5 {
                        let value = u32::from_le_bytes(persistence::extract_range(self, 1..=4)?)
                            .to_string();
                        self.drain(0..5);
                        return Some(value);
                    }
                }
                _ => return None,
            }
        }
        None
    }

    pub(crate) fn check_identifier(&self, iden: u8) -> bool {
        self.get(0) == Some(&iden)
    }
}

impl BytesEndec<Init> {
    // read data and check first 5 ascii code convertable hex bytes are equal to "REDIS"
    // then read 4 digit Header version (like 0011) and return RdbFileLoader<MetadataSectionLoading> with header value as "REDIS" + 4 digit version
    pub fn load_header(mut self) -> Result<BytesEndec<HeaderReady>> {
        const RDB_HEADER_MAGIC_STRING: &str = "REDIS";

        if self.len() < 9 {
            return Err(anyhow::Error::msg(
                "header loading: data length is less than 9",
            ))?;
        }
        let header = String::from_utf8(self.drain(0..5).collect())?;
        if header != RDB_HEADER_MAGIC_STRING {
            return Err(anyhow::Error::msg("header loading: header is not REDIS"))?;
        }
        let version = String::from_utf8(self.drain(0..4).collect());
        if version.is_err() {
            return Err(anyhow::Error::msg("header loading: version is not valid"))?;
        }
        Ok(BytesEndec {
            data: self.data,
            state: HeaderReady(format!("{}{}", header, version?)),
        })
    }
}

impl BytesEndec<HeaderReady> {
    pub fn load_metadata(mut self) -> Result<BytesEndec<MetadataReady>> {
        const METADATA_SECTION_IDENTIFIER: u8 = 0xFA;

        let mut metadata = HashMap::new();
        while self.check_identifier(METADATA_SECTION_IDENTIFIER) {
            let (key, value) = self
                .try_extract_metadata_key_value()
                .context("metadata loading: key value extraction failed")?;
            metadata.insert(key, value);
        }
        Ok(BytesEndec {
            data: self.data,
            state: MetadataReady {
                metadata,
                header: self.state.0,
            },
        })
    }
    pub fn try_extract_metadata_key_value(&mut self) -> anyhow::Result<(String, String)> {
        self.remove_identifier();
        let key_data = self.string_decode().context("key decode fail")?;
        let value_data = self.string_decode().context("value decode fail")?;

        Ok((key_data, value_data))
    }
}
impl BytesEndec<MetadataReady> {
    pub fn load_database(mut self) -> Result<RdbFile> {
        const DATABASE_SECTION_IDENTIFIER: u8 = 0xFE;
        let mut database = Vec::new();
        while self.check_identifier(DATABASE_SECTION_IDENTIFIER) {
            let section = self
                .extract_section()
                .context("database loading: section extraction failed")?;
            database.push(section);
        }

        let checksum = self.try_get_checksum()?;
        Ok(RdbFile {
            header: self.state.header,
            metadata: self.state.metadata,
            database,
            checksum,
        })
    }
    pub fn extract_section(&mut self) -> Result<DatabaseSection> {
        const SECTION_INDEX_IDENTIFIER: u8 = 0xFE; // 0b11111110
        const TABLE_SIZE_IDENTIFIER: u8 = 0xFB; //0b11111011

        let mut builder: DatabaseSectionBuilder = DatabaseSectionBuilder::default();

        while let Some(identifier) = self.first() {
            match *identifier {
                SECTION_INDEX_IDENTIFIER => {
                    self.try_set_index(&mut builder)?;
                }

                TABLE_SIZE_IDENTIFIER => {
                    self.try_set_table_sizes(&mut builder)?;
                }
                _ => {
                    if self.should_stop_extending_storage(&mut builder)? {
                        break;
                    }
                }
            }
        }
        Ok(builder.build())
    }

    fn should_stop_extending_storage(
        &mut self,
        builder: &mut DatabaseSectionBuilder,
    ) -> Result<bool> {
        // ! as long as key_value_table_size is not 0 key value is extractable?
        if builder.key_value_table_size == 0 {
            return Ok(true); // No more keys to extract
        }
        let key_value = self.try_key_value()?;
        if key_value.expiry.is_some() {
            builder.expires_table_size = builder
                .expires_table_size
                .checked_sub(1)
                .context("expires_table_size is 0")?;
        }
        builder.storage.push(key_value);
        builder.key_value_table_size -= 1;
        Ok(false) // Continue processing
    }

    fn try_set_index(&mut self, builder: &mut DatabaseSectionBuilder) -> Result<()> {
        self.remove_identifier();
        builder.index = self.size_decode().context("size decode fail")?;
        Ok(())
    }

    fn try_set_table_sizes(&mut self, builder: &mut DatabaseSectionBuilder) -> Result<()> {
        self.remove_identifier();
        (builder.key_value_table_size, builder.expires_table_size) = (
            self.size_decode().context("size decode fail")?,
            self.size_decode().context("size decode fail")?,
        );
        Ok(())
    }

    fn try_key_value(&mut self) -> Result<KeyValueStorage> {
        let mut expiry: Option<u64> = None;
        while self.len() > 0 {
            match self[0] {
                //0b11111100
                0xFC => {
                    expiry = Some(self.try_extract_expiry_time_in_milliseconds()?);
                }
                //0b11111101
                0xFD => {
                    expiry = Some(self.try_extract_expiry_time_in_seconds()?);
                }
                //0b11111110
                0x00 => {
                    let (key, value) = self.try_extract_key_value()?;
                    return Ok(KeyValueStorage { key, value, expiry });
                }
                _ => {
                    return Err(anyhow::anyhow!("Invalid key value pair"));
                }
            }
        }
        Err(anyhow::anyhow!("Invalid key value pair"))
    }

    pub fn try_extract_expiry_time_in_seconds(&mut self) -> Result<u64> {
        self.remove_identifier();
        let range = 0..=3;
        let result = u32::from_le_bytes(
            persistence::extract_range(self, range.clone())
                .context("Failed to extract expiry time in seconds")?,
        );
        self.drain(range);

        Ok(result as u64)
    }

    pub fn try_extract_expiry_time_in_milliseconds(&mut self) -> Result<u64> {
        self.remove_identifier();
        let range = 0..=7;
        let result = u64::from_le_bytes(
            persistence::extract_range(self, range.clone())
                .context("Failed to extract expiry time in milliseconds")?,
        );
        self.drain(range);
        Ok(result)
    }

    pub fn try_extract_key_value(&mut self) -> Result<(String, String)> {
        self.remove_identifier();
        let key_data = self.string_decode().context("key decode fail")?;
        let value_data = self.string_decode().context("value decode fail")?;

        Ok((key_data, value_data))
    }

    pub fn try_get_checksum(&mut self) -> Result<Vec<u8>> {
        self.remove_identifier();
        let checksum = extract_range(&self.data, 0..=7)
            .map(|f: [u8; 8]| f.to_vec())
            .context("failed to extract checksum")?;
        self.data.drain(..8);
        Ok(checksum)
    }
}

impl From<Vec<u8>> for BytesEndec<Init> {
    fn from(data: Vec<u8>) -> Self {
        Self { data, state: Init }
    }
}

impl<T> Deref for BytesEndec<T> {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
impl<T> DerefMut for BytesEndec<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

#[test]
fn test_size_decoding() {
    let mut example1: BytesEndec<Init> = vec![0x0D].into();
    let mut example2: BytesEndec<Init> = vec![0x42, 0xBC].into();
    let mut example3: BytesEndec<Init> = vec![0x80, 0x00, 0x00, 0x42, 0x68].into();
    let mut example4: BytesEndec<Init> = vec![0xC0, 0x0A].into();

    assert_eq!(example1.size_decode(), Some(13));
    assert_eq!(example2.size_decode(), Some(700));
    assert_eq!(example3.size_decode(), Some(17000));
    assert_eq!(example4.size_decode(), None);
}

#[test]
fn test_integer_decoding() {
    let mut example1: BytesEndec<Init> = vec![0xC0, 0x0A].into();
    let mut example2: BytesEndec<Init> = vec![0xC1, 0x39, 0x30].into();
    let mut example3: BytesEndec<Init> = vec![0xC2, 0x87, 0xD6, 0x12, 0x00].into();

    assert_eq!(example1.integer_decode(), Some("10".to_string()));
    assert_eq!(example2.integer_decode(), Some("12345".to_string()));
    assert_eq!(example3.integer_decode(), Some("1234567".to_string()));
}

#[test]
fn test_string_decoding() {
    let mut example1: BytesEndec<Init> = vec![
        0x0D, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21,
    ]
    .into();
    let mut example2: BytesEndec<Init> = vec![0x42, 0x0A, 0x54, 0x65, 0x73, 0x74].into();

    assert_eq!(example1.string_decode(), Some("Hello, World!".to_string()));
    assert_eq!(example2.string_decode(), None);
}

#[test]
fn test_decoding() {
    // "Hello, World!"
    let mut example1: BytesEndec<Init> = vec![
        0x0D, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21,
    ]
    .into();

    // "Test", with size 10 (although more bytes needed)
    let mut example2: BytesEndec<Init> = vec![0x42, 0x0A, 0x54, 0x65, 0x73, 0x74].into();

    assert!(example1.string_decode().is_some());
    assert!(example2.string_decode().is_none()); // due to insufficient bytes
}

#[test]
fn test_decode_multiple_strings() {
    // "abc" and "def"
    let mut encoded: BytesEndec<Init> = vec![0x03, 0x61, 0x62, 0x63, 0x03, 0x64, 0x65, 0x66].into();
    let decoded = encoded.string_decode();
    assert_eq!(decoded, Some("abc".to_string()));
    let decoded = encoded.string_decode();
    assert_eq!(decoded, Some("def".to_string()));
}

#[test]
fn test_database_section_extractor() {
    let data = vec![
        0xFE, 0x00, 0xFB, 0x03, 0x02, 0x00, 0x06, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, 0x06, 0x62,
        0x61, 0x7A, 0x71, 0x75, 0x78, 0xFC, 0x15, 0x72, 0xE7, 0x07, 0x8F, 0x01, 0x00, 0x00, 0x00,
        0x03, 0x66, 0x6F, 0x6F, 0x03, 0x62, 0x61, 0x72, 0xFD, 0x52, 0xED, 0x2A, 0x66, 0x00, 0x03,
        0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78,
    ]
    .into();

    let mut bytes_handler = BytesEndec::<MetadataReady> {
        data,
        state: Default::default(),
    };

    let db_section: DatabaseSection = bytes_handler.extract_section().unwrap();
    assert_eq!(db_section.index, 0);
    assert_eq!(db_section.storage.len(), 3);
    assert_eq!(db_section.storage[0].key, "foobar");
    assert_eq!(db_section.storage[0].value, "bazqux");
    assert_eq!(db_section.storage[0].expiry, None);
    assert_eq!(db_section.storage[1].key, "foo");
    assert_eq!(db_section.storage[1].value, "bar");
    assert_eq!(db_section.storage[1].expiry, Some(1713824559637));
}

#[test]
fn test_non_expiry_key_value_pair() {
    let data = vec![0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78];
    let mut bytes_handler = BytesEndec::<MetadataReady> {
        data,
        state: Default::default(),
    };

    let key_value = bytes_handler
        .try_key_value()
        .expect("Failed to extract key value expiry");
    assert_eq!(key_value.key, "baz");
    assert_eq!(key_value.value, "qux");
    assert_eq!(key_value.expiry, None);
    assert!(bytes_handler.data.is_empty());
}

#[test]
fn test_with_milliseconds_expiry_key_value_pair() {
    let data = vec![
        0xFC, 0x15, 0x72, 0xE7, 0x07, 0x8F, 0x01, 0x00, 0x00, 0x00, 0x03, 0x62, 0x61, 0x7A, 0x03,
        0x71, 0x75, 0x78,
    ];
    let mut bytes_handler = BytesEndec::<MetadataReady> {
        data,
        state: Default::default(),
    };

    let key_value = bytes_handler.try_key_value().unwrap();

    assert_eq!(key_value.key, "baz");
    assert_eq!(key_value.value, "qux");
    assert!(key_value.expiry.is_some());
    assert!(bytes_handler.data.is_empty());
}

#[test]
fn test_with_seconds_expiry_key_value_pair() {
    let data = vec![
        0xFD, 0x52, 0xED, 0x2A, 0x66, 0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78,
    ];
    let mut bytes_handler = BytesEndec::<MetadataReady> {
        data,
        state: Default::default(),
    };

    let key_value = bytes_handler.try_key_value().unwrap();
    assert_eq!(key_value.key, "baz");
    assert_eq!(key_value.value, "qux");
    assert!(key_value.expiry.is_some());
}

#[test]
fn test_invalid_expiry_key_value_pair() {
    let data = vec![
        0xFF, 0x52, 0xED, 0x2A, 0x66, 0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78,
    ];
    let mut bytes_handler = BytesEndec::<MetadataReady> {
        data,
        state: Default::default(),
    };

    let result = bytes_handler.try_key_value();
    assert!(result.is_err());
    assert_eq!(bytes_handler.data.len(), 14);
}

#[test]
fn test_header_loading() {
    let data: BytesEndec<Init> = vec![0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x31].into();
    let header = data.load_header().unwrap();

    assert_eq!(header.state, HeaderReady("REDIS0001".to_string()));
}

#[test]
fn test_header_loading_data_length_error() {
    let data: BytesEndec<Init> = vec![0x52, 0x45, 0x44, 0x49, 0x53].into();

    let result = data.load_header();
    assert!(result.is_err());
}

#[test]
fn test_header_loading_header_error() {
    let data: BytesEndec<Init> = vec![0x52, 0x45, 0x44, 0x49, 0x54, 0x30, 0x30, 0x30, 0x31].into();

    let result = data.load_header();
    assert!(result.is_err());
}

#[test]
fn test_metadata_loading() {
    let data = vec![0xFA, 0x03, 0x61, 0x62, 0x63, 0x03, 0x64, 0x65, 0x66];
    let bytes_handler = BytesEndec::<HeaderReady> {
        data,
        state: HeaderReady("REDIS0001".to_string()),
    };

    let metadata = bytes_handler.load_metadata().unwrap();

    assert_eq!(metadata.state.metadata.get("abc"), Some(&"def".to_string()));
    assert_eq!(metadata.state.metadata.get("ghi"), None);
    assert_eq!(metadata.state.header, "REDIS0001");
}

#[test]
fn test_metadata_loading_multiple() {
    let data = vec![
        0xFA, 0x03, 0x61, 0x62, 0x63, 0x03, 0x64, 0x65, 0x66, 0xFA, 0x03, 0x67, 0x68, 0x69, 0x03,
        0x6A, 0x6B, 0x6C,
    ];
    let bytes_handler = BytesEndec::<HeaderReady> {
        data,
        state: Default::default(),
    };

    let metadata = bytes_handler.load_metadata().unwrap();

    assert_eq!(metadata.state.metadata.get("abc"), Some(&"def".to_string()));
    assert_eq!(metadata.state.metadata.get("ghi"), Some(&"jkl".to_string()));
}

#[test]
fn test_metadata_loading_no_metadata() {
    let data = vec![
        0xFE, 0x00, 0xFB, 0x03, 0x02, 0x00, 0x06, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, 0x06, 0x62,
        0x61, 0x7A, 0x03, 0x71, 0x75, 0x78,
    ];
    let bytes_handler = BytesEndec::<HeaderReady> {
        data,
        state: Default::default(),
    };

    let metadata = bytes_handler.load_metadata().unwrap();
    assert_eq!(metadata.state.metadata, HashMap::new());
}

/// The following tests are for the database loading process
#[test]
fn test_database_loading() {
    let data = vec![
        0xFE, 0x00, 0xFB, 0x03, 0x02, 0x00, 0x06, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, 0x06, 0x62,
        0x61, 0x7A, 0x71, 0x75, 0x78, 0xFC, 0x15, 0x72, 0xE7, 0x07, 0x8F, 0x01, 0x00, 0x00, 0x00,
        0x03, 0x66, 0x6F, 0x6F, 0x03, 0x62, 0x61, 0x72, 0xFD, 0x52, 0xED, 0x2A, 0x66, 0x00, 0x03,
        0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78, 0xFF, 0x89, 0x3B, 0xB7, 0x4E, 0xF8, 0x0F, 0x77,
        0x19,
    ];
    let bytes_handler = BytesEndec::<MetadataReady> {
        data,
        state: Default::default(),
    };

    let rdb_file = bytes_handler.load_database().unwrap();
    assert_eq!(rdb_file.database.len(), 1);
    assert_eq!(rdb_file.database[0].index, 0);
    assert_eq!(rdb_file.database[0].storage.len(), 3);
    assert_eq!(
        rdb_file.checksum,
        vec![0x89, 0x3B, 0xB7, 0x4E, 0xF8, 0x0F, 0x77, 0x19]
    );
}
