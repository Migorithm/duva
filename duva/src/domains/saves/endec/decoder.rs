use crate::domains::caches::cache_objects::{CacheEntry, CacheValue, TypedValue};
use crate::domains::cluster_actors::replication::ReplicationId;
use crate::domains::saves::endec::{
    DATABASE_SECTION_INDICATOR, DATABASE_TABLE_SIZE_INDICATOR,
    EXPIRY_TIME_IN_MILLISECONDS_INDICATOR, EXPIRY_TIME_IN_SECONDS_INDICATOR, HEADER_MAGIC_STRING,
    METADATA_SECTION_INDICATOR, STRING_VALUE_TYPE_INDICATOR, StoredDuration, VERSION,
    extract_range,
};
use crate::domains::saves::snapshot::{Metadata, Snapshot, SubDatabase};

use anyhow::{Context, Result};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::ops::{Deref, DerefMut};

#[derive(Default)]
pub struct BytesDecoder<'a, T> {
    pub data: &'a [u8],
    pub state: T,
}

#[derive(Default)]
pub(crate) struct DatabaseSectionBuilder {
    pub(crate) index: usize,
    pub(crate) storage: Vec<CacheEntry>,
    pub(crate) key_value_table_size: usize,
    pub(crate) expires_table_size: usize,
}

impl DatabaseSectionBuilder {
    pub fn build(self) -> SubDatabase {
        SubDatabase { index: self.index, storage: self.storage }
    }
}

/// General purpose BytesEndec implementation
impl<T> BytesDecoder<'_, T> {
    fn skip(&mut self, n: usize) {
        self.data = &self.data[n..];
    }

    fn take_string(&mut self, n: usize) -> Result<String> {
        let data = String::from_utf8(self[0..n].to_vec())?;
        self.skip(n);
        Ok(data)
    }

    pub fn remove_identifier(&mut self) {
        self.skip(1);
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
            Some(self.take_string(size).ok()?)
        } else {
            self.integer_decode()
        }
    }

    // Decode a size-encoded value and return the decoded value as bytes.
    pub fn bytes_decode(&mut self) -> Option<Bytes> {
        // Ensure we have at least one byte to read.
        if self.is_empty() {
            return None;
        }

        if let Some(size) = self.size_decode() {
            if size > self.len() {
                return None;
            }
            let bytes = Bytes::from(self[0..size].to_vec());
            self.skip(size);
            Some(bytes)
        } else {
            // For integers, convert to string first, then to bytes
            self.integer_decode().map(Bytes::from)
        }
    }

    pub fn size_decode(&mut self) -> Option<usize> {
        if let Some(first_byte) = self.first() {
            match first_byte >> 6 {
                | 0b00 => {
                    let size = (first_byte & 0x3F) as usize;
                    self.skip(1);
                    Some(size)
                },
                | 0b01 => {
                    if self.len() < 2 {
                        return None;
                    }
                    let size = (((first_byte & 0x3F) as usize) << 8) | (self[1] as usize);
                    self.skip(2);
                    Some(size)
                },
                | 0b10 => {
                    if self.len() < 5 {
                        return None;
                    }
                    let size = ((self[1] as usize) << 24)
                        | ((self[2] as usize) << 16)
                        | ((self[3] as usize) << 8)
                        | (self[4] as usize);

                    self.skip(5);
                    Some(size)
                },
                | _ => None,
            }
        } else {
            None
        }
    }

    fn integer_decode(&mut self) -> Option<String> {
        if let Some(first_byte) = self.first() {
            match first_byte {
                // 0b11000000: 8-bit integer
                | 0xC0 => {
                    let value = i8::from_le_bytes([self[1]]).to_string();
                    self.skip(2);
                    return Some(value);
                },
                | 0xC1 => {
                    if self.len() >= 3 {
                        let value = i16::from_le_bytes(extract_range(self, 1..=2)?).to_string();
                        self.skip(3);
                        return Some(value);
                    }
                },
                | 0xC2 => {
                    if self.len() >= 5 {
                        let value = i32::from_le_bytes(extract_range(self, 1..=4)?).to_string();
                        self.skip(5);
                        return Some(value);
                    }
                },
                | _ => return None,
            }
        }
        None
    }

    pub(crate) fn check_indicator(&self, iden: u8) -> bool {
        self.first() == Some(&iden)
    }
}

impl<'a> BytesDecoder<'a, DecoderInit> {
    // read data and check first 5 ascii code convertable hex bytes are equal to "REDIS"
    // then read 4 digit Header version (like 0011) and return RdbFileLoader<MetadataSectionLoading> with header value as "REDIS" + 4 digit version
    pub fn load_header(mut self) -> Result<BytesDecoder<'a, HeaderReady>> {
        let header_len = HEADER_MAGIC_STRING.len() + VERSION.len();
        if self.len() < header_len {
            return Err(anyhow::Error::msg(format!(
                "header loading: data length is less than {header_len}"
            )))?;
        }

        let header = self.take_header()?;
        let version = self.take_version()?;

        Ok(BytesDecoder { data: self.data, state: HeaderReady(format!("{header}{version}")) })
    }
    fn take_header(&mut self) -> Result<String> {
        let header = self.take_string(HEADER_MAGIC_STRING.len())?;
        if header != HEADER_MAGIC_STRING {
            return Err(anyhow::Error::msg(format!(
                "header loading: header is not {HEADER_MAGIC_STRING}",
            )))?;
        }
        Ok(header)
    }
    fn take_version(&mut self) -> Result<String> {
        self.take_string(4).context("header loading: version decode fail")
    }
}

impl<'a> BytesDecoder<'a, HeaderReady> {
    pub fn load_metadata(mut self) -> Result<BytesDecoder<'a, MetadataReady>> {
        let mut metadata = Metadata { repl_id: ReplicationId::Undecided, log_idx: 0 };
        while self.check_indicator(METADATA_SECTION_INDICATOR) {
            let (key, value) = self
                .try_extract_metadata_key_value()
                .context("metadata loading: key value extraction failed")?;

            match key.as_str() {
                | "repl-id" => metadata.repl_id = ReplicationId::Key(value),
                | "repl-offset" => {
                    metadata.log_idx = value.parse().context("repl-offset parse fail")?
                },
                | var => {
                    println!("Unknown metadata key: {var}");
                },
            }
        }
        Ok(BytesDecoder {
            data: self.data,
            state: MetadataReady { metadata, header: self.state.0 },
        })
    }
    pub fn try_extract_metadata_key_value(&mut self) -> anyhow::Result<(String, String)> {
        self.remove_identifier();
        let key_data = self.string_decode().context("key decode fail")?;
        let value_data = self.string_decode().context("value decode fail")?;

        Ok((key_data, value_data))
    }
}
impl BytesDecoder<'_, MetadataReady> {
    pub fn load_database(mut self) -> Result<Snapshot> {
        let mut database = Vec::new();
        while self.check_indicator(DATABASE_SECTION_INDICATOR) {
            let section =
                self.extract_section().context("database loading: section extraction failed")?;

            database.push(section);
        }

        let checksum = self.try_get_checksum()?;
        Ok(Snapshot {
            header: self.state.header,
            metadata: self.state.metadata,
            database,
            checksum,
        })
    }
    fn extract_section(&mut self) -> Result<SubDatabase> {
        let mut builder: DatabaseSectionBuilder = DatabaseSectionBuilder::default();

        while let Some(identifier) = self.first() {
            match *identifier {
                | DATABASE_SECTION_INDICATOR => {
                    self.try_set_index(&mut builder)?;
                },
                | DATABASE_TABLE_SIZE_INDICATOR => {
                    self.try_set_table_sizes(&mut builder)?;
                },
                | _ => {
                    if self.should_stop_extending_storage(&mut builder)? {
                        break;
                    }
                },
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
        if key_value.expiry().is_some() {
            builder.expires_table_size =
                builder.expires_table_size.checked_sub(1).context("expires_table_size is 0")?;
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

    pub fn try_key_value(&mut self) -> Result<CacheEntry> {
        let mut expiry: Option<DateTime<Utc>> = None;
        while self.len() > 0 {
            match self[0] {
                //0b11111100
                | EXPIRY_TIME_IN_MILLISECONDS_INDICATOR => {
                    expiry = Some(self.try_extract_expiry_time_in_milliseconds()?.to_datetime());
                },
                //0b11111101
                | EXPIRY_TIME_IN_SECONDS_INDICATOR => {
                    expiry = Some(self.try_extract_expiry_time_in_seconds()?.to_datetime());
                },
                //0b11111110
                | STRING_VALUE_TYPE_INDICATOR => {
                    let (key, value) = self.try_extract_key_bytes()?;
                    let mut cache_value = CacheValue::new(TypedValue::String(value));
                    if let Some(expiry) = expiry {
                        cache_value = cache_value.with_expiry(expiry);
                    }
                    return Ok(CacheEntry::new_with_cache_value(key, cache_value));
                },
                | _ => {
                    return Err(anyhow::anyhow!("Invalid key value pair"));
                },
            }
        }
        Err(anyhow::anyhow!("Invalid key value pair"))
    }

    pub fn try_extract_expiry_time_in_seconds(&mut self) -> Result<StoredDuration> {
        self.remove_identifier();
        let range = 0..=3;
        let result = u32::from_le_bytes(
            extract_range(self, range.clone())
                .context("Failed to extract expiry time in seconds")?,
        );
        self.skip(4);

        Ok(StoredDuration::Seconds(result))
    }

    pub fn try_extract_expiry_time_in_milliseconds(&mut self) -> Result<StoredDuration> {
        self.remove_identifier();
        let range = 0..=7;
        let result = i64::from_le_bytes(
            extract_range(self, range.clone())
                .context("Failed to extract expiry time in milliseconds")?,
        );
        self.skip(8);

        Ok(StoredDuration::Milliseconds(result))
    }

    pub fn try_extract_key_bytes(&mut self) -> Result<(String, Bytes)> {
        self.remove_identifier();
        let key_data = self.string_decode().context("key decode fail")?;
        let value_data = self.bytes_decode().context("value decode fail")?;

        Ok((key_data, value_data))
    }

    pub fn try_get_checksum(&mut self) -> Result<Vec<u8>> {
        self.remove_identifier();
        let checksum = extract_range(self.data, 0..=7)
            .map(|f: [u8; 8]| f.to_vec())
            .context("failed to extract checksum")?;
        self.skip(8);

        Ok(checksum)
    }
}

#[derive(Default)]
pub struct DecoderInit;

#[derive(Default, PartialEq, Eq, Debug)]
pub struct HeaderReady(pub(crate) String);

pub struct MetadataReady {
    pub(crate) metadata: Metadata,
    pub(crate) header: String,
}

impl<'a> From<&'a [u8]> for BytesDecoder<'a, DecoderInit> {
    fn from(data: &'a [u8]) -> Self {
        Self { data, state: DecoderInit }
    }
}

impl<'a, T> Deref for BytesDecoder<'a, T> {
    type Target = &'a [u8];
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
impl<T> DerefMut for BytesDecoder<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_size_decoding() {
        static V1: [u8; 1] = [0x0D];
        static V2: [u8; 2] = [0x42, 0xBC];
        static V3: [u8; 5] = [0x80, 0x00, 0x00, 0x42, 0x68];
        static V4: [u8; 2] = [0xC0, 0x0A];

        let mut example1: BytesDecoder<DecoderInit> = (&V1 as &'static [u8]).into();
        let mut example2: BytesDecoder<DecoderInit> = (&V2 as &'static [u8]).into();
        let mut example3: BytesDecoder<DecoderInit> = (&V3 as &'static [u8]).into();
        let mut example4: BytesDecoder<DecoderInit> = (&V4 as &'static [u8]).into();

        assert_eq!(example1.size_decode(), Some(13));
        assert_eq!(example2.size_decode(), Some(700));
        assert_eq!(example3.size_decode(), Some(17000));
        assert_eq!(example4.size_decode(), None);
    }

    #[test]
    fn test_integer_decoding() {
        static V1: [u8; 2] = [0xC0, 0x0A];
        static V2: [u8; 3] = [0xC1, 0x39, 0x30];
        static V3: [u8; 5] = [0xC2, 0xEA, 0x17, 0x3E, 0x67];

        let mut example1: BytesDecoder<DecoderInit> = (&V1 as &'static [u8]).into();
        let mut example2: BytesDecoder<DecoderInit> = (&V2 as &'static [u8]).into();
        let mut example3: BytesDecoder<DecoderInit> = (&V3 as &'static [u8]).into();

        assert_eq!(example1.integer_decode(), Some("10".to_string()));
        assert_eq!(example2.integer_decode(), Some("12345".to_string()));
        assert_eq!(example3.integer_decode(), Some("1732122602".to_string()));
    }

    #[test]
    fn test_string_decoding() {
        static V1: [u8; 14] =
            [0x0D, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21];
        let mut example1: BytesDecoder<DecoderInit> = (&V1 as &'static [u8]).into();

        static V2: [u8; 6] = [0x42, 0x0A, 0x54, 0x65, 0x73, 0x74];
        let mut example2: BytesDecoder<DecoderInit> = (&V2 as &'static [u8]).into();

        assert_eq!(example1.string_decode(), Some("Hello, World!".to_string()));
        assert_eq!(example2.string_decode(), None);
    }

    #[test]
    fn test_decoding() {
        // "Hello, World!"
        static V1: [u8; 14] =
            [0x0D, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21];

        let mut example1: BytesDecoder<DecoderInit> = (&V1 as &'static [u8]).into();

        static V2: [u8; 6] = [0x42, 0x0A, 0x54, 0x65, 0x73, 0x74];
        // "Test", with size 10 (although more bytes needed)
        let mut example2: BytesDecoder<DecoderInit> = (&V2 as &'static [u8]).into();

        assert!(example1.string_decode().is_some());
        assert!(example2.string_decode().is_none()); // due to insufficient bytes
    }

    #[test]
    fn test_decode_multiple_strings() {
        // "abc" and "def"
        static V1: [u8; 8] = [0x03, 0x61, 0x62, 0x63, 0x03, 0x64, 0x65, 0x66];

        let mut encoded: BytesDecoder<DecoderInit> = (&V1 as &'static [u8]).into();
        let decoded = encoded.string_decode();
        assert_eq!(decoded, Some("abc".to_string()));
        let decoded = encoded.string_decode();
        assert_eq!(decoded, Some("def".to_string()));
    }

    #[test]
    fn test_database_section_extractor() {
        let data = &[
            0xFE, 0x00, 0xFB, 0x03, 0x02, 0x00, 0x06, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, 0x06,
            0x62, 0x61, 0x7A, 0x71, 0x75, 0x78, 0xFC, 0x15, 0x72, 0xE7, 0x07, 0x8F, 0x01, 0x00,
            0x00, 0x00, 0x03, 0x66, 0x6F, 0x6F, 0x03, 0x62, 0x61, 0x72, 0xFD, 0x52, 0xED, 0x2A,
            0x66, 0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78,
        ];

        let mut bytes_handler = BytesDecoder::<MetadataReady> {
            data,
            state: MetadataReady {
                metadata: Metadata { repl_id: ReplicationId::Undecided, log_idx: 0 },
                header: "".into(),
            },
        };

        let db_section: SubDatabase = bytes_handler.extract_section().unwrap();
        assert_eq!(db_section.index, 0);
        assert_eq!(db_section.storage.len(), 3);

        let cache_entry = &db_section.storage[0];
        assert_eq!(cache_entry.key(), "foobar");
        assert_eq!(cache_entry.value, "bazqux");

        let cache_entry = &db_section.storage[1];
        assert_eq!(cache_entry.key(), "foo");
        assert_eq!(cache_entry.value, "bar");
        assert!(cache_entry.expiry().is_some());
        assert_eq!(
            cache_entry.expiry().unwrap(),
            StoredDuration::Milliseconds(1713824559637).to_datetime()
        );
    }

    #[test]
    fn test_non_expiry_key_value_pair() {
        let mut bytes_handler = BytesDecoder::<MetadataReady> {
            data: &[0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78],
            state: MetadataReady {
                metadata: Metadata { repl_id: ReplicationId::Undecided, log_idx: 0 },
                header: "".into(),
            },
        };

        let key_value = bytes_handler.try_key_value().expect("Failed to extract key value expiry");
        assert_eq!(key_value.key(), "baz");
        assert_eq!(key_value.value, "qux");
        assert!(key_value.expiry().is_none());

        assert!(bytes_handler.data.is_empty());
    }

    #[test]
    fn test_with_milliseconds_expiry_key_value_pair() {
        let mut bytes_handler = BytesDecoder::<MetadataReady> {
            data: &[
                0xFC, 0x15, 0x72, 0xE7, 0x07, 0x8F, 0x01, 0x00, 0x00, 0x00, 0x03, 0x62, 0x61, 0x7A,
                0x03, 0x71, 0x75, 0x78,
            ],
            state: MetadataReady {
                metadata: Metadata { repl_id: ReplicationId::Undecided, log_idx: 0 },
                header: "".into(),
            },
        };

        let key_value = bytes_handler.try_key_value().unwrap();

        assert_eq!(key_value.key(), "baz");
        assert_eq!(key_value.value, "qux");
        assert!(key_value.expiry().is_some());
        assert!(bytes_handler.data.is_empty());
    }

    #[test]
    fn test_with_seconds_expiry_key_value_pair() {
        let mut bytes_handler = BytesDecoder::<MetadataReady> {
            data: &[
                0xFD, 0x52, 0xED, 0x2A, 0x66, 0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78,
            ],
            state: MetadataReady {
                metadata: Metadata { repl_id: ReplicationId::Undecided, log_idx: 0 },
                header: "".into(),
            },
        };

        let key_value = bytes_handler.try_key_value().unwrap();
        assert_eq!(key_value.key(), "baz");
        assert_eq!(key_value.value, "qux");
        assert!(key_value.expiry().is_some());
    }

    #[test]
    fn test_invalid_expiry_key_value_pair() {
        let mut bytes_handler = BytesDecoder::<MetadataReady> {
            data: &[
                0xFF, 0x52, 0xED, 0x2A, 0x66, 0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78,
            ],
            state: MetadataReady {
                metadata: Metadata { repl_id: ReplicationId::Undecided, log_idx: 0 },
                header: "".into(),
            },
        };

        let result = bytes_handler.try_key_value();
        assert!(result.is_err());
        assert_eq!(bytes_handler.data.len(), 14);
    }

    #[test]
    fn test_header_loading() {
        let decoder = BytesDecoder::<DecoderInit> {
            data: &[HEADER_MAGIC_STRING.as_bytes(), VERSION.as_bytes()].concat(),
            state: Default::default(),
        };
        let header = decoder.load_header().unwrap();

        assert_eq!(header.state, HeaderReady(HEADER_MAGIC_STRING.to_string() + VERSION));
    }

    #[test]
    fn test_header_loading_data_length_error() {
        let data = vec![0x52, 0x45, 0x44, 0x49, 0x53];
        let data: BytesDecoder<DecoderInit> = data.as_slice().into();

        let result = data.load_header();
        assert!(result.is_err());
    }

    #[test]
    fn test_metadata_loading_no_metadata() {
        let data = vec![
            0xFE, 0x00, 0xFB, 0x03, 0x02, 0x00, 0x06, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, 0x06,
            0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78,
        ];
        let bytes_handler =
            BytesDecoder::<HeaderReady> { data: data.as_slice(), state: Default::default() };

        let metadata = bytes_handler.load_metadata().unwrap();
        assert_eq!(
            metadata.state.metadata,
            Metadata { repl_id: ReplicationId::Undecided, log_idx: Default::default() }
        );
    }

    #[test]
    fn test_database_loading() {
        let data = vec![
            0xFE, 0x00, 0xFB, 0x03, 0x02, 0x00, 0x06, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, 0x06,
            0x62, 0x61, 0x7A, 0x71, 0x75, 0x78, 0xFC, 0x15, 0x72, 0xE7, 0x07, 0x8F, 0x01, 0x00,
            0x00, 0x00, 0x03, 0x66, 0x6F, 0x6F, 0x03, 0x62, 0x61, 0x72, 0xFD, 0x52, 0xED, 0x2A,
            0x66, 0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78, 0xFF, 0x89, 0x3B, 0xB7,
            0x4E, 0xF8, 0x0F, 0x77, 0x19,
        ];
        let bytes_handler = BytesDecoder::<MetadataReady> {
            data: data.as_slice(),
            state: MetadataReady {
                metadata: Metadata { repl_id: ReplicationId::Undecided, log_idx: 0 },
                header: "".into(),
            },
        };

        let rdb_file = bytes_handler.load_database().unwrap();
        assert_eq!(rdb_file.database.len(), 1);
        assert_eq!(rdb_file.database[0].index, 0);
        assert_eq!(rdb_file.database[0].storage.len(), 3);
        assert_eq!(rdb_file.checksum, vec![0x89, 0x3B, 0xB7, 0x4E, 0xF8, 0x0F, 0x77, 0x19]);
    }

    // ! Most important test for the BytesEndec implementation in decoding path.
    #[test]
    fn test_loading_all() {
        const M_KEY: u8 = 0xFA;

        const R_ID_SIZE: u8 = 0x28;
        const D_KEY: u8 = 0xFE;

        let mut data = Vec::new();
        data.extend_from_slice(HEADER_MAGIC_STRING.as_bytes());
        data.extend_from_slice(VERSION.as_bytes());

        data.extend_from_slice(&[
            // Metadata
            //* repl-id
            M_KEY, 0x07, //size of key
            0x72, 0x65, 0x70, 0x6c, 0x2d, 0x69, 0x64, R_ID_SIZE, // size of value(hex 28 = 40)
            0x34, 0x32, 0x30, 0x64, 0x64, 0x37, 0x65, 0x33, 0x32, 0x34, 0x63, 0x33, 0x61, 0x36,
            0x33, 0x37, 0x31, 0x62, 0x31, 0x30, 0x33, 0x31, 0x32, 0x39, 0x63, 0x65, 0x62, 0x65,
            0x36, 0x65, 0x32, 0x35, 0x61, 0x32, 0x37, 0x30, 0x66, 0x39, 0x66, 0x64, //*
            M_KEY, 0x0B, //size of key - repl-offset
            0x72, 0x65, 0x70, 0x6C, 0x2D, 0x6F, 0x66, 0x66, 0x73, 0x65, 0x74,
            0xC2, //size of offset
            0xA1, 0xC3, 0x83, 0x00, // Database
            D_KEY, 0x00, 0xFB, 0x02, 0x00, 0x00, 0x04, 0x66, 0x6F, 0x6F, 0x32, 0x04, 0x62, 0x61,
            0x72, 0x32, 0x00, 0x03, 0x66, 0x6F, 0x6F, 0x03, 0x62, 0x61, 0x72, 0xFF, 0x60, 0x82,
            0x9C, 0xF8, 0xFB, 0x2E, 0x7F, 0xEB,
        ]);
        let bytes_handler =
            BytesDecoder::<DecoderInit> { data: data.as_slice(), state: Default::default() };

        let rdb_file =
            bytes_handler.load_header().unwrap().load_metadata().unwrap().load_database().unwrap();

        assert_eq!(rdb_file.header, HEADER_MAGIC_STRING.to_string() + VERSION);
        assert_eq!(rdb_file.database.len(), 1);
        assert_eq!(rdb_file.database[0].index, 0);
        assert_eq!(rdb_file.database[0].storage.len(), 2);

        let cache_entry = &rdb_file.database[0].storage[0];
        assert_eq!(cache_entry.key(), "foo2");
        assert_eq!(cache_entry.value, "bar2");

        let cache_entry = &rdb_file.database[0].storage[1];
        assert_eq!(cache_entry.key(), "foo");
        assert_eq!(cache_entry.value, "bar");
        assert!(cache_entry.expiry().is_none());

        assert_eq!(rdb_file.checksum, vec![0x60, 0x82, 0x9C, 0xF8, 0xFB, 0x2E, 0x7F, 0xEB]);
        assert_eq!(
            rdb_file.metadata.repl_id,
            ReplicationId::Key("420dd7e324c3a6371b103129cebe6e25a270f9fd".into())
        );
        assert_eq!(rdb_file.metadata.log_idx, 8635297);
    }
}
