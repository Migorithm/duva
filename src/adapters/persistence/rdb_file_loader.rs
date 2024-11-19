use crate::adapters::persistence::bytes_handler::BytesEndec;

use crate::adapters::persistence::{DatabaseSectionBuilder, RdbFile};
use anyhow::Error;
use std::collections::HashMap;
use std::marker::PhantomData;

use super::extract_range;

#[derive(Default)]
pub(super) struct HeaderLoading;
#[derive(Default)]
pub(super) struct MetadataSectionLoading;
#[derive(Default)]
pub(super) struct DatabaseSectionLoading;

#[derive(Default)]
pub(super) struct RdbFileLoader<T> {
    data: BytesEndec,
    state: PhantomData<T>,
    header: Option<String>,
    metadata: Option<HashMap<String, String>>,
}

impl RdbFileLoader<HeaderLoading> {
    pub(super) fn new(data: Vec<u8>) -> Self {
        Self {
            data: BytesEndec(data),
            ..Default::default()
        }
    }
    // read data and check first 5 ascii code convertable hex bytes are equal to "REDIS"
    // then read 4 digit Header version (like 0011) and return RdbFileLoader<MetadataSectionLoading> with header value as "REDIS" + 4 digit version
    pub(super) fn load_header(mut self) -> anyhow::Result<RdbFileLoader<MetadataSectionLoading>> {
        const RDB_HEADER_MAGIC_STRING: &str = "REDIS";

        if self.data.len() < 9 {
            return Err(LoadingError::new(
                "header loading",
                "data length is less than 9",
            ))?;
        }
        let header = String::from_utf8(self.data.drain(0..5).collect())?;
        if header != RDB_HEADER_MAGIC_STRING {
            return Err(LoadingError::new("header loading", "header is not REDIS"))?;
        }
        let version = String::from_utf8(self.data.drain(0..4).collect());
        if version.is_err() {
            return Err(LoadingError::new("header loading", "version is not valid"))?;
        }
        self.header = Some(format!("{}{}", header, version?));
        Ok(RdbFileLoader {
            data: self.data,
            state: PhantomData,
            header: self.header,
            ..Default::default()
        })
    }
}

impl RdbFileLoader<MetadataSectionLoading> {
    pub(super) fn load_metadata(mut self) -> anyhow::Result<RdbFileLoader<DatabaseSectionLoading>> {
        const METADATA_SECTION_IDENTIFIER: u8 = 0xFA;

        let mut metadata = HashMap::new();
        while self.data.check_identifier(METADATA_SECTION_IDENTIFIER) {
            let Ok((key, value)) = self.data.try_extract_key_value() else {
                return Err(LoadingError::new(
                    "metadata loading",
                    "key value extraction failed",
                ))?;
            };
            metadata.insert(key, value);
        }

        self.metadata = Some(metadata);
        Ok(RdbFileLoader {
            data: self.data,
            state: PhantomData,
            header: self.header,
            metadata: self.metadata,
            ..Default::default()
        })
    }
}

impl RdbFileLoader<DatabaseSectionLoading> {
    pub(super) fn load_database(mut self) -> anyhow::Result<RdbFile> {
        const DATABASE_SECTION_IDENTIFIER: u8 = 0xFE;

        let mut database = Vec::new();
        while self.data.check_identifier(DATABASE_SECTION_IDENTIFIER) {
            let section = DatabaseSectionBuilder::new(&mut self.data);
            let section = section.extract_section();
            if section.is_err() {
                return Err(LoadingError::new(
                    "database loading",
                    "section extraction failed",
                ))?;
            }
            database.push(section?);
        }

        let checksum = self.try_get_checksum()?;
        Ok(RdbFile {
            header: self
                .header
                .ok_or(LoadingError::new("invalid operation", "header is none"))?,
            metadata: self
                .metadata
                .ok_or(LoadingError::new("invalid operation", "metadata is none"))?,
            database,
            checksum: checksum,
        })
    }

    fn try_get_checksum(&mut self) -> anyhow::Result<Vec<u8>> {
        self.data.remove_identifier();
        let checksum = extract_range(&self.data, 0..=7)
            .map(|f: [u8; 8]| f.to_vec())
            .ok_or(Error::msg("failed to extract checksum"))?;
        self.data.drain(..8);
        Ok(checksum)
    }
}

struct LoadingError<'a> {
    state: &'a str,
    message: &'a str,
}
impl<'a> LoadingError<'a> {
    fn new(state: &'a str, message: &'a str) -> Self {
        Self { state, message }
    }
}
impl<'a> From<LoadingError<'a>> for Error {
    fn from(error: LoadingError) -> Self {
        anyhow::anyhow!("Error occurred while {}:{}", error.state, error.message)
    }
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

#[test]
fn test_metadata_loading() {
    let data = vec![0xFA, 0x03, 0x61, 0x62, 0x63, 0x03, 0x64, 0x65, 0x66];
    let loader = RdbFileLoader {
        data: BytesEndec(data),
        state: PhantomData::<MetadataSectionLoading>,
        header: Some("REDIS0001".to_string()),
        metadata: None,
    };
    let loader = loader.load_metadata().unwrap();
    let metadata = loader.metadata.unwrap();
    assert_eq!(metadata.get("abc"), Some(&"def".to_string()));
}

#[test]
fn test_metadata_loading_multiple() {
    let data = vec![
        0xFA, 0x03, 0x61, 0x62, 0x63, 0x03, 0x64, 0x65, 0x66, 0xFA, 0x03, 0x67, 0x68, 0x69, 0x03,
        0x6A, 0x6B, 0x6C,
    ];
    let loader = RdbFileLoader {
        data: BytesEndec(data),
        state: PhantomData::<MetadataSectionLoading>,
        header: Some("REDIS0001".to_string()),
        metadata: None,
    };
    let loader = loader.load_metadata().unwrap();
    let metadata = loader.metadata.unwrap();
    assert_eq!(metadata.get("abc"), Some(&"def".to_string()));
    assert_eq!(metadata.get("ghi"), Some(&"jkl".to_string()));
}

#[test]
fn test_metadata_loading_no_metadata() {
    let data = vec![
        0xFE, 0x00, 0xFB, 0x03, 0x02, 0x00, 0x06, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, 0x06, 0x62,
        0x61, 0x7A, 0x03, 0x71, 0x75, 0x78,
    ];
    let loader = RdbFileLoader {
        data: BytesEndec(data),
        state: PhantomData::<MetadataSectionLoading>,
        header: Some("REDIS0001".to_string()),
        metadata: None,
    };
    let loader = loader.load_metadata().unwrap();
    assert_eq!(loader.metadata, Some(HashMap::new()));
}

#[test]
fn test_database_loading() {
    let data = vec![
        0xFE, 0x00, 0xFB, 0x03, 0x02, 0x00, 0x06, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, 0x06, 0x62,
        0x61, 0x7A, 0x71, 0x75, 0x78, 0xFC, 0x15, 0x72, 0xE7, 0x07, 0x8F, 0x01, 0x00, 0x00, 0x00,
        0x03, 0x66, 0x6F, 0x6F, 0x03, 0x62, 0x61, 0x72, 0xFD, 0x52, 0xED, 0x2A, 0x66, 0x00, 0x03,
        0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78, 0xFF, 0x89, 0x3B, 0xB7, 0x4E, 0xF8, 0x0F, 0x77,
        0x19,
    ];
    let loader = RdbFileLoader {
        data: BytesEndec(data),
        state: PhantomData::<DatabaseSectionLoading>,
        header: Some("REDIS0001".to_string()),
        metadata: Some(HashMap::new()),
    };
    let rdb_file = loader.load_database().unwrap();
    assert_eq!(rdb_file.header, "REDIS0001");
    assert_eq!(rdb_file.metadata, HashMap::new());
    assert_eq!(rdb_file.database.len(), 1);
    assert_eq!(rdb_file.database[0].index, 0);
    assert_eq!(rdb_file.database[0].storage.len(), 3);
}
