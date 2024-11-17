use std::collections::HashMap;
use anyhow::Error;
use crate::adapters::persistence::bytes_handler::BytesHandler;
use crate::adapters::persistence::database_extractor::{DatabaseSection, DatabaseSectionBuilder};
use crate::adapters::persistence::RdbFile;

const RDB_HEADER_MAGIC_STRING: &str = "REDIS";

#[derive(Default)]
struct RdbFileLoader<T = HeaderLoading> {
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
    fn load_header(mut self) -> anyhow::Result<RdbFileLoader<MetadataSectionLoading>> {
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

impl RdbFileLoader<MetadataSectionLoading> {
    fn load_metadata(mut self) -> anyhow::Result<RdbFileLoader<DatabaseSectionLoading>> {
        let mut metadata = HashMap::new();
        while self.is_metadata_section() {
            let (key, value) = self.data.try_extract_key_value()?;
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
    fn is_metadata_section(&self) -> bool {
        let identifier = self.data.get(0);
        identifier == Some(&0xFA)
    }
}

impl RdbFileLoader<DatabaseSectionLoading> {
    fn load_database(mut self) -> anyhow::Result<RdbFile> {
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
    fn is_database_section(&self) -> bool {
        let identifier = self.data.get(0);
        identifier == Some(&0xFE)
    }
}

fn create_error_while_loading(state: &str, message: &str) -> Error {
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
        data: BytesHandler(data),
        state: DatabaseSectionLoading,
        header: Some("REDIS0001".to_string()),
        metadata: Some(HashMap::new()),
        database: None,
    };
    let rdb_file = loader.load_database().unwrap();
    assert_eq!(rdb_file.header, "REDIS0001");
    assert_eq!(rdb_file.metadata, HashMap::new());
    assert_eq!(rdb_file.database.len(), 1);
    assert_eq!(rdb_file.database[0].index, 0);
    assert_eq!(rdb_file.database[0].storage.len(), 3);
}
