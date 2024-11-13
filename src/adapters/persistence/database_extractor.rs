use anyhow::Result;

use super::{Data, KeyValue};

struct DatabaseSection<'a> {
    index: usize,
    storage: Vec<KeyValue>,
    checksum: Vec<u8>,
    data: &'a mut Data,

    key_value_table_size: usize,
    expires_table_size: usize,
}

impl<'a> DatabaseSection<'a> {
    pub fn new(data: &'a mut Data) -> Result<Self> {
        let section = Self {
            data,
            index: Default::default(),
            storage: Default::default(),
            checksum: Default::default(),
            key_value_table_size: Default::default(),
            expires_table_size: Default::default(),
        };
        section.create_section()
    }

    fn create_section(mut self) -> Result<Self> {
        while self.data.len() > 0 {
            match self.data[0] {
                // 0b11111110
                0xFE => {
                    let _ = self.data.when_0xFE();
                }

                //0b11111011
                0xFB => {
                    (self.key_value_table_size, self.expires_table_size) = self.data.when_0xFB()?
                }
                //0b11111111
                0xFF => {
                    self.checksum = self.data.when_0xFF();
                }
                _ => {
                    if self.is_key_value_extractable() {
                        self.extract_key_value()?;
                    } else {
                        break;
                    }
                }
            }
        }
        Ok(self)
    }

    // ! as long as key_value_table_size is not 0 key value is extractable?
    fn is_key_value_extractable(&self) -> bool {
        self.key_value_table_size > 0
    }

    fn extract_key_value(&mut self) -> Result<()> {
        let key_value = KeyValue::default().extract_key_value_expiry(self.data)?;

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

#[test]
fn test_database_section_extractor() {
    let mut data = vec![
        0xFE, 0x00, 0xFB, 0x03, 0x02, 0x00, 0x06, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, 0x06, 0x62,
        0x61, 0x7A, 0x71, 0x75, 0x78, 0xFC, 0x15, 0x72, 0xE7, 0x07, 0x8F, 0x01, 0x00, 0x00, 0x00,
        0x03, 0x66, 0x6F, 0x6F, 0x03, 0x62, 0x61, 0x72, 0xFD, 0x52, 0xED, 0x2A, 0x66, 0x00, 0x03,
        0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78, 0xFF, 0x89, 0x3B, 0xB7, 0x4E, 0xF8, 0x0F, 0x77,
        0x19,
    ]
    .into();

    let db_section = DatabaseSection::new(&mut data).unwrap();
    assert_eq!(db_section.index, 0);
    assert_eq!(db_section.storage.len(), 3);
    assert_eq!(db_section.checksum.len(), 8);
    assert_eq!(data.len(), 0);
}
