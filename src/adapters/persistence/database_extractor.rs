use crate::adapters::persistence::bytes_handler::BytesHandler;
use crate::make_smart_pointer;
use anyhow::Result;

use super::KeyValueStorage;

pub struct Unset;
pub struct Initialized<'a>(pub &'a mut BytesHandler);
make_smart_pointer!(Initialized<'a>, BytesHandler);

pub struct DatabaseSection {
    pub index: usize,
    pub storage: Vec<KeyValueStorage>,
}

pub struct DatabaseSectionBuilder<T> {
    index: usize,
    storage: Vec<KeyValueStorage>,
    state: T,
    key_value_table_size: usize,
    expires_table_size: usize,
}

impl DatabaseSectionBuilder<Unset> {
    pub fn new(data: &mut BytesHandler) -> DatabaseSectionBuilder<Initialized<'_>> {
        DatabaseSectionBuilder {
            state: Initialized(data),
            index: Default::default(),
            storage: Default::default(),
            key_value_table_size: Default::default(),
            expires_table_size: Default::default(),
        }
    }
}

impl DatabaseSectionBuilder<Initialized<'_>> {
    pub fn extract_section(mut self) -> Result<DatabaseSection> {
        while self.state.len() > 0 {
            match self.state[0] {
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
        self.state.remove_identifier();
        self.index = self.state.try_size_decode()?;
        Ok(())
    }

    fn try_set_table_sizes(&mut self) -> Result<()> {
        self.state.remove_identifier();
        self.key_value_table_size = self.state.try_size_decode()?;
        self.expires_table_size = self.state.try_size_decode()?;
        Ok(())
    }

    // ! as long as key_value_table_size is not 0 key value is extractable?
    fn is_key_value_extractable(&self) -> bool {
        self.key_value_table_size > 0
    }

    fn save_key_value_expiry_time_in_storage(&mut self) -> Result<()> {
        let key_value = KeyValueStorage::new(self.state.0)?;

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
