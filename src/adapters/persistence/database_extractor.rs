use crate::adapters::persistence::key_value_storage_extractor::extract_key_value_expiry;
use crate::adapters::persistence::size_encoding::size_decode;
use crate::{from_to, make_smart_pointer};
use anyhow::Result;

struct DatabaseSection<'a> {
    index: usize,
    storage: Vec<(String, String, Option<u64>)>,
    checksum: Vec<u8>,
    data: &'a mut Data,

    key_value_table_size: usize,
    expires_table_size: usize,
}

impl<'a> DatabaseSection<'a> {
    fn when_0xFE(&mut self) -> usize {
        self.data.remove(0);
        size_decode(self.data).unwrap()
    }
    fn when_0xFB(&mut self) -> Result<()> {
        self.data.remove(0);

        let err_mt = || anyhow::anyhow!("size decode fail");
        (self.key_value_table_size, self.expires_table_size) = (
            size_decode(self.data).ok_or(err_mt())?,
            size_decode(self.data).ok_or(err_mt())?,
        );
        Ok(())
    }

    fn when_0xFF(&mut self) {
        self.data.remove(0);
        self.checksum = self.data[0..8].to_vec();
        self.data.drain(..8);
    }
}

// TODO rename it
struct Data(Vec<u8>);
make_smart_pointer!(Data, Vec<u8>);
from_to!(Vec<u8>, Data);

fn database_section_extractor(data: &mut Data) -> Result<DatabaseSection> {
    let mut section = DatabaseSection {
        index: 0,
        storage: Vec::new(),
        checksum: Vec::new(),
        data,
        key_value_table_size: 0,
        expires_table_size: 0,
    };

    while section.data.len() > 0 {
        match section.data[0] {
            // 0b11111110
            0xFE => {
                section.when_0xFE();
            }

            //0b11111011
            0xFB => section.when_0xFB()?,
            //0b11111111
            0xFF => {
                section.when_0xFF();
            }
            _ => {
                if section.key_value_table_size > 0 {
                    let (key, value, expiry_time) = extract_key_value_expiry(section.data)
                        .ok_or(anyhow::anyhow!("extract_key_value_expiry fail"))?;
                    section.storage.push((key, value, expiry_time));
                    println!("storage {:?}", section.storage);
                    if expiry_time.is_some() {
                        if section.expires_table_size > 0 {
                            section.expires_table_size -= 1;
                        } else {
                            return Err(anyhow::anyhow!("expires_table_size is 0"));
                        }
                    }
                    section.key_value_table_size -= 1;
                } else {
                    break;
                }
            }
        }
    }
    Ok(section)
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

    let mut_data = &mut data;
    let db_section = database_section_extractor(mut_data).unwrap();
    assert_eq!(db_section.index, 0);
    assert_eq!(db_section.storage.len(), 3);
    assert_eq!(db_section.checksum.len(), 8);
    assert_eq!(data.len(), 0);
}
