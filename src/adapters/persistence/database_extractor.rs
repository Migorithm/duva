use crate::adapters::persistence::key_value_storage_extractor::extract_key_value_expiry;
use crate::adapters::persistence::size_encoding::{size_decode, string_decode};

struct DatabaseSection {
    index: usize,
    storage: Vec<(String,String,Option<u64>)>,
    checksum: Vec<u8>,
}
fn database_section_extractor(data : &mut Vec<u8>) -> Option<DatabaseSection>{
    let mut index = 0; 
    let mut key_value_table_size = 0;
    let mut expires_table_size = 0;
    let mut storage = Vec::new();
    let mut checksum = Vec::new();
    while data.len() > 0 {
        match data[0] {
            0xFE => {
                data.remove(0);
                index = size_decode(data)?;
            }
            0xFB => {
                data.remove(0);
                key_value_table_size = size_decode(data)?;
                expires_table_size = size_decode(data)?;
            }
            0xFF => {
                data.remove(0);
                checksum = data[0..8].to_vec();
                data.drain(..8);
            }
            _ => {
                if key_value_table_size > 0 {
                    let (key, value, expiry_time) = extract_key_value_expiry(data)?;
                    storage.push((key, value, expiry_time));
                    println!("storage {:?}", storage);
                    if expiry_time.is_some(){
                        if expires_table_size > 0 {
                            expires_table_size -= 1;
                        } else {
                            return None
                        }
                    }
                    key_value_table_size -= 1;
                } else {
                    break;
                }
            }
        }
    }
    Some(DatabaseSection{
        index,
        storage,
        checksum,
    })
}

#[test]
fn test_database_section_extractor(){
    let mut data = vec![
        0xFE,
        0x00,
        0xFB,
        0x03,
        0x02,
        0x00,
        0x06, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72,
        0x06, 0x62, 0x61, 0x7A, 0x71, 0x75, 0x78,
        0xFC,
        0x15, 0x72, 0xE7, 0x07, 0x8F, 0x01, 0x00, 0x00,
        0x00,
        0x03, 0x66, 0x6F, 0x6F,
        0x03, 0x62, 0x61, 0x72,
        0xFD,
        0x52, 0xED, 0x2A, 0x66,
        0x00,
        0x03, 0x62, 0x61, 0x7A,
        0x03, 0x71, 0x75, 0x78,
        0xFF, 0x89, 0x3B, 0xB7, 0x4E, 0xF8, 0x0F, 0x77, 0x19,
    ];
    let mut_data = &mut data;
    let db_section = database_section_extractor(mut_data).unwrap();
    assert_eq!(db_section.index, 0);
    assert_eq!(db_section.storage.len(), 3);
    assert_eq!(db_section.checksum.len(), 8);
    assert_eq!(data.len(), 0);
}