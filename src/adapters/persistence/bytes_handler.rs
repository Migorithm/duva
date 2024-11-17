use crate::adapters::persistence;
use crate::{from_to, make_smart_pointer};

make_smart_pointer!(BytesHandler, Vec<u8>);
from_to!(Vec<u8>, BytesHandler);
#[derive(Default)]
pub struct BytesHandler(pub Vec<u8>);

impl BytesHandler {
    // TODO subject to refactor
    pub fn from_u32(value: u32) -> Self {
        let mut result = BytesHandler::default();
        if value <= 0xFF {
            result.push(0xC0);
            result.push(value as u8);
        } else if value <= 0xFFFF {
            result.push(0xC1);
            let value = value as u16;
            result.extend_from_slice(&value.to_le_bytes());
        } else {
            result.push(0xC2);
            result.extend_from_slice(&value.to_le_bytes());
        }
        result
    }

    pub fn remove_identifier(&mut self) {
        self.remove(0);
    }

    pub fn try_extract_key_value(&mut self) -> anyhow::Result<(String, String)> {
        self.remove_identifier();
        let key_data = self
            .string_decode()
            .ok_or(anyhow::anyhow!("key decode fail"))?;

        let value_data = self
            .string_decode()
            .ok_or(anyhow::anyhow!("value decode fail"))?;

        Ok((key_data, value_data))
    }

    pub fn try_extract_expiry_time_in_seconds(&mut self) -> anyhow::Result<u64> {
        let range = 0..=3;
        let result = u32::from_le_bytes(
            persistence::extract_range(self, range.clone())
                .ok_or(anyhow::anyhow!("Failed to extract expiry time in seconds"))?,
        );
        self.drain(range);

        Ok(result as u64)
    }

    pub fn try_extract_expiry_time_in_milliseconds(&mut self) -> anyhow::Result<u64> {
        let range = 0..=7;
        let result = u64::from_le_bytes(persistence::extract_range(self, range.clone()).ok_or(
            anyhow::anyhow!("Failed to extract expiry time in milliseconds"),
        )?);
        self.drain(range);
        Ok(result)
    }
    pub fn try_size_decode(&mut self) -> anyhow::Result<usize> {
        self.size_decode()
            .ok_or(anyhow::anyhow!("size decode fail"))
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
            let data = String::from_utf8(self.drain(0..size).collect()).unwrap();
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
                        let value = u16::from_le_bytes(persistence::extract_range(self, 1..=2)?).to_string();
                        self.drain(0..3);
                        return Some(value);
                    }
                }
                0xC2 => {
                    if self.len() == 5 {
                        let value = u32::from_le_bytes(persistence::extract_range(self, 1..=4)?).to_string();
                        self.drain(0..5);
                        return Some(value);
                    }
                }
                _ => return None,
            }
        }
        None
    }
}

#[test]
fn test_size_decoding() {
    let mut example1: BytesHandler = vec![0x0D].into();
    let mut example2: BytesHandler = vec![0x42, 0xBC].into();
    let mut example3: BytesHandler = vec![0x80, 0x00, 0x00, 0x42, 0x68].into();
    let mut example4: BytesHandler = vec![0xC0, 0x0A].into();

    assert_eq!(example1.size_decode(), Some(13));
    assert_eq!(example2.size_decode(), Some(700));
    assert_eq!(example3.size_decode(), Some(17000));
    assert_eq!(example4.size_decode(), None);
}

#[test]
fn test_integer_decoding() {
    let mut example1: BytesHandler = vec![0xC0, 0x0A].into();
    let mut example2: BytesHandler = vec![0xC1, 0x39, 0x30].into();
    let mut example3: BytesHandler = vec![0xC2, 0x87, 0xD6, 0x12, 0x00].into();

    assert_eq!(
        example1.integer_decode(),
        Some("10".to_string())
    );
    assert_eq!(
        example2.integer_decode(),
        Some( "12345".to_string())
    );
    assert_eq!(
        example3.integer_decode(),
        Some("1234567".to_string())
    );
}

#[test]
fn test_string_decoding() {
    let mut example1: BytesHandler = vec![0x0D, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21].into();
    let mut example2: BytesHandler = vec![0x42, 0x0A, 0x54, 0x65, 0x73, 0x74].into();

    assert_eq!(
        example1.string_decode(),
        Some("Hello, World!".to_string())
    );
    assert_eq!(example2.string_decode(), None);
}

#[test]
fn test_decoding() {
    // "Hello, World!"
    let mut example1: BytesHandler = vec![
        0x0D, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21,
    ]
        .into();

    // "Test", with size 10 (although more bytes needed)
    let mut example2: BytesHandler = vec![0x42, 0x0A, 0x54, 0x65, 0x73, 0x74].into();

    assert!(example1.string_decode().is_some());
    assert!(example2.string_decode().is_none()); // due to insufficient bytes
}

#[test]
fn test_decode_multiple_strings() {
    // "abc" and "def"
    let mut encoded: BytesHandler = vec![0x03, 0x61, 0x62, 0x63, 0x03, 0x64, 0x65, 0x66].into();
    let decoded = encoded.string_decode();
    assert_eq!(
        decoded,
        Some("abc".to_string())
    );
    let decoded = encoded.string_decode();
    assert_eq!(
        decoded,
        Some("def".to_string())
    );
}
