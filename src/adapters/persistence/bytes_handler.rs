use crate::adapters::persistence;
use crate::adapters::persistence::DecodedData;

#[derive(Default)]
pub struct BytesHandler(pub Vec<u8>);

impl BytesHandler {
    // TODO subject to refactor
    fn from_u32(value: u32) -> Self {
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

        Ok((key_data.data, value_data.data))
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
    pub fn string_decode(&mut self) -> Option<DecodedData> {
        // Ensure we have at least one byte to read.
        if self.is_empty() {
            return None;
        }

        if let Some(size) = self.size_decode() {
            if size > self.len() {
                return None;
            }
            let data = String::from_utf8(self.drain(0..size).collect()).unwrap();
            Some(DecodedData { data })
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

    fn integer_decode(&mut self) -> Option<DecodedData> {
        if let Some(first_byte) = self.get(0) {
            match first_byte {
                // 0b11000000: 8-bit integer
                0xC0 => {
                    let value = u8::from_le_bytes([self[1]]).to_string();
                    self.drain(0..2);
                    return Some(DecodedData { data: value });
                }
                0xC1 => {
                    if self.len() == 3 {
                        let value = u16::from_le_bytes(persistence::extract_range(self, 1..=2)?).to_string();
                        self.drain(0..3);
                        return Some(DecodedData { data: value });
                    }
                }
                0xC2 => {
                    if self.len() == 5 {
                        let value = u32::from_le_bytes(persistence::extract_range(self, 1..=4)?);
                        self.drain(0..5);
                        return Some(DecodedData {
                            data: value.to_string(),
                        });
                    }
                }
                _ => return None,
            }
        }
        None
    }
}