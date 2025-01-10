use crate::services::statefuls::{
    cache::CacheEntry,
    persist::endec::{
        CHECKSUM_INDICATOR, DATABASE_SECTION_INDICATOR, DATABASE_TABLE_SIZE_INDICATOR,
        EXPIRY_TIME_IN_MILLISECONDS_INDICATOR, HEADER_MAGIC_STRING, METADATA_SECTION_INDICATOR,
        STRING_VALUE_TYPE_INDICATOR,
    },
};

use anyhow::Result;
use std::time::UNIX_EPOCH;

// TODO subject refactor
impl CacheEntry {
    pub fn encode_with_key(&self) -> Result<Vec<u8>> {
        let mut result = Vec::new();
        match self {
            CacheEntry::KeyValue(key, value) => {
                result.push(STRING_VALUE_TYPE_INDICATOR);
                result.extend_from_slice(&encode_key_value(key, value)?);
            }
            CacheEntry::KeyValueExpiry(key, value, expiry) => {
                let secs = (expiry.duration_since(UNIX_EPOCH)?.as_millis()) as u64;
                result.push(EXPIRY_TIME_IN_MILLISECONDS_INDICATOR);
                result.extend_from_slice(&secs.to_le_bytes());

                result.push(STRING_VALUE_TYPE_INDICATOR);
                result.extend_from_slice(&encode_key_value(key, value)?);
            }
        }
        Ok(result)
    }
}

pub fn encode_header(version: &str) -> Result<Vec<u8>> {
    if version.len() != 4 {
        return Err(anyhow::anyhow!("Invalid version string"));
    }
    let mut result = Vec::new();
    result.extend_from_slice(HEADER_MAGIC_STRING.as_bytes());
    result.extend_from_slice(version.as_bytes());
    Ok(result)
}

pub fn encode_metadata(metadata: Vec<(&str, &str)>) -> Result<Vec<u8>> {
    let mut result = Vec::new();
    for (key, value) in metadata.iter() {
        result.push(METADATA_SECTION_INDICATOR);
        result.extend_from_slice(&encode_key_value(key, value)?);
    }
    Ok(result)
}
pub fn encode_database_info(index: usize) -> Result<Vec<u8>> {
    let mut result = Vec::new();
    result.push(DATABASE_SECTION_INDICATOR);
    result.extend_from_slice(&encode_size(index)?);
    Ok(result)
}
pub fn encode_database_table_size(table_size: usize, expires_table_size: usize) -> Result<Vec<u8>> {
    let mut result = Vec::new();
    result.push(DATABASE_TABLE_SIZE_INDICATOR);
    result.extend_from_slice(&encode_size(table_size)?);
    result.extend_from_slice(&encode_size(expires_table_size)?);
    Ok(result)
}
pub fn encode_checksum(checksum: &[u8]) -> Result<Vec<u8>> {
    let mut result = Vec::new();
    result.push(CHECKSUM_INDICATOR);
    result.extend_from_slice(checksum);
    Ok(result)
}
fn encode_key_value(key: &str, value: &str) -> Result<Vec<u8>> {
    let mut result = Vec::new();
    let key = encode_string(key.len(), key)?;
    let value = encode_string(value.len(), value)?;
    result.extend_from_slice(&key);
    result.extend_from_slice(&value);
    Ok(result)
}

fn encode_integer(value: u32) -> Result<Vec<u8>> {
    let mut result = Vec::new();
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
    Ok(result)
}

fn encode_string(size: usize, value: &str) -> Result<Vec<u8>> {
    if let Ok(value) = value.parse::<u32>() {
        return encode_integer(value);
    }
    let mut result = encode_size(size)?;
    // Append the data to be encoded as a string after the size.
    result.extend_from_slice(value.as_bytes());
    Ok(result)
}

fn encode_size(size: usize) -> Result<Vec<u8>> {
    let mut result = Vec::new();
    // if value is representable with 6bits : 0b00 -> Use the remaining 6 bits to represent the size.
    if size < (1 << 6) {
        result.push(size as u8);
    }
    // if value is representable with 14bits : 0b01 -> Use the next 14 bits (6 bits in the first byte + the next byte).
    else if size < (1 << 14) {
        // Big endian
        result.push((0b01 << 6) | ((size >> 8) as u8 & 0x3F));
        result.push(size as u8);
    }
    // if value is representable with 32bits : 0b10 -> Use the next 4 bytes.
    else if size < (1 << 32) {
        result.push(0b10 << 6);
        //`to_be_bytes` returns big endian
        result.extend_from_slice(&(size as u32).to_be_bytes());
    } else {
        // Size too large to encode within the supported format.
        return Err(anyhow::anyhow!("Size too large to encode within the supported format."));
    }
    Ok(result)
}

#[cfg(test)]
mod test {
    use crate::services::statefuls::persist::endec::{
        decoder::{byte_decoder::BytesDecoder, states::DecoderInit},
        StoredDuration,
    };

    use super::*;

    #[test]
    fn test_size_encode_6_bit() {
        // 6-bit size encoding (0b00): Size is 10, "0A" in hex.
        let data = "test";
        let size = 10;
        let encoded = encode_string(size, data).expect("Encoding failed");
        assert_eq!(encoded[0], 0b00_001010); // 6-bit size encoding
        assert_eq!(&encoded[1..], data.as_bytes()); // Check the appended data.
    }
    #[test]
    fn test_size_encode_14_bit() {
        // 14-bit size encoding (0b01): Size is 700.
        let data = "example";
        let size = 700;
        let encoded = encode_string(size, data).expect("Encoding failed");
        assert_eq!(encoded[0] >> 6, 0b01); // First two bits should be 0b01.
        assert_eq!(((encoded[0] & 0x3F) as usize) << 8 | (encoded[1] as usize), size); // Check the 14-bit size.
        assert_eq!(&encoded[2..], data.as_bytes()); // Check the appended data.
    }

    #[test]
    fn test_size_encode_32_bit() {
        // 32-bit size encoding (0b10): Size is 17000.
        let data = "test32bit";
        let size = 17000;
        let encoded = encode_string(size, data).expect("Encoding failed");
        assert_eq!(encoded[0] >> 6, 0b10); // First two bits should be 0b10.

        // Check 4-byte big-endian size encoding.
        let expected_size = (encoded[1] as usize) << 24
            | (encoded[2] as usize) << 16
            | (encoded[3] as usize) << 8
            | (encoded[4] as usize);
        assert_eq!(expected_size, size);
        assert_eq!(&encoded[5..], data.as_bytes()); // Check the appended data.
    }

    #[test]
    fn test_size_encode_too_large() {
        // Ensure encoding fails for sizes too large to encode (greater than 2^32).
        let data = "overflow";
        let size = usize::MAX; // Maximum usize value, likely to exceed the allowed encoding size.
        let encoded = encode_string(size, data);
        assert!(encoded.is_err(), "Encoding should fail for too large size");
    }

    #[test]
    fn test_long_string() {
        // Create a string of length 30000
        let long_string = "A".repeat(30000);
        let data = &long_string;

        let encoded = encode_string(30000, data).unwrap();

        // Let's examine the encoding:
        assert_eq!(encoded[0] >> 6, 0b10); // First two bits should be 0b10

        // Next 4 bytes contain the size 30000 in big-endian
        let size_bytes = [encoded[1], encoded[2], encoded[3], encoded[4]];
        let decoded_size = u32::from_be_bytes(size_bytes);

        println!("Size in bytes: {}", decoded_size); // Should be 30000

        // Data starts from index 5
        assert_eq!(&encoded[5..], data.as_bytes());
    }

    #[test]
    fn test_8_bit_integer_encode() {
        let data = "123";
        let size = data.len();
        let encoded = encode_string(size, data).unwrap();
        assert_eq!(encoded[0], 0xC0);
        assert_eq!(encoded[1], 0x7B);
    }

    #[test]
    fn test_8_bit_integer_decode() {
        let data = "123";
        let size = data.len();
        let encoded = encode_string(size, data).unwrap();
        let mut decoder = BytesDecoder { data: &encoded, state: DecoderInit };
        assert_eq!(decoder.string_decode(), Some("123".to_string()));
    }

    #[test]
    fn test_16_bit_integer() {
        let data = "12345";
        let size = data.len();
        let encoded = encode_string(size, data).unwrap();
        assert_eq!(encoded[0], 0xC1);
        assert_eq!(encoded[1..], [0x39, 0x30]);
    }

    #[test]
    fn test_16_bit_integer_decode() {
        let data = "12345";
        let size = data.len();
        let encoded = encode_string(size, data).unwrap();
        let mut decoder = BytesDecoder { data: &encoded, state: DecoderInit };
        assert_eq!(decoder.string_decode(), Some("12345".to_string()));
    }

    #[test]
    fn test_32_bit_integer() {
        let data = "1234567";
        let size = data.len();
        let encoded = encode_string(size, data).unwrap();
        assert_eq!(encoded[0], 0xC2);
        assert_eq!(encoded[1..], [0x87, 0xD6, 0x12, 0x00]);
    }

    #[test]
    fn test_32_bit_integer_decode() {
        let data = "1234567";
        let size = data.len();
        let encoded = encode_string(size, data).unwrap();
        let mut decoder = BytesDecoder { data: &encoded, state: DecoderInit };
        assert_eq!(decoder.string_decode(), Some("1234567".to_string()));
    }

    #[test]
    fn test_integer_decoding1() {
        let data = "42";
        let size = data.len();
        let encoded = encode_string(size, data).unwrap();
        let mut decoder = BytesDecoder { data: &encoded, state: DecoderInit };
        assert_eq!(decoder.string_decode(), Some("42".to_string()));
    }

    #[test]
    fn test_integer_decoding2() {
        let data = "1000";
        let size = data.len();
        let encoded = encode_string(size, data).unwrap();
        let mut decoder = BytesDecoder { data: &encoded, state: DecoderInit };
        assert_eq!(decoder.string_decode(), Some("1000".to_string()));
    }

    #[test]
    fn test_integer_decoding3() {
        let data = "100000";
        let size = data.len();
        let encoded = encode_string(size, data).unwrap();
        let mut decoder = BytesDecoder { data: &encoded, state: DecoderInit };
        assert_eq!(decoder.string_decode(), Some("100000".to_string()));
    }

    #[test]
    fn test_cache_value_encode() {
        let value = CacheEntry::KeyValue("key".to_string(), "value".to_string());
        let encoded = value.encode_with_key().unwrap();
        let expected = vec![
            STRING_VALUE_TYPE_INDICATOR,
            0x03,
            b'k',
            b'e',
            b'y',
            0x05,
            b'v',
            b'a',
            b'l',
            b'u',
            b'e',
        ];
        assert_eq!(encoded, expected);
    }

    #[test]
    fn test_cache_value_with_expiry_milliseconds() {
        let kvs = CacheEntry::KeyValueExpiry(
            "key".to_string(),
            "value".to_string(),
            StoredDuration::Milliseconds(1713824559637).to_systemtime(),
        );

        let encoded = kvs.encode_with_key().unwrap();
        let expected = vec![
            EXPIRY_TIME_IN_MILLISECONDS_INDICATOR,
            0x15,
            0x72,
            0xE7,
            0x07,
            0x8F,
            0x01,
            0x00,
            0x00,
            STRING_VALUE_TYPE_INDICATOR,
            0x03,
            b'k',
            b'e',
            b'y',
            0x05,
            b'v',
            b'a',
            b'l',
            b'u',
            b'e',
        ];
        assert_eq!(encoded, expected);
    }

    #[test]
    fn test_encode_header() {
        let version = "0001";
        let encoded = encode_header(version).unwrap();
        let expected = vec![0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x31];
        assert_eq!(encoded, expected);
    }

    #[test]
    fn test_encode_metadata() {
        let mut metadata = Vec::new();
        metadata.push(("key1", "value1"));
        metadata.push(("key2", "value2"));
        let encoded = encode_metadata(metadata).unwrap();
        let expected = vec![
            METADATA_SECTION_INDICATOR,
            0x04,
            b'k',
            b'e',
            b'y',
            b'1',
            0x06,
            b'v',
            b'a',
            b'l',
            b'u',
            b'e',
            b'1',
            METADATA_SECTION_INDICATOR,
            0x04,
            b'k',
            b'e',
            b'y',
            b'2',
            0x06,
            b'v',
            b'a',
            b'l',
            b'u',
            b'e',
            b'2',
        ];
        assert_eq!(encoded, expected);
    }
}
