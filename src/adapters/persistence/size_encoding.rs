//! A module providing variable-length size encoding followed by arbitrary data.
//!
//! This module implements an encoding scheme that can encode a size value using a variable
//! number of bytes based on the magnitude of the size, followed by arbitrary data bytes.
//! The size encoding uses a prefix to indicate how many bytes are used to represent the size:
//!
//! # Size Encoding Format
//!
//! The size value is encoded using one of three formats, chosen based on the value's magnitude:
//!
//! * 6-bit (1 byte total):  `00xxxxxx`
//!   - First 2 bits are `00`
//!   - Next 6 bits contain size value
//!   - Can encode sizes 0-63
//!
//! * 14-bit (2 bytes total): `01xxxxxx yyyyyyyy`
//!   - First 2 bits are `01`
//!   - Next 14 bits contain size value in big-endian order
//!   - Can encode sizes 64-16383
//!
//! * 32-bit (5 bytes total): `10000000 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx`
//!   - First 2 bits are `10`
//!   - Next 4 bytes contain size value in big-endian order
//!   - Can encode sizes 16384-4294967295
//!
//! After the size encoding, the arbitrary data bytes follow immediately.
//!
//! # Examples
//!
//! Basic usage:
//! ```rust
//! # fn main() -> Option<()> {
//! let data = "Hello".as_bytes();
//! let size = 42;
//!
//! // Encode size 42 (fits in 6 bits) followed by "Hello"
//! let encoded = size_encode(size, data)?;
//!
//! // First byte should be 00101010 (0b00 prefix + 42)
//! assert_eq!(encoded[0], 0b00101010);
//!
//! // Remaining bytes should be "Hello"
//! assert_eq!(&encoded[1..], b"Hello");
//! # Some(())
//! # }
//! ```
//!
//! The benefit is that size_encode allows you to encode any size value independently of the data length. This is useful when:
//!
//! 1. You're sending a header for a larger message:
//! ```rust,text
//! // First message part
//! let header = size_encode(1000, b"start").unwrap();
//! send(header);  // Sends: size=1000 + data="start"
//! // Later messages
//! send(b"more data");  // Just data
//! send(b"final part"); // Just data
//! ```
//!
//! It's primarily about communication/protocol rather than efficiency.
use std::ops::RangeInclusive;

#[derive(Debug, PartialEq)]
pub struct DecodedData {
    pub size: usize,
    pub data: String,
}
impl From<String> for DecodedData {
    fn from(data: String) -> Self {
        DecodedData {
            size: data.len(),
            data,
        }
    }
}

// Decode a size-encoded value based on the first two bits and return the decoded value as a string.
pub fn data_decode(encoded: &[u8]) -> Option<DecodedData> {
    // Ensure we have at least one byte to read.
    if encoded.is_empty() {
        return None;
    }

    let first_byte = encoded[0];
    let size = match first_byte >> 6 {
        // 0b00: The size is the remaining 6 bits of the byte.
        0b00 => (first_byte & 0x3F) as usize,

        // 0b01: The size is in the next 14 bits (6 bits of the first byte + next byte).
        0b01 => {
            if encoded.len() < 2 {
                return None;
            }
            (((first_byte & 0x3F) as usize) << 8) | (encoded[1] as usize)
        }

        // 0b10: The size is in the next 4 bytes (ignoring the remaining 6 bits of the first byte).
        0b10 => {
            if encoded.len() < 5 {
                return None;
            }
            ((encoded[1] as usize) << 24)
                | ((encoded[2] as usize) << 16)
                | ((encoded[3] as usize) << 8)
                | (encoded[4] as usize)
        }

        // 0b11: The remaining 6 bits specify a type of string encoding.
        0b11 => {
            return integer_decode(encoded);
        }
        _ => return None, // Fallback for unexpected cases.
    };
    
    if size > encoded.len() - 1 {
        return None;
    }
    
    let data = String::from_utf8(encoded[1..=size].to_vec()).ok()?;
    Some(DecodedData { size, data })
}

fn integer_decode(encoded: &[u8]) -> Option<DecodedData> {
    if let Some(first_byte) = encoded.get(0) {
        match first_byte {
            // 0b11000000: 8-bit integer
            0xC0 => return Some(u8::from_le_bytes([encoded[1]]).to_string().into()),
            0xC1 => {
                if encoded.len() == 3 {
                    let value = u16::from_le_bytes(extract_range(encoded, 1..=2)?);
                    return Some(value.to_string().into());
                }
            }
            0xC2 => {
                if encoded.len() == 5 {
                    let value = u32::from_le_bytes(extract_range(encoded, 1..=4)?);
                    return Some(value.to_string().into());
                }
            }
            _ => return None,
        }
    }
    None
}

fn extract_range<const N: usize>(encoded: &[u8], range: RangeInclusive<usize>) -> Option<[u8; N]> {
    TryInto::<[u8; N]>::try_into(encoded.get(range)?).ok()
}

/// # Notes
///
/// * The size value does not need to match the length of the data. This allows for:
///   - Streaming scenarios where the size represents total expected bytes
///   - Protocol-specific uses where size might have different meaning
///   - Partial message transmission

/// * Size values larger than 2^32 - 1 cannot be encoded and will return None
///
/// # Limitations
///
/// * Maximum encodable size is 2^32 - 1
/// * No error correction or detection mechanisms
/// * Size encoding overhead varies from 1 to 5 bytes
pub fn data_encode(size: usize, data: &str) -> Option<Vec<u8>> {
    let mut result = Vec::new();

    // if data can be parsed as an integer as u32
    if let Ok(value) = data.parse::<u32>() {
        return if value <= 0xFF {
            result.push(0xC0);
            result.push(value as u8);
            Some(result)
        } else if value <= 0xFFFF {
            result.push(0xC1);
            let value = value as u16;
            result.extend_from_slice(&value.to_le_bytes());
            Some(result)
        } else {
            result.push(0xC2);
            result.extend_from_slice(&value.to_le_bytes());
            Some(result)
        };
    }

    // if value is representable with 6bits : 0b00 -> Use the remaining 6 bits to represent the size.
    if size < (1 << 6) {
        result.push((0b00 << 6) | (size as u8));
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
        return None;
    }

    // Append the data to be encoded as a string after the size.
    result.extend_from_slice(data.as_bytes());
    Some(result)
}

#[test]
fn test_decoding() {
    // "Hello, World!"
    let example1 = vec![
        0x0D, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21,
    ];

    // "Test", with size 10 (although more bytes needed)
    let example2 = vec![0x42, 0x0A, 0x54, 0x65, 0x73, 0x74];

    assert!(data_decode(&example1).is_some());
    assert!(data_decode(&example2).is_none()); // due to insufficient bytes
    assert_eq!(data_decode(&example1), Some("Hello, World!".to_string().into()));
}

#[test]
fn test_size_encode_6_bit() {
    // 6-bit size encoding (0b00): Size is 10, "0A" in hex.
    let data = "test";
    let size = 10;
    let encoded = data_encode(size, data).expect("Encoding failed");
    assert_eq!(encoded[0], 0b00_001010); // 6-bit size encoding
    assert_eq!(&encoded[1..], data.as_bytes()); // Check the appended data.
}

#[test]
fn test_size_encode_14_bit() {
    // 14-bit size encoding (0b01): Size is 700.
    let data = "example";
    let size = 700;
    let encoded = data_encode(size, data).expect("Encoding failed");
    assert_eq!(encoded[0] >> 6, 0b01); // First two bits should be 0b01.
    assert_eq!(
        ((encoded[0] & 0x3F) as usize) << 8 | (encoded[1] as usize),
        size
    ); // Check the 14-bit size.
    assert_eq!(&encoded[2..], data.as_bytes()); // Check the appended data.
}

#[test]
fn test_size_encode_32_bit() {
    // 32-bit size encoding (0b10): Size is 17000.
    let data = "test32bit";
    let size = 17000;
    let encoded = data_encode(size, data).expect("Encoding failed");
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
    let encoded = data_encode(size, data);
    assert!(encoded.is_none(), "Encoding should fail for too large size");
}

#[test]
fn test_long_string() {
    // Create a string of length 30000
    let long_string = "A".repeat(30000);
    let data = &long_string;

    let encoded = data_encode(30000, data).unwrap();

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
    let encoded = data_encode(size, data).unwrap();
    assert_eq!(encoded[0], 0xC0);
    assert_eq!(encoded[1], 0x7B);
}

#[test]
fn test_8_bit_integer_decode() {
    let data = "123";
    let size = data.len();
    let encoded = data_encode(size, data).unwrap();
    assert_eq!(data_decode(&encoded), Some("123".to_string().into()));
}

#[test]
fn test_16_bit_integer() {
    let data = "12345";
    let size = data.len();
    let encoded = data_encode(size, data).unwrap();
    assert_eq!(encoded[0], 0xC1);
    assert_eq!(encoded[1..], [0x39, 0x30]);
}

#[test]
fn test_16_bit_integer_decode() {
    let data = "12345";
    let size = data.len();
    let encoded = data_encode(size, data).unwrap();
    assert_eq!(data_decode(&encoded), Some("12345".to_string().into()));
}

#[test]
fn test_32_bit_integer() {
    let data = "1234567";
    let size = data.len();
    let encoded = data_encode(size, data).unwrap();
    assert_eq!(encoded[0], 0xC2);
    assert_eq!(encoded[1..], [0x87, 0xD6, 0x12, 0x00]);
}

#[test]
fn test_32_bit_integer_decode() {
    let data = "1234567";
    let size = data.len();
    let encoded = data_encode(size, data).unwrap();
    assert_eq!(data_decode(&encoded), Some("1234567".to_string().into()));
}

#[test]
fn test_integer_decoding() {
    let data = "42";
    let size = data.len();
    let encoded = data_encode(size, data).unwrap();
    assert_eq!(data_decode(&encoded), Some("42".to_string().into()));

    let data = "1000";
    let size = data.len();
    let encoded = data_encode(size, data).unwrap();
    assert_eq!(data_decode(&encoded), Some("1000".to_string().into()));

    let data = "100000";
    let size = data.len();
    let encoded = data_encode(size, data).unwrap();
    assert_eq!(data_decode(&encoded), Some("100000".to_string().into()));
}
