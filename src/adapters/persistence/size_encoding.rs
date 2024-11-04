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
//! ```
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

// Decode a size-encoded value based on the first two bits and return the decoded value as a string.
fn size_decode(encoded: &[u8]) -> Option<String> {
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
        0b11 => return None, // Handle specific string encoding if needed.

        _ => return None, // Fallback for unexpected cases.
    };

    // Ensure we have enough bytes in `encoded` for the decoded string.
    if encoded.len() < size + 1 {
        return None;
    }

    // Convert the size-specified bytes into a UTF-8 string.
    match String::from_utf8(encoded[1..=size].to_vec()) {
        Ok(result) => Some(result),
        Err(_) => None, // Handle non-UTF-8 encoded data.
    }
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
fn size_encode(value: usize, data: &[u8]) -> Option<Vec<u8>> {
    let mut result = Vec::new();

    // if value is representable with 6bits : 0b00 -> Use the remaining 6 bits to represent the size.
    if value < (1 << 6) {
        result.push((0b00 << 6) | (value as u8));
    }
    // if value is representable with 14bits : 0b01 -> Use the next 14 bits (6 bits in the first byte + the next byte).
    else if value < (1 << 14) {
        // Big endian
        result.push((0b01 << 6) | ((value >> 8) as u8 & 0x3F));
        result.push(value as u8);
    }
    // if value is representable with 32bits : 0b10 -> Use the next 4 bytes.
    else if value < (1 << 32) {
        result.push(0b10 << 6);
        //`to_be_bytes` returns big endian
        result.extend_from_slice(&(value as u32).to_be_bytes());
    } else {
        // Size too large to encode within the supported format.
        return None;
    }

    // Append the data to be encoded as a string after the size.
    result.extend_from_slice(data);
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

    assert!(size_decode(&example1).is_some());
    assert!(size_decode(&example2).is_none()); // due to insufficient bytes
    assert_eq!(size_decode(&example1), Some("Hello, World!".to_string()));
}

#[test]
fn test_size_encode_6_bit() {
    // 6-bit size encoding (0b00): Size is 10, "0A" in hex.
    let data = "test".as_bytes();

    let size = 10;
    let encoded = size_encode(size, data).expect("Encoding failed");
    assert_eq!(encoded[0], 0b00_001010); // 6-bit size encoding
    assert_eq!(&encoded[1..], data); // Check the appended data.
}

#[test]
fn test_size_encode_14_bit() {
    // 14-bit size encoding (0b01): Size is 700.
    let data = "example".as_bytes();
    let size = 700;
    let encoded = size_encode(size, data).expect("Encoding failed");
    assert_eq!(encoded[0] >> 6, 0b01); // First two bits should be 0b01.
    assert_eq!(
        ((encoded[0] & 0x3F) as usize) << 8 | (encoded[1] as usize),
        size
    ); // Check the 14-bit size.
    assert_eq!(&encoded[2..], data); // Check the appended data.
}

#[test]
fn test_size_encode_32_bit() {
    // 32-bit size encoding (0b10): Size is 17000.
    let data = "test32bit".as_bytes();
    let size = 17000;
    let encoded = size_encode(size, data).expect("Encoding failed");
    assert_eq!(encoded[0] >> 6, 0b10); // First two bits should be 0b10.

    // Check 4-byte big-endian size encoding.
    let expected_size = (encoded[1] as usize) << 24
        | (encoded[2] as usize) << 16
        | (encoded[3] as usize) << 8
        | (encoded[4] as usize);
    assert_eq!(expected_size, size);
    assert_eq!(&encoded[5..], data); // Check the appended data.
}

#[test]
fn test_size_encode_too_large() {
    // Ensure encoding fails for sizes too large to encode (greater than 2^32).
    let data = "overflow".as_bytes();
    let size = usize::MAX; // Maximum usize value, likely to exceed the allowed encoding size.
    let encoded = size_encode(size, data);
    assert!(encoded.is_none(), "Encoding should fail for too large size");
}
