// SIZE ENCODING
// Size-encoded values specify the size of something. Here are some examples:
// The database indexes and hash table sizes are size encoded.
// String encoding begins with a size-encoded value that specifies the number of characters in the string.
// List encoding begins with a size-encoded value that specifies the number of elements in the list.
// The first two bits of a size-encoded value indicate how the value should be parsed. Here's a guide (bits are shown in both hexadecimal and binary):
/*
    If the first two bits are 0b00:
    The size is the remaining 6 bits of the byte.
    In this example, the size is 10:
    0A
    00001010

    If the first two bits are 0b01:
    The size is the next 14 bits
    (remaining 6 bits in the first byte, combined with the next byte),
    in big-endian (read left-to-right).
    In this example, the size is 700:
    42 BC
    01000010 10111100

    If the first two bits are 0b10:
    Ignore the remaining 6 bits of the first byte.
    The size is the next 4 bytes, in big-endian (read left-to-right).
    In this example, the size is 17000:
    80 00 00 42 68
    10000000 00000000 00000000 01000010 01101000

    If the first two bits are 0b11:
    The remaining 6 bits specify a type of string encoding.
    See string encoding section.
*/

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

// Encode a size as a size-encoded byte vector.
fn size_encode(value: usize, data: &[u8]) -> Option<Vec<u8>> {
    let mut result = Vec::new();

    if value < (1 << 6) {
        // 0b00: Use the remaining 6 bits to represent the size.
        result.push((0b00 << 6) | (value as u8));
    } else if value < (1 << 14) {
        // 0b01: Use the next 14 bits (6 bits in the first byte + the next byte).
        result.push((0b01 << 6) | ((value >> 8) as u8 & 0x3F));
        result.push(value as u8);
    } else if value < (1 << 32) {
        // 0b10: Use the next 4 bytes.
        result.push(0b10 << 6);
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
