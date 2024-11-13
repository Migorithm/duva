use std::panic;
use super::size_encoding::string_decode;

/// # Extract Key-Value Pair Storage
/// Extract key-value pair from the data buffer and remove the extracted data from the buffer.
///
/// Each key-value pair is stored as follows:
///
/// 1. Optional Expire Information:
/// - **Timestamp in Seconds:**
/// ```
/// FD
/// Expire timestamp in seconds (4-byte unsigned integer)
/// ```
/// - **Timestamp in Milliseconds:**
/// ```
/// FC
/// Expire timestamp in milliseconds (8-byte unsigned long)
/// ```
/// 2. **Value Type:** 1-byte flag indicating the type and encoding of the value.
/// 3. **Key:** String encoded.
/// 4. **Value:** Encoding depends on the value type.
///
/// # Arguments
///
/// * `data`: &mut Vec<u8>
///
/// returns: Option<(key: String, value: String, expiry_time: Option<u64>)>
///
/// # Examples
///
/// ```
/// #[test]
/// fn test_non_expiry_key_value_pair() {
///     let mut data = vec![0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78];
///     let mut_data = &mut data;
///     let (key, string, expiry_time) = extract_key_value_expiry(mut_data).expect("Failed to extract key value expiry");
///     assert_eq!(key, "baz");
///     assert_eq!(string, "qux");
///     assert_eq!(expiry_time, None);
///     assert_eq!(data.len(), 0);
/// }
///
/// #[test]
/// fn test_with_milliseconds_expiry_key_value_pair() {
///     let mut data:Vec<u8> = vec![0xFC, 0x15, 0x72, 0xE7, 0x07, 0x8F, 0x01, 0x00, 0x00, 0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78];
///     let mut_data = &mut data;
///     let (key, string, expiry_time) = extract_key_value_expiry(mut_data).expect("Failed to extract key value expiry");
///     assert_eq!(key, "baz");
///     assert_eq!(string, "qux");
///     assert!(expiry_time.is_some());
///     assert_eq!(data.len(), 0);
/// }
///
/// #[test]
/// fn test_with_seconds_expiry_key_value_pair() {
///     let mut data:Vec<u8> = vec![0xFD, 0x52, 0xED, 0x2A, 0x66, 0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78];
///     let mut_data = &mut data;
///     let (key, string, expiry_time) = extract_key_value_expiry(mut_data).expect("Failed to extract key value expiry");
///     assert_eq!(key, "baz");
///     assert_eq!(string, "qux");
///     assert!(expiry_time.is_some());
///     assert_eq!(data.len(), 0);
// }
/// ```
pub fn extract_key_value_expiry(data : &mut Vec<u8>) -> Option<(String, String, Option<u64>)> {
    let mut key = String::new();
    let mut value = String::new();
    let mut expiry_time = None;
    while data.len() > 0 {
        match data[0] {
            0xFC => {
                data.remove(0);
                expiry_time = extract_expiry_time_in_milliseconds(data);
            }
            0xFD => {
                data.remove(0);
                expiry_time = extract_expiry_time_in_seconds(data);
            }
            0x00 => {
                data.remove(0);
                let key_data = string_decode(data)?;
                key = key_data.data;
                let value_data = string_decode(data)?;
                value = value_data.data;
                return Some((key, value, expiry_time));
            }
            _ => {
                return None;
            }
        }
    }
    None
}

fn extract_expiry_time_in_seconds(data: &mut Vec<u8>) -> Option<u64>{
    let end_pos = 4;
    let slice: &[u8] = &data[..end_pos];
    if let Ok(result) = panic::catch_unwind(|| u32::from_le_bytes(slice.try_into().unwrap()).into()){
        data.drain(..end_pos);
        Some(result)
    } else {
        None
    }
}

fn extract_expiry_time_in_milliseconds(data: &mut Vec<u8>) -> Option<u64>{
    let end_pos =  8;
    let slice: &[u8] = &data[..end_pos];
    if let Ok(result) = panic::catch_unwind(|| u64::from_le_bytes(slice.try_into().unwrap()).into()){
        data.drain(..end_pos);
        Some(result)
    } else {
        None
    }
}

#[test]
fn test_non_expiry_key_value_pair() {
    let mut data = vec![0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78];
    let mut_data = &mut data;
    let (key, string, expiry_time) = extract_key_value_expiry(mut_data).expect("Failed to extract key value expiry");
    assert_eq!(key, "baz");
    assert_eq!(string, "qux");
    assert_eq!(expiry_time, None);
    assert_eq!(data.len(), 0);
}

#[test]
fn test_with_milliseconds_expiry_key_value_pair() {
    let mut data:Vec<u8> = vec![0xFC, 0x15, 0x72, 0xE7, 0x07, 0x8F, 0x01, 0x00, 0x00, 0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78];
    let mut_data = &mut data;
    let (key, string, expiry_time) = extract_key_value_expiry(mut_data).expect("Failed to extract key value expiry");
    assert_eq!(key, "baz");
    assert_eq!(string, "qux");
    assert!(expiry_time.is_some());
    assert_eq!(data.len(), 0);
}

#[test]
fn test_with_seconds_expiry_key_value_pair() {
    let mut data:Vec<u8> = vec![0xFD, 0x52, 0xED, 0x2A, 0x66, 0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78];
    let mut_data = &mut data;
    let (key, string, expiry_time) = extract_key_value_expiry(mut_data).expect("Failed to extract key value expiry");
    assert_eq!(key, "baz");
    assert_eq!(string, "qux");
    assert!(expiry_time.is_some());
    assert_eq!(data.len(), 0);
}

#[test]
fn test_invalid_expiry_key_value_pair() {
    let mut data:Vec<u8> = vec![0xFF, 0x52, 0xED, 0x2A, 0x66, 0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78];
    let mut_data = &mut data;
    let result = extract_key_value_expiry(mut_data);
    assert!(result.is_none());
    assert_eq!(data.len(), 14);
}