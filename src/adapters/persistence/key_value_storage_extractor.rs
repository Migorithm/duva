use super::size_encoding::data_decode;

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
    let mut i = 0;
    while i < data.len() {
        match data[i] {
            0xFC => {
                i += 1;
                let expiry = extract_expiry_time_in_milliseconds(data.as_slice(), i);
                expiry_time = Some(expiry);
                i += 8;
            }
            0xFD => {
                i += 1;
                let expiry = extract_expiry_time_in_seconds(data.as_slice(), i);
                expiry_time = Some(expiry);
                i += 4;
            }
            00 => {
                i += 1;
                let key_data = data_decode(&data[i..])?;
                key = key_data.data ;
                i += key_data.byte_length;
                let value_data = data_decode(&data[i..])?;
                value = value_data.data;
                i += value_data.byte_length;
            }
            _ => {
                break;
            }
        }
    }
    data.drain(..i);
    Some((key, value, expiry_time))
}

fn extract_expiry_time_in_seconds(data: &[u8], start_pos: usize) -> u64{
    let end_pos = start_pos  + 4;
    let slice: &[u8] = &data[start_pos ..end_pos];
    u32::from_le_bytes(slice.try_into().unwrap()).into()
}

fn extract_expiry_time_in_milliseconds(data: &[u8], start_pos: usize) -> u64 {
    let end_pos = start_pos + 8;
    let slice: &[u8] = &data[start_pos..end_pos];
    u64::from_le_bytes(slice.try_into().unwrap()).into()
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
