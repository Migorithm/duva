use super::Data;
use anyhow::Result;
use std::panic;
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

#[derive(Default)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
    pub expiry: Option<u64>,
}

impl KeyValue {
    pub fn new(data: &mut Data) -> Result<Self> {
        KeyValue::default().extract_key_value_expiry(data)
    }
    pub fn extract_key_value_expiry(mut self, data: &mut Data) -> Result<Self> {
        while data.len() > 0 {
            match data[0] {
                //0b11111100
                0xFC => self.expiry = Some(data.when_0xFC()?),
                //0b11111101
                0xFD => self.expiry = Some(data.when_0xFD()?),
                //0b11111110
                0x00 => return data.when_0x00(self),
                _ => {
                    return Err(anyhow::anyhow!("Invalid key value pair"));
                }
            }
        }
        return Err(anyhow::anyhow!("Invalid key value pair"));
    }
}

pub fn extract_expiry_time_in_seconds(data: &mut Vec<u8>) -> Result<u64> {
    let end_pos = 4;
    let slice: &[u8] = &data[..end_pos];
    let result = u32::from_le_bytes(slice.try_into()?);
    data.drain(..end_pos);

    Ok(result as u64)
}

pub fn extract_expiry_time_in_milliseconds(data: &mut Vec<u8>) -> Result<u64> {
    let end_pos = 8;
    let slice: &[u8] = &data[..end_pos];
    if let Ok(result) = panic::catch_unwind(|| u64::from_le_bytes(slice.try_into().unwrap()).into())
    {
        data.drain(..end_pos);
        Ok(result)
    } else {
        Err(anyhow::anyhow!("Invalid expiry time in milliseconds"))
    }
}

#[test]
fn test_non_expiry_key_value_pair() {
    let mut data = vec![0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78].into();

    let key_value = KeyValue::default()
        .extract_key_value_expiry(&mut data)
        .expect("Failed to extract key value expiry");
    assert_eq!(key_value.key, "baz");
    assert_eq!(key_value.value, "qux");
    assert_eq!(key_value.expiry, None);
    assert_eq!(data.len(), 0);
}

#[test]
fn test_with_milliseconds_expiry_key_value_pair() {
    let mut data = vec![
        0xFC, 0x15, 0x72, 0xE7, 0x07, 0x8F, 0x01, 0x00, 0x00, 0x00, 0x03, 0x62, 0x61, 0x7A, 0x03,
        0x71, 0x75, 0x78,
    ]
    .into();

    let key_value = KeyValue::new(&mut data).unwrap();

    assert_eq!(key_value.key, "baz");
    assert_eq!(key_value.value, "qux");
    assert!(key_value.expiry.is_some());
    assert_eq!(data.len(), 0);
}

#[test]
fn test_with_seconds_expiry_key_value_pair() {
    let mut data = vec![
        0xFD, 0x52, 0xED, 0x2A, 0x66, 0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78,
    ]
    .into();

    let key_value = KeyValue::new(&mut data).unwrap();
    assert_eq!(key_value.key, "baz");
    assert_eq!(key_value.value, "qux");
    assert!(key_value.expiry.is_some());
    assert_eq!(data.len(), 0);
}

#[test]
fn test_invalid_expiry_key_value_pair() {
    let mut data = vec![
        0xFF, 0x52, 0xED, 0x2A, 0x66, 0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78,
    ]
    .into();

    let result = KeyValue::new(&mut data);
    assert!(result.is_err());
    assert_eq!(data.len(), 14);
}
