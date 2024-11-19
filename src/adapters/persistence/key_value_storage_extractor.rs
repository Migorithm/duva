use crate::adapters::persistence::bytes_handler::BytesEndec;
use anyhow::Result;
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

pub struct KeyValueStorage {
    pub key: String,
    pub value: String,
    pub expiry: Option<u64>,
}

impl TryFrom<&mut BytesEndec> for KeyValueStorage {
    type Error = anyhow::Error;
    fn try_from(data: &mut BytesEndec) -> Result<Self> {
        let mut expiry: Option<u64> = None;
        while data.len() > 0 {
            match data[0] {
                //0b11111100
                0xFC => {
                    expiry = Some(data.try_extract_expiry_time_in_milliseconds()?);
                }
                //0b11111101
                0xFD => {
                    expiry = Some(data.try_extract_expiry_time_in_seconds()?);
                }
                //0b11111110
                0x00 => {
                    let (key, value) = data.try_extract_key_value()?;
                    return Ok(KeyValueStorage { key, value, expiry });
                }
                _ => {
                    return Err(anyhow::anyhow!("Invalid key value pair"));
                }
            }
        }
        Err(anyhow::anyhow!("Invalid key value pair"))
    }
}

#[test]
fn test_non_expiry_key_value_pair() {
    let mut data = vec![0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78].into();

    let key_value =
        KeyValueStorage::try_from(&mut data).expect("Failed to extract key value expiry");
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

    let key_value = KeyValueStorage::try_from(&mut data).unwrap();

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

    let key_value = KeyValueStorage::try_from(&mut data).unwrap();
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

    let result = KeyValueStorage::try_from(&mut data);
    assert!(result.is_err());
    assert_eq!(data.len(), 14);
}
