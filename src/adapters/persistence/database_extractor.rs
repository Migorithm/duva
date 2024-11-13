use crate::adapters::persistence::size_encoding::{size_decode, string_decode};
use crate::services::statefuls::CacheDb;
//```
// FE                       // Indicates the start of a database subsection.
// 00                       // Database index (size encoded): 0.
// 
// FB                       // Indicates that hash table size information follows.
// 03                       // Size of the key-value hash table (size encoded): 3.
// 02                       // Size of the expires hash table (size encoded): 2.
// 00                       // 1-byte flag specifying value type and encoding: 0 (string).
// 
// 06 66 6F 6F 62 61 72     // Key name (string encoded): "foobar".
// 06 62 61 7A 71 75 78     // Value (string encoded): "bazqux".
// 
// FC                       // Indicates that the key "foo" has an expire timestamp in milliseconds.
// 15 72 E7 07 8F 01 00 00  // Expire timestamp (8-byte unsigned long, little-endian): 1713824559637.
// 
// 00                       // Value type: string.
// 03 66 6F 6F              // Key name: "foo".
// 03 62 61 72              // Value: "bar".
// 
// FD                       // Indicates that the key "baz" has an expire timestamp in seconds.
// 52 ED 2A 66              // Expire timestamp (4-byte unsigned integer, little-endian): 1714089298.
// 
// 00                       // Value type: string.
// 03 62 61 7A              // Key name: "baz".
// 03 71 75 78              // Value: "qux".
// ```

struct DatabaseSection {
    index: usize,
    key_value_table_size: usize,
    expires_table_size: usize,
    storage: Vec<(String,String,Option<u64>)>,
    // 8-byte CRC64 checksum
    checksum: u64,
}
// fn database_section_extractor(data : &mut Vec<u8>) -> Option<DatabaseSection>{
//     let mut storage = Vec::new();
//     let index = data.remove(0);
//     let key_value_table_size = size_decode(data);
//     let expires_table_size = size_decode(data);
//     let mut checksum = 0;
//     while !data.is_empty() {
//         let key = extract_string(data);
//         let value = extract_string(data);
//         let expiry_time = match data.remove(0) {
//             0xFC => Some(u64::from_le_bytes([
// }