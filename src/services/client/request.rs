use std::time::SystemTime;

pub enum ClientRequest {
    Ping,
    Echo(String),
    Config { key: String, value: String },
    Get { key: String },
    Set { key: String, value: String },
    SetWithExpiry { key: String, value: String, expiry: SystemTime },
    Keys { pattern: Option<String> },
    Delete { key: String },
    Save,
    Info,
}
