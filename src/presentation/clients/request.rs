use crate::domains::{append_only_files::WriteRequest, peers::identifier::PeerIdentifier};
use std::time::SystemTime;

#[derive(Clone, Debug)]
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
    ClusterInfo,
    ClusterForget(PeerIdentifier),
}

impl ClientRequest {
    pub fn to_write_request(&self) -> Option<WriteRequest> {
        match self {
            ClientRequest::Set { key, value } => {
                Some(WriteRequest::Set { key: key.clone(), value: value.clone() })
            }
            ClientRequest::SetWithExpiry { key, value, expiry } => {
                let expires_at =
                    expiry.duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_millis()
                        as u64;

                Some(WriteRequest::SetWithExpiry {
                    key: key.clone(),
                    value: value.clone(),
                    expires_at,
                })
            }
            _ => None,
        }
    }
}
