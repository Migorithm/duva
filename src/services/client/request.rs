use crate::{
    services::{cluster::peers::identifier::PeerIdentifier, query_io::QueryIO},
    write_array,
};
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
    // TODO return could be Option<Log>
    pub fn log(&self) -> Option<QueryIO> {
        match self {
            ClientRequest::Set { key, value } => {
                Some(write_array!("set", key.clone(), value.clone()))
            }
            ClientRequest::SetWithExpiry { key, value, expiry } => Some(
                write_array!(
                    "set",
                    key.clone(),
                    value.clone(),
                    "px",
                    expiry.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis().to_string()
                )
                .into(),
            ),
            _ => None,
        }
    }
}
