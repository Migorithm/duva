use crate::domains::{append_only_files::WriteRequest, peers::identifier::PeerIdentifier};
use chrono::{DateTime, Utc};

#[derive(Clone, Debug)]
pub enum ClientAction {
    Ping,
    Echo(String),
    Config { key: String, value: String },
    Get { key: String },
    IndexGet { key: String, index: u64 },
    Set { key: String, value: String },
    SetWithExpiry { key: String, value: String, expiry: DateTime<Utc> },
    Keys { pattern: Option<String> },
    Delete { key: String },
    Save,
    Info,
    ClusterInfo,
    ClusterNodes,
    ClusterForget(PeerIdentifier),
    ReplicaOf(PeerIdentifier),
}

impl ClientAction {
    pub fn to_write_request(&self) -> Option<WriteRequest> {
        match self {
            ClientAction::Set { key, value } => {
                Some(WriteRequest::Set { key: key.clone(), value: value.clone() })
            },
            ClientAction::SetWithExpiry { key, value, expiry } => {
                let expires_at = expiry.timestamp_millis() as u64;

                Some(WriteRequest::SetWithExpiry {
                    key: key.clone(),
                    value: value.clone(),
                    expires_at,
                })
            },
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClientRequest {
    pub action: ClientAction,
    pub request_id: Option<u64>,
}
