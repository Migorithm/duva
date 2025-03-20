use crate::domains::{
    append_only_files::WriteRequest,
    cluster_actors::commands::{ClusterCommand, WriteConsensusResponse},
    peers::identifier::PeerIdentifier,
};
use chrono::{DateTime, Utc};
use tokio::sync::oneshot::Receiver;

#[derive(Clone, Debug)]
pub enum ClientRequest {
    Ping,
    Echo(String),
    Config { key: String, value: String },
    Get { key: String },
    Set { key: String, value: String },
    SetWithExpiry { key: String, value: String, expiry: DateTime<Utc> },
    Keys { pattern: Option<String> },
    Delete { key: String },
    Save,
    Info,
    ClusterInfo,
    ClusterNodes,
    ClusterForget(PeerIdentifier),
}

impl ClientRequest {
    pub fn to_write_request(&self) -> Option<(ClusterCommand, Receiver<WriteConsensusResponse>)> {
        match self {
            ClientRequest::Set { key, value } => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                Some((
                    ClusterCommand::LeaderReqConsensus {
                        log: WriteRequest::Set { key: key.clone(), value: value.clone() },
                        sender: tx,
                    },
                    rx,
                ))
            },
            ClientRequest::SetWithExpiry { key, value, expiry } => {
                let expires_at = expiry.timestamp_millis() as u64;
                let (tx, rx) = tokio::sync::oneshot::channel();

                Some((
                    ClusterCommand::LeaderReqConsensus {
                        log: WriteRequest::SetWithExpiry {
                            key: key.clone(),
                            value: value.clone(),
                            expires_at,
                        },
                        sender: tx,
                    },
                    rx,
                ))
            },
            _ => None,
        }
    }
}
