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
