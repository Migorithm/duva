use crate::domains::{IoError, cluster_actors::topology::Topology, replications::ReplicationId};
use crate::prelude::PeerIdentifier;
use uuid::Uuid;

// TODO make the following enum and make it explicit about why it wants to connect
#[derive(Debug, Clone, PartialEq, Eq, Default, bincode::Decode, bincode::Encode)]
pub struct ConnectionRequest {
    pub client_id: Option<String>,
    pub request_id: u64,
}

impl ConnectionRequest {
    pub(crate) fn deconstruct(self) -> anyhow::Result<(String, u64)> {
        let client_id = self.client_id.map_or_else(
            || Ok(Uuid::now_v7()),
            |id| Uuid::parse_str(&id).map_err(|e| IoError::Custom(e.to_string())),
        )?;
        Ok((client_id.to_string(), self.request_id))
    }
}
#[derive(Debug, Clone, PartialEq, Eq, bincode::Decode, bincode::Encode)]
pub enum ConnectionRequests {
    Discovery,
    Authenticate(ConnectionRequest),
}
#[derive(Debug, Clone, bincode::Decode, bincode::Encode)]
pub enum ConnectionResponses {
    Discovery { leader_id: PeerIdentifier },
    Authenticated(ConnectionResponse),
}

#[derive(Debug, Clone, Default, bincode::Decode, bincode::Encode)]
pub struct ConnectionResponse {
    pub client_id: String,
    pub request_id: u64,
    pub topology: Topology,
    pub is_leader_node: bool,
    pub replication_id: ReplicationId,
}
