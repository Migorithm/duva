use std::collections::HashMap;

use crate::domains::{
    cluster_actors::replication::ReplicationId,
    operation_logs::WriteOperation,
    peers::{identifier::PeerIdentifier, peer::PeerState},
};

#[derive(Clone, Debug, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct ClientSessionInfo {
    pub(crate) client_id: uuid::Uuid,
    pub(crate) processed_req_id: u64,
    pub(crate) timestamp: u64,
}

#[derive(Debug, Clone, PartialEq, bincode::Encode, bincode::Decode)]
pub(crate) struct HeartBeatMessage {
    pub(crate) from: PeerIdentifier,
    pub(crate) term: u64,
    pub(crate) hwm: u64,
    pub(crate) replid: ReplicationId,
    pub(crate) hop_count: u8,
    pub(crate) ban_list: Vec<BannedPeer>,
    pub(crate) append_entries: Vec<WriteOperation>,
    #[bincode(with_serde)]
    pub(crate) client_sessions: HashMap<uuid::Uuid, ClientSessionInfo>,
    pub(crate) cluster_nodes: Vec<PeerState>,
    pub(crate) prev_log_index: u64, //index of log entry immediately preceding new ones
    pub(crate) prev_log_term: u64,  //term of prev_log_index entry
}
impl HeartBeatMessage {
    pub(crate) fn set_append_entries(mut self, entries: Vec<WriteOperation>) -> Self {
        self.append_entries = entries;
        self
    }

    pub(crate) fn set_cluster_nodes(mut self, cluster_nodes: Vec<PeerState>) -> Self {
        self.cluster_nodes = cluster_nodes;
        self
    }

    pub(crate) fn set_client_sessions(
        mut self,
        sessions: HashMap<uuid::Uuid, ClientSessionInfo>,
    ) -> Self {
        self.client_sessions = sessions;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, bincode::Encode, bincode::Decode)]
pub struct BannedPeer {
    pub(crate) p_id: PeerIdentifier,
    pub(crate) ban_time: u64,
}

#[derive(Debug, Clone, PartialEq, bincode::Encode, bincode::Decode)]
pub struct AppendEntriesRPC(pub(crate) HeartBeatMessage);
#[derive(Debug, Clone, PartialEq, bincode::Encode, bincode::Decode)]
pub struct ClusterHeartBeat(pub(crate) HeartBeatMessage);
