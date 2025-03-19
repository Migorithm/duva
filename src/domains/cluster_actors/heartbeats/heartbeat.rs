use crate::domains::{
    append_only_files::{WriteOperation, log::LogIndex},
    cluster_actors::replication::ReplicationId,
    peers::identifier::PeerIdentifier,
};

#[derive(Debug, Clone, PartialEq, bincode::Encode, bincode::Decode)]

pub struct HeartBeatMessage {
    pub(crate) heartbeat_from: PeerIdentifier,
    pub(crate) term: u64,
    pub(crate) hwm: u64,
    pub(crate) replid: ReplicationId,
    pub(crate) hop_count: u8,
    pub(crate) ban_list: Vec<BannedPeer>,
    pub(crate) append_entries: Vec<WriteOperation>,
    pub(crate) cluster_nodes: Vec<String>,
    pub(crate) prev_log_index: LogIndex, //index of log entry immediately preceding new ones
    pub(crate) prev_log_term: u64,       //term of prev_log_index entry
}
impl HeartBeatMessage {
    pub(crate) fn set_append_entries(mut self, entries: Vec<WriteOperation>) -> Self {
        self.append_entries = entries;
        self
    }

    pub(crate) fn set_cluster_nodes(mut self, cluster_nodes: Vec<String>) -> Self {
        self.cluster_nodes = cluster_nodes;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, bincode::Encode, bincode::Decode)]
pub struct BannedPeer {
    pub(crate) p_id: PeerIdentifier,
    pub(crate) ban_time: u64,
}

#[derive(Debug, Clone, PartialEq, bincode::Encode, bincode::Decode)]
pub(crate) struct AppendEntriesRPC(pub(crate) HeartBeatMessage);
#[derive(Debug, Clone, PartialEq, bincode::Encode, bincode::Decode)]
pub(crate) struct ClusterHeartBeat(pub(crate) HeartBeatMessage);
