use crate::domains::{
    append_only_files::WriteOperation, cluster_actors::replication::ReplicationId,
    peers::identifier::PeerIdentifier,
};

#[derive(Debug, Clone, PartialEq, bincode::Encode, bincode::Decode)]

pub struct HeartBeatMessage {
    pub(crate) heartbeat_from: PeerIdentifier,
    pub(crate) term: u64,
    pub(crate) hwm: u64,
    pub(crate) leader_replid: ReplicationId,
    pub(crate) hop_count: u8, // Decremented on each hop - for gossip
    pub(crate) ban_list: Vec<BannedPeer>,
    pub(crate) append_entries: Vec<WriteOperation>,
    pub(crate) cluster_nodes: Vec<String>,
    // voting data structure required!
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
