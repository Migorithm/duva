use crate::domains::{
    cluster_actors::replication::ReplicationState, peers::identifier::PeerIdentifier,
};

#[derive(Debug)]
pub(crate) enum ConsensusClientResponse {
    AlreadyProcessed { key: String, index: u64 },
    LogIndex(Option<u64>),
}
