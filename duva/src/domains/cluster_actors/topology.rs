use crate::domains::{cluster_actors::hash_ring::HashRing, replications::state::ReplicationState};

#[derive(bincode::Encode, bincode::Decode, Debug, PartialEq, Clone, Default)]
pub struct Topology {
    pub repl_states: Vec<ReplicationState>,
    pub hash_ring: HashRing,
}
