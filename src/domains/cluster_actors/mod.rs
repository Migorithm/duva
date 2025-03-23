use crate::domains::peers::identifier::PeerIdentifier;
use crate::domains::peers::peer::Peer;
use crate::domains::peers::peer::PeerState;
use consensus::LogConsensusTracker;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::collections::BTreeMap;
use tokio::time::Instant;
pub mod actor;
pub mod commands;

pub mod consensus;

pub mod election_state;
pub mod heartbeats;
pub mod replication;

pub const FANOUT: usize = 2;
pub use actor::ClusterActor;
