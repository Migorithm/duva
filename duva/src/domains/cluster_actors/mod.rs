use crate::domains::peers::identifier::PeerIdentifier;
use crate::domains::peers::peer::Peer;

use consensus::LogConsensusTracker;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::collections::BTreeMap;
use tokio::time::Instant;
pub mod actor;
pub mod commands;
pub mod consensus;
pub(crate) mod hash_ring;
pub mod heartbeats;

pub(crate) mod peer_connections;
pub mod replication;
mod service;
pub mod session;
pub const FANOUT: usize = 2;
pub use actor::ClusterActor;
