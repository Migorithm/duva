use crate::domains::peers::identifier::PeerIdentifier;
use crate::domains::peers::peer::Peer;
use crate::domains::peers::peer::PeerKind;
use crate::services::interface::TWrite;
use consensus::ConsensusTracker;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::collections::BTreeMap;
use tokio::time::Instant;
pub mod actor;
pub mod commands;
pub mod consensus;
pub mod replication;

pub const FANOUT: usize = 2;
pub use actor::ClusterActor;
