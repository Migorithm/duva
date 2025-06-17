use crate::domains::peers::identifier::PeerIdentifier;
use crate::domains::peers::peer::Peer;
use consensus::LogConsensusTracker;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::collections::BTreeMap;
use tokio::time::Instant;
pub mod actor;
mod command;
pub(crate) use command::*;
pub mod consensus;
pub(crate) mod hash_ring;

pub mod replication;
mod service;
pub(crate) mod topology;

pub const FANOUT: usize = 2;
pub use actor::ClusterActor;
