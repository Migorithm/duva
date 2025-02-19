use crate::domains::peers::identifier::PeerIdentifier;
use crate::domains::peers::kind::PeerKind;
use crate::domains::peers::peer::Peer;

use crate::services::interface::TWrite;

use consensus::ConsensusTracker;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::collections::BTreeMap;

use tokio::time::Instant;
pub mod actor;
pub mod commands;
pub mod consensus;
pub mod replication;
mod replid_generator;
pub const FANOUT: usize = 2;
pub use actor::ClusterActor;
