use crate::domains::peers::identifier::PeerIdentifier;
use crate::domains::peers::kind::PeerKind;
use crate::domains::peers::peer::Peer;
use crate::services::aof::{WriteOperation, WriteRequest};

use crate::services::cluster::actors::replication::{
    time_in_secs, BannedPeer, HeartBeatMessage, ReplicationInfo,
};
use crate::services::interface::TWrite;
use crate::services::query_io::QueryIO;
use consensus::ConsensusTracker;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::collections::BTreeMap;

use tokio::time::Instant;
pub mod actor;
pub mod commands;
pub mod consensus;
pub const FANOUT: usize = 2;
pub use actor::ClusterActor;
