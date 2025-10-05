use crate::{
    domains::{QueryIO, cluster_actors::ClusterCommand},
    prelude::PeerIdentifier,
};

pub(crate) use peer_messages::*;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PeerCommand {
    pub(crate) from: PeerIdentifier,
    pub(crate) msg: Vec<PeerMessage>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PeerMessage {
    AppendEntriesRPC(HeartBeat),
    ClusterHeartBeat(HeartBeat),
    AckReplication(ReplicationAck),
    RequestVote(RequestVote),
    ElectionVoteReply(ElectionVote),
    StartRebalance,
    ReceiveBatch(BatchEntries),
    MigrationBatchAck(BatchId),
}

impl TryFrom<QueryIO> for PeerMessage {
    type Error = anyhow::Error;
    fn try_from(query: QueryIO) -> anyhow::Result<Self> {
        match query {
            | QueryIO::AppendEntriesRPC(peer_state) => Ok(Self::AppendEntriesRPC(peer_state)),
            | QueryIO::ClusterHeartBeat(heartbeat) => Ok(Self::ClusterHeartBeat(heartbeat)),
            | QueryIO::Ack(acks) => Ok(PeerMessage::AckReplication(acks)),
            | QueryIO::RequestVote(vote) => Ok(PeerMessage::RequestVote(vote)),
            | QueryIO::RequestVoteReply(reply) => Ok(PeerMessage::ElectionVoteReply(reply)),
            | QueryIO::StartRebalance => Ok(PeerMessage::StartRebalance),
            | QueryIO::MigrateBatch(batch) => Ok(PeerMessage::ReceiveBatch(batch)),
            | QueryIO::MigrationBatchAck(ack) => Ok(PeerMessage::MigrationBatchAck(ack)),
            | _ => Err(anyhow::anyhow!("Invalid data")),
        }
    }
}

impl From<PeerCommand> for ClusterCommand {
    fn from(cmd: PeerCommand) -> Self {
        ClusterCommand::Peer(cmd)
    }
}

mod peer_messages {
    use std::{
        collections::{HashMap, VecDeque},
        hash::Hash,
    };

    use super::*;
    use crate::{
        domains::{
            caches::cache_objects::CacheEntry,
            cluster_actors::{ConsensusRequest, hash_ring::HashRing},
            replications::WriteOperation,
            replications::{ReplicationId, state::ReplicationState},
        },
        types::Callback,
    };

    #[derive(Clone, Debug, PartialEq, Eq, bincode::Encode, bincode::Decode)]
    pub struct RequestVote {
        pub(crate) term: u64, // current term of the candidate. Without it, the old leader wouldn't be able to step down gracefully.
        pub(crate) candidate_id: PeerIdentifier,
        pub(crate) last_log_index: u64,
        pub(crate) last_log_term: u64, //the term of the last log entry, used for election restrictions. If the term is low, it won't win the election.
    }

    #[derive(Clone, Debug, PartialEq, Eq, bincode::Encode, bincode::Decode)]
    pub struct ElectionVote {
        pub(crate) term: u64,
        pub(crate) vote_granted: bool,
    }

    #[derive(Debug, Clone, PartialEq, Eq, bincode::Decode, bincode::Encode)]
    pub struct ReplicationAck {
        pub(crate) log_idx: u64,
        pub(crate) term: u64,
        pub(crate) rej_reason: Option<RejectionReason>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, bincode::Decode, bincode::Encode)]
    pub(crate) enum RejectionReason {
        ReceiverHasHigherTerm,
        LogInconsistency,
        FailToWrite,
    }

    impl ReplicationAck {
        pub(crate) fn ack(log_idx: u64, curr_term: u64) -> Self {
            Self { log_idx, term: curr_term, rej_reason: None }
        }

        pub(crate) fn reject(log_idx: u64, reason: RejectionReason, curr_term: u64) -> Self {
            Self { log_idx, term: curr_term, rej_reason: Some(reason) }
        }

        pub(crate) fn is_granted(&self) -> bool {
            self.rej_reason.is_none()
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, bincode::Encode, bincode::Decode, Default)]
    pub struct HeartBeat {
        pub(crate) from: PeerIdentifier,
        pub(crate) term: u64,
        pub(crate) leader_commit_idx: Option<u64>,
        pub(crate) replid: ReplicationId,
        pub(crate) hop_count: u8,
        pub(crate) banlist: Vec<BannedPeer>,
        pub(crate) append_entries: Vec<WriteOperation>,
        pub(crate) cluster_nodes: Vec<ReplicationState>,
        pub(crate) prev_log_index: u64, //index of log entry immediately preceding new ones
        pub(crate) prev_log_term: u64,  //term of prev_log_index entry
        pub(crate) hashring: Option<Box<HashRing>>,
    }
    impl HeartBeat {
        pub(crate) fn set_append_entries(self, append_entries: Vec<WriteOperation>) -> Self {
            Self { append_entries, ..self }
        }

        pub(crate) fn set_prev_log(self, prev_log_index: u64, prev_log_term: u64) -> Self {
            Self { prev_log_index, prev_log_term, ..self }
        }

        pub(crate) fn set_cluster_nodes(self, cluster_nodes: Vec<ReplicationState>) -> Self {
            Self { cluster_nodes, ..self }
        }

        pub(crate) fn set_hashring(&self, ring: HashRing) -> Self {
            Self { hashring: Some(Box::new(ring)), ..self.clone() }
        }

        pub(crate) fn set_banlist(self, banlist: Vec<BannedPeer>) -> Self {
            Self { banlist, ..self }
        }
    }

    #[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord, bincode::Encode, bincode::Decode)]
    pub struct BannedPeer {
        pub(crate) p_id: PeerIdentifier,
        pub(crate) ban_time: u64,
    }

    #[derive(Debug, Clone, PartialEq, Eq, bincode::Encode, bincode::Decode, Hash)]
    pub struct BatchId(pub(crate) String);

    #[derive(Debug, Clone, PartialEq, Eq, bincode::Encode, bincode::Decode)]
    pub struct BatchEntries {
        pub(crate) batch_id: BatchId,
        pub(crate) entries: Vec<CacheEntry>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct PendingMigrationTask {
        pub(crate) batch_id: BatchId,
        pub(crate) target_repl: ReplicationId,
        pub(crate) chunks: Vec<MigrationChunk>,
    }

    impl PendingMigrationTask {
        pub(crate) fn new(target_repl: ReplicationId, tasks: Vec<MigrationChunk>) -> Self {
            Self { batch_id: BatchId(uuid::Uuid::now_v7().to_string()), target_repl, chunks: tasks }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct MigrationChunk {
        pub(crate) range: (u64, u64),            // (start_hash, end_hash)
        pub(crate) keys_to_migrate: Vec<String>, // actual keys in this range
    }

    #[derive(Debug)]
    pub(crate) struct QueuedKeysToMigrate {
        pub(crate) callback: Callback<anyhow::Result<()>>,
        pub(crate) keys: Vec<String>,
    }

    #[derive(Debug, Default)]
    pub(crate) struct PendingRequests {
        requests: VecDeque<ConsensusRequest>,
        batches: HashMap<BatchId, QueuedKeysToMigrate>,
        pub(crate) callbacks: Vec<Callback<()>>,
    }
    impl PendingRequests {
        pub(crate) fn add_req(&mut self, req: ConsensusRequest) {
            self.requests.push_back(req);
        }
        pub(crate) fn store_batch(&mut self, id: BatchId, batch: QueuedKeysToMigrate) {
            self.batches.insert(id, batch);
        }
        pub(crate) fn pop_batch(&mut self, id: &BatchId) -> Option<QueuedKeysToMigrate> {
            self.batches.remove(id)
        }
        pub(crate) fn extract_requests(&mut self) -> VecDeque<ConsensusRequest> {
            std::mem::take(&mut self.requests)
        }

        #[cfg(test)]
        pub(crate) fn num_reqs(&self) -> usize {
            self.requests.len()
        }

        pub(crate) fn num_batches(&self) -> usize {
            self.batches.len()
        }
    }
}
