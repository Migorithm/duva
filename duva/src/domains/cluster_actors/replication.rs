use crate::domains::cluster_actors::consensus::election::ElectionVotes;
use crate::domains::operation_logs::interfaces::TWriteAheadLog;
use crate::domains::operation_logs::logger::ReplicatedLogs;
use crate::domains::peers::command::BannedPeer;
use crate::domains::peers::command::HeartBeat;
use crate::domains::peers::identifier::PeerIdentifier;

use crate::domains::peers::peer::PeerState;
use std::collections::HashSet;
use std::fmt::Display;

use std::sync::atomic::Ordering;

#[derive(Debug)]
pub(crate) struct ReplicationState<T> {
    pub(crate) replid: ReplicationId, // The replication ID of the master example: 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb
    pub(crate) role: ReplicationRole,
    pub(crate) self_host: String,
    pub(crate) self_port: u16,
    // * state is shared among peers
    pub(crate) term: u64,
    pub(crate) banlist: HashSet<BannedPeer>,
    pub(crate) election_votes: ElectionVotes,
    pub(crate) logger: ReplicatedLogs<T>,
    pub(crate) last_applied: u64,
}

impl<T: TWriteAheadLog> ReplicationState<T> {
    pub(crate) fn new(
        replid: ReplicationId,
        role: ReplicationRole,
        self_host: &str,
        self_port: u16,
        logger: ReplicatedLogs<T>,
    ) -> Self {
        Self {
            election_votes: ElectionVotes::default(),
            role,
            replid,
            self_host: self_host.to_string(),
            self_port,
            term: 0,
            banlist: Default::default(),
            last_applied: 0,
            logger,
        }
    }

    pub(crate) fn reset_election_votes(&mut self) {
        self.election_votes = ElectionVotes::default();
    }

    pub(crate) fn info(&self) -> ReplicationInfo {
        ReplicationInfo {
            replid: self.replid.clone(),
            last_log_idx: self.logger.last_log_index,
            role: self.role.clone(),
            self_host: self.self_host.clone(),
            self_port: self.self_port,
            term: self.term,
        }
    }

    pub(super) fn self_info(&self) -> PeerState {
        PeerState {
            id: self.self_identifier(),
            last_log_index: self.logger.last_log_index,
            replid: self.replid.clone(),
            role: self.role.clone(),
        }
    }

    pub(crate) fn self_identifier(&self) -> PeerIdentifier {
        PeerIdentifier::new(&self.self_host, self.self_port)
    }

    pub(super) fn in_ban_list(&self, peer_identifier: &PeerIdentifier) -> bool {
        let Ok(current_time) = time_in_secs() else { return false };
        self.banlist.get(peer_identifier).is_some_and(|node| current_time - node.ban_time < 60)
    }

    pub(super) fn default_heartbeat(&self, hop_count: u8) -> HeartBeat {
        HeartBeat {
            from: self.self_identifier(),
            term: self.term,
            leader_commit_idx: self
                .is_leader()
                .then_some(self.logger.con_idx.load(Ordering::Relaxed)),
            replid: self.replid.clone(),
            hop_count,
            ban_list: self.banlist.iter().cloned().collect(),
            prev_log_index: self.logger.last_log_index,
            prev_log_term: self.logger.last_log_term,
            ..Default::default()
        }
    }

    pub(super) fn revert_voting(&mut self, term: u64, candidate_id: &PeerIdentifier) {
        self.election_votes.votes.remove(&candidate_id);
        self.term = term;
    }

    pub(super) fn vote_for(&mut self, candidate_id: PeerIdentifier) {
        self.election_votes.votes.insert(candidate_id);
    }

    pub(crate) fn is_leader(&self) -> bool {
        self.role == ReplicationRole::Leader
    }

    pub(crate) fn is_candidate(&self) -> bool {
        self.election_votes.votes.contains(&self.self_identifier())
    }

    pub(super) fn is_log_up_to_date(
        &self,
        candidate_last_log_index: u64,
        candidate_last_log_term: u64,
    ) -> bool {
        if candidate_last_log_term > self.logger.last_log_term {
            return true;
        }

        candidate_last_log_term == self.logger.last_log_term
            && candidate_last_log_index >= self.logger.last_log_index
    }
}

pub(crate) fn time_in_secs() -> anyhow::Result<u64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs())
}

#[derive(
    Debug, Clone, PartialEq, Default, Eq, PartialOrd, Ord, bincode::Encode, bincode::Decode, Hash,
)]
pub enum ReplicationId {
    #[default]
    Undecided,
    Key(String),
}

impl Display for ReplicationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            | ReplicationId::Undecided => write!(f, "?"),
            | ReplicationId::Key(key) => write!(f, "{key}"),
        }
    }
}

impl From<ReplicationId> for String {
    fn from(value: ReplicationId) -> Self {
        match value {
            | ReplicationId::Undecided => "?".to_string(),
            | ReplicationId::Key(key) => key,
        }
    }
}

impl From<String> for ReplicationId {
    fn from(value: String) -> Self {
        match value.as_str() {
            | "?" => ReplicationId::Undecided,
            | _ => ReplicationId::Key(value),
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, bincode::Encode, bincode::Decode, Default, PartialOrd, Ord,
)]
pub enum ReplicationRole {
    Leader,
    #[default]
    Follower,
}

impl Display for ReplicationRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            | ReplicationRole::Leader => write!(f, "leader"),
            | ReplicationRole::Follower => write!(f, "follower"),
        }
    }
}
impl From<String> for ReplicationRole {
    fn from(value: String) -> Self {
        match value.to_lowercase().as_str() {
            | "leader" => ReplicationRole::Leader,
            | _ => ReplicationRole::Follower,
        }
    }
}

#[derive(Debug)]
pub struct ReplicationInfo {
    pub(crate) replid: ReplicationId,
    pub(crate) last_log_idx: u64,
    pub(crate) role: ReplicationRole,
    pub(crate) self_host: String,
    pub(crate) self_port: u16,
    pub(crate) term: u64,
}
impl ReplicationInfo {
    pub(crate) fn self_identifier(&self) -> PeerIdentifier {
        PeerIdentifier::new(&self.self_host, self.self_port)
    }

    pub(crate) fn vectorize(self) -> Vec<String> {
        vec![
            format!("role:{}", self.role),
            format!("leader_repl_id:{}", self.replid),
            format!("last_log_index:{}", self.last_log_idx),
            format!("self_identifier:{}", self.self_identifier()),
        ]
    }
}

#[test]
fn test_cloning_replication_state() {
    use crate::adapters::op_logs::memory_based::MemoryOpLogs;

    //GIVEN
    let replication_state = ReplicationState::new(
        ReplicationId::Key("dsd".into()),
        ReplicationRole::Leader,
        "ads",
        1231,
        ReplicatedLogs::new(MemoryOpLogs { writer: vec![] }, 0, 0),
    );
    let cloned = replication_state.logger.con_idx.clone();

    //WHEN
    replication_state.logger.con_idx.store(5, Ordering::Release);

    //THEN
    assert_eq!(cloned.load(Ordering::Relaxed), 5);
}
