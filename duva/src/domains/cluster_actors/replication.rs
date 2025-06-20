use super::consensus::election::ElectionState;
use crate::domains::peers::command::BannedPeer;
use crate::domains::peers::command::HeartBeat;
use crate::domains::peers::identifier::PeerIdentifier;

use crate::domains::peers::peer::PeerState;
use std::collections::HashSet;
use std::fmt::Display;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

#[derive(Debug, Clone)]
pub(crate) struct ReplicationState {
    pub(crate) replid: ReplicationId, // The replication ID of the master example: 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb
    pub(crate) hwm: Arc<AtomicU64>,   // high water mark (commit idx)
    pub(crate) role: ReplicationRole,

    pub(crate) self_host: String,
    pub(crate) self_port: u16,

    // * state is shared among peers
    pub(crate) term: u64,
    pub(crate) banlist: HashSet<BannedPeer>,

    pub(crate) election_state: ElectionState,
}

impl ReplicationState {
    pub(crate) fn new(
        replid: ReplicationId,
        role: ReplicationRole,
        self_host: &str,
        self_port: u16,
        hwm: u64,
    ) -> Self {
        ReplicationState {
            election_state: ElectionState::new(&role),
            role,
            replid,
            hwm: Arc::new(hwm.into()),
            term: 0,
            self_host: self_host.to_string(),
            self_port,
            banlist: Default::default(),
        }
    }

    pub(super) fn self_info(&self) -> PeerState {
        let self_id = self.self_identifier();

        PeerState::new(
            &self_id,
            self.hwm.load(Ordering::Relaxed),
            self.replid.clone(),
            self.role.clone(),
        )
    }

    pub(crate) fn self_identifier(&self) -> PeerIdentifier {
        PeerIdentifier::new(&self.self_host, self.self_port)
    }

    pub(crate) fn vectorize(self) -> Vec<String> {
        vec![
            format!("role:{}", self.role),
            format!("leader_repl_id:{}", self.replid),
            format!("high_watermark:{}", self.hwm.load(Ordering::Relaxed)),
            format!("self_identifier:{}", self.self_identifier()),
        ]
    }

    pub(super) fn in_ban_list(&self, peer_identifier: &PeerIdentifier) -> bool {
        let Ok(current_time) = time_in_secs() else { return false };
        self.banlist.get(peer_identifier).is_some_and(|node| current_time - node.ban_time < 60)
    }

    pub(super) fn default_heartbeat(
        &self,
        hop_count: u8,
        prev_log_index: u64,
        prev_log_term: u64,
    ) -> HeartBeat {
        HeartBeat {
            from: self.self_identifier(),
            term: self.term,
            hwm: self.hwm.load(Ordering::Relaxed),
            replid: self.replid.clone(),
            hop_count,
            ban_list: self.banlist.iter().cloned().collect(),
            append_entries: vec![],
            cluster_nodes: vec![],
            prev_log_index,
            prev_log_term,
            hashring: None,
        }
    }

    pub(super) fn become_follower_if_term_higher_and_votable(
        &mut self,
        candidate_id: &PeerIdentifier,
        election_term: u64,
    ) -> bool {
        // If the candidate's term is less than mine → reject
        if election_term < self.term {
            return false;
        }

        // When a node sees a higher term, it must forget any vote it cast in a prior term, because:
        if election_term > self.term {
            self.term = election_term;
            self.vote_for(None);
        }

        if !self.election_state.is_votable(candidate_id) {
            return false;
        }

        self.vote_for(Some(candidate_id.clone()));

        true
    }

    pub(super) fn vote_for(&mut self, leader_id: Option<PeerIdentifier>) {
        self.election_state = ElectionState::Follower { voted_for: leader_id };
        self.set_follower_mode();
    }
    pub(super) fn become_leader(&mut self) {
        self.role = ReplicationRole::Leader;
        self.election_state.become_leader();
    }

    fn set_follower_mode(&mut self) {
        self.role = ReplicationRole::Follower;
    }

    pub(crate) fn is_leader(&self) -> bool {
        self.role == ReplicationRole::Leader
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
pub(crate) enum ReplicationId {
    #[default]
    Undecided,
    Key(String),
}

impl Display for ReplicationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            | ReplicationId::Undecided => write!(f, "?"),
            | ReplicationId::Key(key) => write!(f, "{}", key),
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

#[derive(Debug, Clone, PartialEq, Eq, bincode::Encode, bincode::Decode, Default)]
pub enum ReplicationRole {
    #[default]
    Follower,
    Leader,
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
impl From<ReplicationRole> for String {
    fn from(value: ReplicationRole) -> Self {
        match value {
            | ReplicationRole::Leader => "leader".to_string(),
            | ReplicationRole::Follower => "follower".to_string(),
        }
    }
}

#[test]
fn test_cloning_replication_state() {
    //GIVEN
    let replication_state = ReplicationState::new(
        ReplicationId::Key("dsd".into()),
        ReplicationRole::Leader,
        "ads",
        1231,
        0,
    );
    let cloned = replication_state.clone();

    //WHEN
    replication_state.hwm.store(5, Ordering::Release);

    //THEN
    assert_eq!(cloned.hwm.load(Ordering::Relaxed), 5);
}
