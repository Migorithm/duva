use crate::domains::cluster_actors::consensus::election::ElectionVotes;
use crate::domains::operation_logs::interfaces::TWriteAheadLog;
use crate::domains::operation_logs::logger::ReplicatedLogs;
use crate::domains::peers::command::BannedPeer;
use crate::domains::peers::command::HeartBeat;
use crate::domains::peers::command::RequestVote;
use crate::domains::peers::identifier::PeerIdentifier;
use crate::domains::replications::state::ReplicationState;
use std::fmt::Display;
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub(crate) struct Replication<T> {
    pub(crate) self_port: u16,
    pub(crate) banlist: Vec<BannedPeer>,
    pub(crate) election_votes: ElectionVotes,
    pub(crate) logger: ReplicatedLogs<T>,
}

impl<T: TWriteAheadLog> Replication<T> {
    pub(crate) fn new(self_port: u16, logger: ReplicatedLogs<T>) -> Self {
        Self {
            election_votes: ElectionVotes::default(),
            self_port,
            banlist: Default::default(),
            logger,
        }
    }

    pub(crate) fn reset_election_votes(&mut self) {
        self.election_votes = ElectionVotes::default();
    }

    pub(crate) fn state(&self) -> ReplicationState {
        self.logger.state.clone()
    }

    pub(crate) fn self_identifier(&self) -> PeerIdentifier {
        self.logger.state.node_id.clone()
    }

    pub(crate) fn in_ban_list(&self, peer_identifier: &PeerIdentifier) -> bool {
        let Ok(current_time) = time_in_secs() else { return false };
        self.banlist.iter().any(|x| x.p_id == *peer_identifier && current_time - x.ban_time < 60)
    }
    pub(crate) fn replid(&mut self) -> &ReplicationId {
        &self.logger.state.replid
    }
    pub(crate) fn set_replid(&mut self, replid: ReplicationId) {
        self.logger.state.replid = replid;
    }

    pub(crate) fn set_term(&mut self, term: u64) {
        self.logger.state.term = term;
    }
    pub(crate) fn set_role(&mut self, new_role: ReplicationRole) {
        self.logger.state.role = new_role;
    }

    pub(crate) fn default_heartbeat(&self, hop_count: u8) -> HeartBeat {
        HeartBeat {
            from: self.self_identifier(),
            term: self.logger.state.term,
            leader_commit_idx: self
                .is_leader()
                .then_some(self.logger.con_idx.load(Ordering::Relaxed)),
            replid: self.logger.state.replid.clone(),
            hop_count,
            ban_list: self.banlist.clone(),
            prev_log_index: self.logger.state.last_log_index,
            prev_log_term: self.logger.last_log_term,
            ..Default::default()
        }
    }

    pub(crate) fn revert_voting(&mut self, term: u64, candidate_id: &PeerIdentifier) {
        self.election_votes.votes.remove(candidate_id);
        self.logger.state.term = term;
    }

    pub(crate) fn vote_for(&mut self, candidate_id: PeerIdentifier) {
        self.election_votes.votes.insert(candidate_id);
    }

    pub(crate) fn is_leader(&self) -> bool {
        self.logger.state.role == ReplicationRole::Leader
    }

    pub(crate) fn is_candidate(&self) -> bool {
        self.election_votes.votes.contains(&self.self_identifier())
    }

    pub(crate) fn is_log_up_to_date(
        &self,
        candidate_last_log_index: u64,
        candidate_last_log_term: u64,
    ) -> bool {
        if candidate_last_log_term > self.logger.last_log_term {
            return true;
        }

        candidate_last_log_term == self.logger.last_log_term
            && candidate_last_log_index >= self.logger.state.last_log_index
    }

    pub(crate) fn request_vote(&self) -> RequestVote {
        RequestVote {
            term: self.logger.state.term,
            candidate_id: self.self_identifier(),
            last_log_index: self.logger.state.last_log_index,
            last_log_term: self.logger.last_log_term,
        }
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

#[test]
fn test_cloning_replication_state() {
    use crate::adapters::op_logs::memory_based::MemoryOpLogs;

    //GIVEN
    let state = ReplicationState {
        node_id: PeerIdentifier::new("127.0.0.1", 1231),
        replid: ReplicationId::Key("dsd".into()),
        role: ReplicationRole::Leader,
        last_log_index: 0,
        term: 0,
    };
    let repllogs = ReplicatedLogs::new(MemoryOpLogs { writer: vec![] }, state);
    let replication_state = Replication::new(1231, repllogs);
    let cloned = replication_state.logger.con_idx.clone();

    //WHEN
    replication_state.logger.con_idx.store(5, Ordering::Release);

    //THEN
    assert_eq!(cloned.load(Ordering::Relaxed), 5);
}
