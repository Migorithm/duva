use super::*;
use crate::domains::cluster_actors::SessionRequest;
use crate::domains::peers::command::HeartBeat;
use crate::domains::peers::command::RejectionReason;
use crate::domains::peers::command::ReplicationAck;
use crate::domains::peers::command::RequestVote;
use crate::domains::peers::identifier::PeerIdentifier;
use crate::err;
use std::fmt::Display;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub(crate) struct Replication<T> {
    pub(crate) self_port: u16,
    election_votes: ElectionVotes,
    target: T,
    state: ReplicationState,
    last_log_term: u64,
    con_idx: Arc<AtomicU64>,
    last_applied: u64,
}

impl<T: TWriteAheadLog> Replication<T> {
    pub(crate) fn new(self_port: u16, target: T, state: ReplicationState) -> Self {
        Self {
            election_votes: ElectionVotes::default(),
            self_port,
            target,
            last_log_term: state.term,
            con_idx: Arc::new(state.last_log_index.into()),
            state,
            last_applied: 0,
        }
    }

    pub(crate) fn reset_election_votes(&mut self) {
        self.election_votes = ElectionVotes::default();
        self.set_role(ReplicationRole::Follower);
    }

    pub(crate) fn last_applied(&self) -> u64 {
        self.last_applied
    }
    pub(crate) fn last_applied_mut(&mut self) -> &mut u64 {
        &mut self.last_applied
    }

    pub(crate) fn clone_state(&self) -> ReplicationState {
        self.state.clone()
    }
    pub(crate) fn log_state(&self) -> &ReplicationState {
        &self.state
    }
    pub(crate) fn is_empty_log(&self) -> bool {
        self.target.is_empty()
    }

    pub(crate) fn self_identifier(&self) -> PeerIdentifier {
        self.state.node_id.clone()
    }

    pub(crate) fn replid(&mut self) -> &ReplicationId {
        &self.state.replid
    }
    pub(crate) fn set_replid(&mut self, replid: ReplicationId) {
        self.state.replid = replid;
    }

    pub(crate) fn set_term(&mut self, term: u64) {
        self.state.term = term;
    }
    pub(crate) fn set_role(&mut self, new_role: ReplicationRole) {
        self.state.role = new_role;
    }

    pub(crate) fn default_heartbeat(&self, hop_count: u8) -> HeartBeat {
        let state = self.log_state();

        HeartBeat {
            from: state.node_id.clone(),
            term: state.term,
            leader_commit_idx: self
                .is_leader()
                .then_some(self.con_idx.load(Ordering::Relaxed)),
            replid: state.replid.clone(),
            hop_count,
            banlist: Vec::new(),
            prev_log_index: state.last_log_index,
            prev_log_term: self.last_log_term,
            ..Default::default()
        }
    }

    pub(crate) fn revert_voting(&mut self, term: u64, candidate_id: &PeerIdentifier) {
        self.election_votes.votes.remove(candidate_id);
        self.set_term(term);
    }

    pub(crate) fn grant_vote(&mut self, request_vote: &RequestVote) -> bool {
        // Check if log is up-to-date and if not already voted in this term or voted for this candidate
        if self.is_log_up_to_date(request_vote.last_log_index, request_vote.last_log_term)
            && self.is_votable(&request_vote.candidate_id)
        {
            self.vote_for(request_vote.candidate_id.clone());
            return true;
        }

        false
    }

    fn vote_for(&mut self, candidate_id: PeerIdentifier) {
        self.election_votes.votes.insert(candidate_id);
    }

    pub(crate) fn is_leader(&self) -> bool {
        self.state.role == ReplicationRole::Leader
    }

    pub(crate) fn is_candidate(&self) -> bool {
        self.election_votes.votes.contains(&self.state.node_id)
    }

    pub(crate) fn is_log_up_to_date(
        &self,
        candidate_last_log_index: u64,
        candidate_last_log_term: u64,
    ) -> bool {
        if candidate_last_log_term > self.last_log_term {
            return true;
        }

        candidate_last_log_term == self.last_log_term
            && candidate_last_log_index >= self.state.last_log_index
    }

    pub(crate) fn request_vote(&self) -> RequestVote {
        RequestVote {
            term: self.state.term,
            candidate_id: self.state.node_id.clone(),
            last_log_index: self.state.last_log_index,
            last_log_term: self.last_log_term,
        }
    }

    pub(crate) fn is_votable(&self, candidate_id: &PeerIdentifier) -> bool {
        self.election_votes.is_votable(candidate_id)
    }

    pub(crate) fn record_vote(&mut self, voter_id: PeerIdentifier) -> bool {
        self.election_votes.record_vote(voter_id)
    }

    pub(crate) fn has_majority_vote(&self) -> bool {
        self.election_votes.has_majority()
    }
    pub(crate) fn clear_votes(&mut self) {
        self.election_votes.votes.clear();
    }
    pub(crate) fn initiate_vote(&mut self, replica_count: usize) {
        self.election_votes =
            ElectionVotes::new(replica_count as u8, self.state.node_id.clone());
    }

    pub(crate) fn write_single_entry(
        &mut self,
        entry: LogEntry,
        current_term: u64,
        session_req: Option<SessionRequest>,
    ) -> anyhow::Result<()> {
        let op = WriteOperation {
            entry,
            log_index: (self.state.last_log_index + 1),
            term: current_term,
            session_req,
        };

        self.write_many(vec![op])?;
        self.last_log_term = current_term;
        Ok(())
    }

    pub(crate) fn write_many(&mut self, entries: Vec<WriteOperation>) -> anyhow::Result<u64> {
        if entries.is_empty() {
            return Ok(self.state.last_log_index);
        }

        self.update_metadata(&entries);
        self.target.write_many(entries)?;
        Ok(self.state.last_log_index)
    }

    pub(crate) fn read_at(&mut self, at: u64) -> Option<WriteOperation> {
        self.target.read_at(at)
    }

    pub(crate) fn increase_con_idx_by(&self, by: u64) {
        self.con_idx.fetch_add(by, Ordering::Relaxed);
    }
    pub(crate) fn curr_con_idx(&self) -> u64 {
        self.con_idx.load(Ordering::Relaxed)
    }
    pub(crate) fn clone_con_idx(&self) -> Arc<AtomicU64> {
        self.con_idx.clone()
    }

    pub(crate) fn reset_log(&mut self) {
        self.con_idx.store(0, Ordering::Release);
        self.state.last_log_index = 0;
        self.last_log_term = 0;
        self.target.truncate_after(0);
        self.set_replid(ReplicationId::Undecided)
    }

    pub(crate) fn list_append_log_entries(
        &self,
        low_watermark: Option<u64>,
    ) -> Vec<WriteOperation> {
        let start_exclusive = low_watermark.unwrap_or(self.state.last_log_index);
        self.target.range(start_exclusive, self.state.last_log_index)
    }
    pub(crate) fn truncate_after(&mut self, log_index: u64) {
        self.target.truncate_after(log_index);
    }

    pub(crate) fn range(&self, start_exclusive: u64, end_inclusive: u64) -> Vec<WriteOperation> {
        self.target.range(start_exclusive, end_inclusive)
    }

    pub(crate) fn replicate_log_entries(
        &mut self,
        operations: Vec<WriteOperation>,
        prev_log_index: u64,
        prev_log_term: u64,
        session_reqs: &mut Vec<SessionRequest>,
    ) -> Result<ReplicationAck, RejectionReason> {
        let mut entries = Vec::with_capacity(operations.len());

        let last_log_index = self.state.last_log_index;
        for mut log in operations {
            if log.log_index > last_log_index {
                if let Some(session_req) = log.session_req.take() {
                    session_reqs.push(session_req);
                }
                entries.push(log);
            }
        }

        // ! Ensure Previous Log consistency
        // Case: Empty log
        if self.is_empty_log() && prev_log_index != 0 {
            err!("Log is empty but leader expects an entry");
            return Err(RejectionReason::LogInconsistency); // Log empty but leader expects an entry
        }

        // * Raft followers should truncate their log starting at prev_log_index + 1 and then append the new entries
        // * Just returning an error is breaking consistency
        if let Some(prev_entry) = self.read_at(prev_log_index)
            && prev_entry.term != prev_log_term
        {
            // ! Term mismatch -> triggers log truncation
            err!("Term mismatch: {} != {}", prev_entry.term, prev_log_term);
            self.truncate_after(prev_log_index);
        }

        let log_idx = self.write_many(entries).map_err(|e| {
            err!("{}", e);
            RejectionReason::FailToWrite
        })?;

        Ok(ReplicationAck::ack(log_idx, self.state.term))
    }

    fn update_metadata(&mut self, new_entries: &[WriteOperation]) {
        if let Some(last_entry) = new_entries.last() {
            self.state.last_log_index = last_entry.log_index;
            self.last_log_term = last_entry.term;
        }
    }

    #[cfg(test)]
    pub fn election_votes(&self) -> ElectionVotes {
        self.election_votes.clone()
    }

    #[cfg(test)]
    pub fn set_target(&mut self, target: T) {
        self.target = target;
    }

    #[cfg(test)]
    pub fn set_state(&mut self, state: ReplicationState) {
        self.state = state;
    }

    #[cfg(test)]
    pub(crate) fn last_log_term(&self) -> u64 {
        self.last_log_term
    }

    pub(crate) fn on_election_timeout(&mut self, term: u64) {
        self.set_role(ReplicationRole::Follower);
        if term > self.state.term {
            self.set_term(term);
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
            ReplicationId::Undecided => write!(f, "?"),
            ReplicationId::Key(key) => write!(f, "{key}"),
        }
    }
}

impl From<ReplicationId> for String {
    fn from(value: ReplicationId) -> Self {
        match value {
            ReplicationId::Undecided => "?".to_string(),
            ReplicationId::Key(key) => key,
        }
    }
}

impl From<String> for ReplicationId {
    fn from(value: String) -> Self {
        match value.as_str() {
            "?" => ReplicationId::Undecided,
            _ => ReplicationId::Key(value),
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
            ReplicationRole::Leader => write!(f, "leader"),
            ReplicationRole::Follower => write!(f, "follower"),
        }
    }
}
impl From<String> for ReplicationRole {
    fn from(value: String) -> Self {
        match value.to_lowercase().as_str() {
            "leader" => ReplicationRole::Leader,
            _ => ReplicationRole::Follower,
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
    let target = MemoryOpLogs { writer: vec![] };
    let replication_state = Replication::new(1231, target, state);
    let cloned = replication_state.con_idx.clone();

    //WHEN
    replication_state.con_idx.store(5, Ordering::Release);

    //THEN
    assert_eq!(cloned.load(Ordering::Relaxed), 5);
}
