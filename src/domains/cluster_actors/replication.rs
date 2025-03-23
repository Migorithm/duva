use super::election_state::ElectionState;
pub(crate) use super::heartbeats::heartbeat::BannedPeer;
pub(crate) use super::heartbeats::heartbeat::HeartBeatMessage;
use std::fmt::Display;
use std::sync::Arc;

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use crate::domains::peers::identifier::PeerIdentifier;

#[derive(Debug, Clone)]
pub struct ReplicationState {
    pub(crate) replid: ReplicationId, // The replication ID of the master example: 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb
    pub(crate) hwm: Arc<AtomicU64>,   // high water mark (commit idx)
    role: String,

    pub(crate) self_host: String,
    pub(crate) self_port: u16,
    //If the instance is a replica, these additional fields are provided:
    pub(crate) leader_host: Option<String>,
    pub(crate) leader_port: Option<u16>,

    // * state is shared among peers
    pub(crate) term: u64,
    pub(crate) ban_list: Vec<BannedPeer>,

    pub(crate) election_state: ElectionState,
    pub(crate) is_leader_mode: bool,
}

impl ReplicationState {
    pub(crate) fn new(
        replicaof: Option<(String, String)>,
        self_host: &str,
        self_port: u16,
    ) -> Self {
        let replid = if replicaof.is_none() {
            ReplicationId::Key(uuid::Uuid::now_v7().to_string())
        } else {
            ReplicationId::Undecided
        };

        let role = if replicaof.is_some() { "follower".to_string() } else { "leader".to_string() };
        let replication = ReplicationState {
            is_leader_mode: replicaof.is_none(),
            election_state: ElectionState::new(&role),
            role,
            replid,
            hwm: Arc::new(0.into()),
            leader_host: replicaof.as_ref().cloned().map(|(host, _)| host),
            leader_port: replicaof
                .map(|(_, port)| port.parse().expect("Invalid port number given")),
            term: 0,
            self_host: self_host.to_string(),
            self_port,
            ban_list: Default::default(),
        };

        replication
    }

    pub(crate) fn self_info(&self) -> String {
        let self_id = self.self_identifier();

        //TODO last 0 denotes slots - subject to work
        format!("{} myself,{} 0", self_id, self.replid)
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

    pub(crate) fn leader_bind_addr(&self) -> Option<PeerIdentifier> {
        Some(format!("{}:{}", self.leader_host.as_ref()?, self.leader_port?).into())
    }

    pub(crate) fn in_ban_list(&self, peer_identifier: &PeerIdentifier) -> bool {
        if let Ok(current_time_in_sec) = time_in_secs() {
            self.ban_list.iter().any(|node| {
                &node.p_id == peer_identifier && current_time_in_sec - node.ban_time < 60
            })
        } else {
            false
        }
    }

    pub(crate) fn default_heartbeat(
        &self,
        hop_count: u8,
        prev_log_index: u64,
        prev_log_term: u64,
    ) -> HeartBeatMessage {
        HeartBeatMessage {
            from: self.self_identifier(),
            term: self.term,
            hwm: self.hwm.load(Ordering::Relaxed),
            replid: self.replid.clone(),
            hop_count,
            ban_list: self.ban_list.clone(),
            append_entries: vec![],
            cluster_nodes: vec![],
            prev_log_index,
            prev_log_term,
        }
    }

    pub(crate) fn ban_peer(&mut self, p_id: &PeerIdentifier) -> anyhow::Result<()> {
        self.ban_list.push(BannedPeer { p_id: p_id.clone(), ban_time: time_in_secs()? });
        Ok(())
    }

    pub(crate) fn remove_from_ban_list(&mut self, peer_addr: &PeerIdentifier) {
        let idx = self.ban_list.iter().position(|node| &node.p_id == peer_addr);
        if let Some(idx) = idx {
            self.ban_list.swap_remove(idx);
        }
    }

    pub(crate) fn become_candidate(&mut self, replica_count: usize) {
        self.term += 1;

        self.election_state.become_candidate(replica_count);
    }
    pub(crate) fn may_become_follower(
        &mut self,
        candidate_id: &PeerIdentifier,
        election_term: u64,
    ) -> bool {
        if !(self.election_state.is_votable(candidate_id) && self.term < election_term) {
            return false;
        }
        self.become_follower(Some(candidate_id.clone()));
        self.term = election_term;
        true
    }

    pub(crate) fn should_become_leader(&mut self, granted: bool) -> bool {
        match self.election_state.should_become_leader(granted) {
            Some(true) => {
                eprintln!("\x1b[32m[INFO] Election succeeded\x1b[0m");
                self.become_leader();

                true
            },
            Some(false) => {
                println!("[INFO] Election failed");
                self.become_follower(None);
                true
            },
            None => false,
        }
    }
    fn become_follower(&mut self, leader_id: Option<PeerIdentifier>) {
        self.election_state.become_follower(leader_id);
        self.is_leader_mode = false;
    }
    fn become_leader(&mut self) {
        self.role = "leader".to_string();
        self.leader_host = None;
        self.leader_port = None;
        self.is_leader_mode = true;
        self.election_state.become_leader();
    }
}

pub(crate) fn time_in_secs() -> anyhow::Result<u64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs())
}

#[derive(
    Debug, Clone, PartialEq, Default, Eq, PartialOrd, Ord, bincode::Encode, bincode::Decode,
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
            ReplicationId::Key(key) => write!(f, "{}", key),
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

#[test]
fn test_cloning_replication_state() {
    //GIVEN
    let replication_state = ReplicationState::new(None, "ads", 1231);
    let cloned = replication_state.clone();

    //WHEN
    replication_state.hwm.store(5, Ordering::Release);

    //THEN
    assert_eq!(cloned.hwm.load(Ordering::Relaxed), 5);
}
