use uuid::Uuid;

use super::consensus::ElectionState;
pub(crate) use super::heartbeats::heartbeat::BannedPeer;
pub(crate) use super::heartbeats::heartbeat::HeartBeatMessage;
use crate::domains::peers::identifier::PeerIdentifier;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

#[derive(Debug, Clone)]
pub(crate) struct ReplicationState {
    pub(crate) replid: ReplicationId, // The replication ID of the master example: 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb
    pub(crate) hwm: Arc<AtomicU64>,   // high water mark (commit idx)
    pub(crate) role: Role,

    pub(crate) self_host: String,
    pub(crate) self_port: u16,

    // * state is shared among peers
    pub(crate) term: u64,
    pub(crate) ban_list: Vec<BannedPeer>,

    pub(crate) election_state: ElectionState,
    pub(crate) is_leader_mode: bool,
}

impl ReplicationState {
    pub(crate) fn new(replicaof: Option<PeerIdentifier>, self_host: &str, self_port: u16) -> Self {
        let replid = if replicaof.is_none() {
            ReplicationId::Key(uuid::Uuid::now_v7().to_string())
        } else {
            ReplicationId::Undecided
        };

        let role = if replicaof.is_some() { Role::Follower } else { Role::Leader };
        let replication = ReplicationState {
            is_leader_mode: replicaof.is_none(),
            election_state: ElectionState::new(&role),
            role,
            replid,
            hwm: Arc::new(0.into()),
            term: 0,
            self_host: self_host.to_string(),
            self_port,
            ban_list: Default::default(),
        };

        replication
    }

    pub(crate) fn self_info(&self) -> String {
        let self_id = self.self_identifier();
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

    pub(crate) fn become_candidate(&mut self, replica_count: u8) {
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

    pub(super) fn become_follower(&mut self, leader_id: Option<PeerIdentifier>) {
        self.election_state = ElectionState::Follower { voted_for: leader_id };
        self.is_leader_mode = false;
        self.role = Role::Follower;
    }
    pub(super) fn become_leader(&mut self) {
        self.role = Role::Leader;
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
pub(crate) enum ReplicationId {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    Leader,
    Follower,
}

impl Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Leader => write!(f, "leader"),
            Role::Follower => write!(f, "follower"),
        }
    }
}
#[derive(Debug, Clone)]
pub struct NodeInfo {
    bind_addr: PeerIdentifier,
    repl_id: Uuid,
    is_myself: bool,
    priority: u8, // lower value = higher priority
}

impl NodeInfo {
    /// 127.0.0.1:6000 0196477d-f227-72f2-81eb-6a3703076de8 0
    /// 127.0.0.1:6001 0196477d-f227-72f2-81eb-6a3703076de8 0
    /// 127.0.0.1:6002 myself,0196477d-f227-72f2-81eb-6a3703076de8 0
    fn parse_node_info(line: &str, myself_id: Uuid) -> Option<NodeInfo> {
        let parts: Vec<&str> = line.trim().split_whitespace().collect();
        if parts.len() != 3 {
            return None;
        }

        let address = parts[0].to_string().into();
        let raw_id = parts[1];
        let id_parts: Vec<&str> = raw_id.split(',').collect();

        let (is_myself, id) = if id_parts.len() == 2 {
            (id_parts[0] == "myself", Uuid::from_str(id_parts[1]).unwrap())
        } else {
            (false, Uuid::from_str(id_parts[0]).unwrap())
        };

        let priority = if id == myself_id { 0 } else { 1 };

        Some(NodeInfo { bind_addr: address, repl_id: id, is_myself, priority })
    }
    pub(crate) fn read_and_prioritize_nodes(path: &str) -> Vec<NodeInfo> {
        let contents = std::fs::read_to_string(path).unwrap_or_default();

        let lines: Vec<&str> = contents.lines().filter(|line| !line.trim().is_empty()).collect();

        // Find the line with "myself" to get the ID
        let my_repl_id = lines.iter().find_map(|line| {
            let parts: Vec<&str> = line.trim().split_whitespace().collect();
            if parts.len() != 3 {
                return None;
            }

            let raw_id = parts[1];
            let id_parts: Vec<&str> = raw_id.split(',').collect();

            if id_parts.len() == 2 && id_parts[0] == "myself" {
                Some(id_parts[1].to_string())
            } else {
                None
            }
        });

        let my_repl_id = match my_repl_id {
            Some(id) => Uuid::from_str(&id).unwrap(),
            None => return vec![], // No myself ID, no valid peers
        };

        let mut nodes: Vec<NodeInfo> = lines
            .into_iter()
            .filter_map(|line| NodeInfo::parse_node_info(line, my_repl_id))
            .filter(|node| !node.is_myself) // 🧼 Exclude self
            .collect();

        nodes.sort_by_key(|n| n.priority);
        nodes
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

#[test]
fn test_prioritize_nodes_with_myself() {
    use std::io::Write;

    let file_content = r#"
    127.0.0.1:6000 0196477d-f227-72f2-81eb-6a3703076de8 0
    127.0.0.1:6001 0196477d-f227-72f2-81eb-6a3703076de8 0
    127.0.0.1:6002 myself,0196477d-f227-72f2-81eb-6a3703076de8 0
    127.0.0.1:6003 99999999-aaaa-bbbb-cccc-111111111111 0
    127.0.0.1:6004 deadbeef-dead-beef-dead-beefdeadbeef 0
    "#;

    // Create temp file and write content
    let mut temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
    write!(temp_file, "{}", file_content).expect("Failed to write to temp file");

    // Read and prioritize nodes
    let nodes = NodeInfo::read_and_prioritize_nodes(temp_file.path().to_str().unwrap());

    // There should be 3 nodes, all with priority 0 (same ID as myself)
    assert_eq!(nodes.len(), 4);
    assert_eq!(nodes.iter().filter(|n| n.priority == 0).count(), 2);
    assert_eq!(nodes.iter().filter(|n| n.priority == 1).count(), 2);

    // Optionally print for debugging
    for node in nodes {
        println!("{:?}", node);
    }
}
