use super::connections::connection_types::WriteConnected;
use super::identifier::TPeerAddress;
use crate::domains::QueryIO;
use crate::domains::cluster_actors::replication::{ReplicationId, ReplicationRole};
use crate::domains::{IoError, TRead};
use crate::prelude::PeerIdentifier;
use tokio::task::JoinHandle;
use tokio::time::Instant;

#[derive(Debug)]
pub(crate) struct Peer {
    pub(crate) w_conn: WriteConnected,
    pub(crate) listener_kill_trigger: ListeningActorKillTrigger,
    pub(crate) last_seen: Instant,
    state: PeerState,
}

impl Peer {
    pub(crate) fn new(
        w: impl Into<WriteConnected>,
        state: PeerState,
        listener_kill_trigger: ListeningActorKillTrigger,
    ) -> Self {
        Self { w_conn: w.into(), listener_kill_trigger, last_seen: Instant::now(), state }
    }
    pub(crate) fn id(&self) -> &PeerIdentifier {
        &self.state.id
    }
    pub(crate) fn state(&self) -> &PeerState {
        &self.state
    }
    pub(crate) fn replid(&self) -> &ReplicationId {
        &self.state.replid
    }
    pub(crate) fn match_index(&self) -> u64 {
        self.state.match_index
    }
    pub(crate) fn set_match_index(&mut self, match_index: u64) {
        self.state.match_index = match_index;
    }

    pub(crate) async fn send(&mut self, io: impl Into<QueryIO> + Send) -> Result<(), IoError> {
        self.w_conn.write(io.into()).await
    }

    pub(crate) async fn kill(self) -> Box<dyn TRead> {
        self.listener_kill_trigger.kill().await
    }

    pub(crate) fn is_replica(&self, replid: &ReplicationId) -> bool {
        self.state.replid == *replid
    }

    pub(crate) fn is_follower(&self, replid: &ReplicationId) -> bool {
        self.is_replica(replid) && self.state.role == ReplicationRole::Follower
    }

    pub(crate) fn set_role(&mut self, role: ReplicationRole) {
        self.state.role = role;
    }

    pub(crate) fn role(&self) -> ReplicationRole {
        self.state.role.clone()
    }
}

#[derive(Clone, Debug, PartialEq, bincode::Encode, bincode::Decode)]
pub struct PeerState {
    id: PeerIdentifier,
    pub(crate) match_index: u64,
    pub(crate) replid: ReplicationId,
    pub(crate) role: ReplicationRole,
}

impl PeerState {
    pub(crate) fn new(
        id: &str,
        match_index: u64,
        replid: ReplicationId,
        role: ReplicationRole,
    ) -> Self {
        Self { id: id.bind_addr().into(), match_index, replid, role }
    }

    pub(crate) fn id(&self) -> &PeerIdentifier {
        &self.id
    }

    pub(crate) fn parse_node_info(line: &str) -> Option<Self> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() != 5 {
            return None;
        }

        let [addr, id_part, _, match_index, role] = parts[..] else {
            return None;
        };

        let repl_id = Self::extract_replid(id_part)?;

        let match_index = match_index.parse().unwrap_or_default();

        Some(Self {
            id: addr.bind_addr().into(),
            replid: repl_id.into(),
            match_index,
            role: role.to_string().into(),
        })
    }

    fn extract_replid(id_part: &str) -> Option<String> {
        if id_part.contains("myself,") {
            Some(id_part[7..].to_string())
        } else {
            Some(id_part.to_string())
        }
    }

    pub(crate) fn from_file(path: &str) -> Vec<Self> {
        let Some(contents) = Self::read_file_if_valid(path) else {
            return vec![];
        };

        let Some(my_repl_id) = Self::extract_my_repl_id(&contents) else {
            return vec![];
        };

        let mut nodes: Vec<Self> = contents
            .lines()
            .filter(|line| !line.trim().is_empty())
            .filter_map(Self::parse_node_info)
            .collect();

        nodes.sort_by_key(|n| n.replid.to_string() == my_repl_id);
        nodes
    }

    fn read_file_if_valid(path: &str) -> Option<String> {
        let metadata = std::fs::metadata(path).ok()?;
        let modified = metadata.modified().ok()?;

        if modified.elapsed().unwrap_or_default().as_secs() > 300 {
            return None;
        }

        std::fs::read_to_string(path).ok()
    }

    fn extract_my_repl_id(contents: &str) -> Option<String> {
        contents.lines().filter(|line| !line.trim().is_empty()).find_map(|line| {
            let parts: Vec<&str> = line.split_whitespace().collect();
            for part in parts {
                if part.contains("myself,") {
                    return Some(part[7..].to_string());
                }
            }
            None
        })
    }

    pub(crate) fn format(&self, peer_id: &PeerIdentifier) -> String {
        if self.id == *peer_id {
            return format!(
                "{} myself,{} 0 {} {}",
                self.id, self.replid, self.match_index, self.role
            );
        }
        format!("{} {} 0 {} {}", self.id, self.replid, self.match_index, self.role)
    }

    pub(crate) fn is_self(&self, bind_addr: &str) -> bool {
        self.id.bind_addr() == bind_addr
    }
}

#[derive(Debug)]
pub(crate) struct ListeningActorKillTrigger(
    tokio::sync::oneshot::Sender<()>,
    JoinHandle<Box<dyn TRead>>,
);
impl ListeningActorKillTrigger {
    pub(crate) fn new(
        kill_trigger: tokio::sync::oneshot::Sender<()>,
        listning_task: JoinHandle<Box<dyn TRead>>,
    ) -> Self {
        Self(kill_trigger, listning_task)
    }
    pub(crate) async fn kill(self) -> Box<dyn TRead> {
        let _ = self.0.send(());
        self.1.await.unwrap()
    }
}

#[test]
fn test_prioritize_nodes_with_same_replid() {
    use std::io::Write;

    let file_content = r#"
    127.0.0.1:6000 0196477d-f227-72f2-81eb-6a3703076de8 0 11 follower
    127.0.0.1:6001 0196477d-f227-72f2-81eb-6a3703076de8 0 13 follower
    127.0.0.1:6002 myself,0196477d-f227-72f2-81eb-6a3703076de8 0 15 leader
    127.0.0.1:6003 99999999-aaaa-bbbb-cccc-111111111111 0 5 leader
    127.0.0.1:6004 deadbeef-dead-beef-dead-beefdeadbeef 0 5 leader
    "#;

    // Create temp file and write content
    let mut temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
    write!(temp_file, "{file_content}").expect("Failed to write to temp file");

    // Read and prioritize nodes
    let nodes = PeerState::from_file(temp_file.path().to_str().unwrap());

    // There should be 4 nodes, all with priority 0 (same ID as myself)
    assert_eq!(nodes.len(), 5);

    assert_eq!(nodes.iter().filter(|n| n.role == ReplicationRole::Follower).count(), 2);
    assert_eq!(nodes.iter().filter(|n| n.role == ReplicationRole::Leader).count(), 3);

    // Optionally print for debugging
    for node in nodes {
        println!("{node:?}");
    }
}
