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
    pub(crate) role: ReplicationRole,
}

impl Peer {
    pub(crate) fn new(
        w: impl Into<WriteConnected>,
        state: PeerState,
        listener_kill_trigger: ListeningActorKillTrigger,
        role: ReplicationRole,
    ) -> Self {
        Self { w_conn: w.into(), listener_kill_trigger, last_seen: Instant::now(), state, role }
    }
    pub(crate) fn id(&self) -> &PeerIdentifier {
        &self.state.addr
    }
    pub(crate) fn state(&self) -> &PeerState {
        &self.state
    }
    pub(crate) fn replid(&self) -> &ReplicationId {
        &self.state.replid
    }

    pub(crate) fn kind(&self) -> &NodeKind {
        &self.state.kind
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

    pub(crate) fn is_replica(&self) -> bool {
        self.state.kind == NodeKind::Replica
    }
}

#[derive(Clone, Debug, PartialEq, bincode::Encode, bincode::Decode)]
pub struct PeerState {
    pub(crate) addr: PeerIdentifier,
    pub(crate) match_index: u64,
    pub(crate) replid: ReplicationId,
    pub(crate) kind: NodeKind,
}

impl PeerState {
    pub(crate) fn new(id: &str, match_index: u64, replid: ReplicationId, kind: NodeKind) -> Self {
        Self { addr: id.bind_addr().into(), match_index, replid, kind }
    }

    pub(crate) fn parse_node_info(line: &str, self_repl_id: &str) -> Option<Self> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() != 4 {
            return None;
        }

        let [addr, id_part, _, match_index] = parts[..] else {
            return None;
        };

        let (is_myself, repl_id) = Self::parse_id_part(id_part)?;
        let kind = Self::determine_node_kind(&repl_id, self_repl_id, is_myself);
        let match_index = match_index.parse().unwrap_or_default();

        Some(Self { addr: addr.bind_addr().into(), replid: repl_id.into(), kind, match_index })
    }

    fn parse_id_part(id_part: &str) -> Option<(bool, String)> {
        if id_part.contains("myself,") {
            Some((true, id_part[7..].to_string()))
        } else {
            Some((false, id_part.to_string()))
        }
    }

    fn determine_node_kind(repl_id: &str, self_repl_id: &str, is_myself: bool) -> NodeKind {
        if is_myself {
            NodeKind::Myself
        } else if repl_id == self_repl_id {
            NodeKind::Replica
        } else {
            NodeKind::NonData
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
            .filter_map(|line| Self::parse_node_info(line, &my_repl_id))
            .filter(|node| node.kind != NodeKind::Myself)
            .collect();

        nodes.sort_by_key(|n| match n.kind {
            | NodeKind::Replica => 0,
            | NodeKind::NonData => 1,
            | NodeKind::Myself => 2,
        });
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
}

impl std::fmt::Display for PeerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            | NodeKind::Replica | NodeKind::NonData => {
                write!(f, "{} {} 0 {}", self.addr, self.replid, self.match_index)
            },
            | NodeKind::Myself => {
                write!(f, "{} myself,{} 0 {}", self.addr, self.replid, self.match_index)
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, bincode::Encode, bincode::Decode)]
pub(crate) enum NodeKind {
    Replica,
    NonData,
    Myself,
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
    127.0.0.1:6000 0196477d-f227-72f2-81eb-6a3703076de8 0 11
    127.0.0.1:6001 0196477d-f227-72f2-81eb-6a3703076de8 0 13
    127.0.0.1:6002 myself,0196477d-f227-72f2-81eb-6a3703076de8 0 15
    127.0.0.1:6003 99999999-aaaa-bbbb-cccc-111111111111 0 5
    127.0.0.1:6004 deadbeef-dead-beef-dead-beefdeadbeef 0 5
    "#;

    // Create temp file and write content
    let mut temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
    write!(temp_file, "{}", file_content).expect("Failed to write to temp file");

    // Read and prioritize nodes
    let nodes = PeerState::from_file(temp_file.path().to_str().unwrap());

    // There should be 4 nodes, all with priority 0 (same ID as myself)
    assert_eq!(nodes.len(), 4);
    assert_eq!(nodes.iter().filter(|n| n.kind == NodeKind::NonData).count(), 2);
    assert_eq!(nodes.iter().filter(|n| n.kind == NodeKind::Replica).count(), 2);

    // Optionally print for debugging
    for node in nodes {
        println!("{:?}", node);
    }
}
