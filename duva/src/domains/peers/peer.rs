use crate::domains::IoError;
use crate::domains::cluster_actors::replication::ReplicationId;
use crate::domains::peers::connected_types::WriteConnected;
use crate::domains::query_parsers::QueryIO;
use crate::prelude::PeerIdentifier;
use crate::services::interface::TWrite;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::identifier::TPeerAddress;

#[derive(Debug)]
pub(crate) struct Peer {
    pub(crate) w_conn: WriteConnected,
    pub(crate) listener_kill_trigger: ListeningActorKillTrigger,
    pub(crate) last_seen: Instant,
    state: PeerState,
}

impl Peer {
    pub(crate) fn new(
        w: OwnedWriteHalf,
        state: PeerState,
        listener_kill_trigger: ListeningActorKillTrigger,
    ) -> Self {
        Self {
            w_conn: WriteConnected::new(w),
            listener_kill_trigger,
            last_seen: Instant::now(),
            state,
        }
    }
    pub(crate) fn state(&self) -> &PeerState {
        &self.state
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

    pub(crate) async fn send_to_peer(
        &mut self,
        io: impl Into<QueryIO> + Send,
    ) -> Result<(), IoError> {
        self.w_conn.stream.write_io(io).await
    }

    pub(crate) async fn kill(self) -> OwnedReadHalf {
        self.listener_kill_trigger.kill().await
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

    pub(crate) fn parse_node_info(line: &str, self_repl_id: &str) -> Option<PeerState> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() != 3 {
            return None;
        }

        let id_parts: Vec<_> = parts[1].split(',').collect();
        let (is_myself, repl_id) = match id_parts.as_slice() {
            ["myself", id] => (true, id.to_string()),
            [id] => (false, id.to_string()),
            _ => return None,
        };

        let priority = if repl_id == self_repl_id { NodeKind::Replica } else { NodeKind::NonData };

        Some(Self {
            addr: parts[0].bind_addr().into(),
            replid: repl_id.into(),
            kind: if is_myself { NodeKind::Myself } else { priority },
            match_index: 0, // TODO perhaps we should set this to the last known match index
        })
    }

    pub(crate) fn from_file(path: &str) -> Vec<Self> {
        let Ok(metadata) = std::fs::metadata(path) else {
            return vec![];
        };
        let Ok(modified) = metadata.modified() else {
            return vec![];
        };
        if modified.elapsed().unwrap_or_default().as_secs() > 300 {
            // File is too old, ignoring
            return vec![];
        }

        let contents = std::fs::read_to_string(path).unwrap_or_default();

        let lines: Vec<&str> = contents.lines().filter(|line| !line.trim().is_empty()).collect();

        // Find the line with "myself" to get the ID
        let my_repl_id = lines.iter().find_map(|line| {
            let parts: Vec<&str> = line.split_whitespace().collect();
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
            Some(id) => id,
            None => return vec![], // No myself ID, no valid peers
        };

        let mut nodes: Vec<Self> = lines
            .into_iter()
            .filter_map(|line| Self::parse_node_info(line, &my_repl_id))
            .filter(|node| node.kind != NodeKind::Myself) // 🧼 Exclude self
            .collect();

        nodes.sort_by_key(|n| match n.kind {
            NodeKind::Replica => 0,
            NodeKind::NonData => 1,
            NodeKind::Myself => 2,
        });
        nodes
    }
}

impl std::fmt::Display for PeerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            NodeKind::Replica => write!(f, "{} {} 0", self.addr, self.replid),
            NodeKind::NonData => write!(f, "{} {} 0", self.addr, self.replid),
            NodeKind::Myself => write!(f, "{} myself,{} 0", self.addr, self.replid),
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
    JoinHandle<OwnedReadHalf>,
);
impl ListeningActorKillTrigger {
    pub(crate) fn new(
        kill_trigger: tokio::sync::oneshot::Sender<()>,
        listning_task: JoinHandle<OwnedReadHalf>,
    ) -> Self {
        Self(kill_trigger, listning_task)
    }
    pub(crate) async fn kill(self) -> OwnedReadHalf {
        let _ = self.0.send(());
        self.1.await.unwrap()
    }
}

#[test]
fn test_prioritize_nodes_with_same_replid() {
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
    let nodes = PeerState::from_file(temp_file.path().to_str().unwrap());

    // There should be 3 nodes, all with priority 0 (same ID as myself)
    assert_eq!(nodes.len(), 4);
    assert_eq!(nodes.iter().filter(|n| n.kind == NodeKind::NonData).count(), 2);
    assert_eq!(nodes.iter().filter(|n| n.kind == NodeKind::Replica).count(), 2);

    // Optionally print for debugging
    for node in nodes {
        println!("{:?}", node);
    }
}
