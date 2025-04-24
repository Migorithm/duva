use crate::{domains::cluster_actors::replication::ReplicationId, prelude::PeerIdentifier};

#[derive(Debug, Clone, PartialEq, bincode::Encode, bincode::Decode)]
pub struct ClusterNode {
    pub(crate) bind_addr: PeerIdentifier,
    pub(crate) repl_id: String,
    pub(crate) is_myself: bool,
    pub(crate) priority: NodeKind, // lower value = higher priority
}

#[derive(Debug, Clone, PartialEq, bincode::Encode, bincode::Decode)]
pub(crate) enum NodeKind {
    Replica,
    NonData,
}

impl ClusterNode {
    pub(crate) fn new(
        bind_addr: &str,
        repl_id: &ReplicationId,
        is_myself: bool,
        priority: NodeKind,
    ) -> Self {
        Self {
            bind_addr: bind_addr.to_string().into(),
            repl_id: repl_id.to_string(),
            is_myself,
            priority,
        }
    }

    pub(crate) fn parse_node_info(line: &str, self_repl_id: &str) -> Option<ClusterNode> {
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

        Some(Self { bind_addr: parts[0].to_string().into(), repl_id, is_myself, priority })
    }
    pub(crate) fn from_file(path: &str) -> Vec<ClusterNode> {
        println!("Reading cluster nodes from file: {}", path);

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

        let mut nodes: Vec<ClusterNode> = lines
            .into_iter()
            .filter_map(|line| ClusterNode::parse_node_info(line, &my_repl_id))
            .filter(|node| !node.is_myself) // 🧼 Exclude self
            .collect();

        nodes.sort_by_key(|n| match n.priority {
            NodeKind::Replica => 0,
            NodeKind::NonData => 1,
        });
        nodes
    }
}

impl std::fmt::Display for ClusterNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_myself {
            write!(f, "{} myself,{} 0", self.bind_addr, self.repl_id)
        } else {
            write!(f, "{} {} 0", self.bind_addr, self.repl_id)
        }
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
    let nodes = ClusterNode::from_file(temp_file.path().to_str().unwrap());

    // There should be 3 nodes, all with priority 0 (same ID as myself)
    assert_eq!(nodes.len(), 4);
    assert_eq!(nodes.iter().filter(|n| n.priority == NodeKind::NonData).count(), 2);
    assert_eq!(nodes.iter().filter(|n| n.priority == NodeKind::Replica).count(), 2);

    // Optionally print for debugging
    for node in nodes {
        println!("{:?}", node);
    }
}
