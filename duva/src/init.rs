use crate::{env_var, prelude::PeerIdentifier};

pub struct Environment {
    pub seed_server: Option<PeerIdentifier>,
    pub pre_connected_peers: Vec<PeerIdentifier>,
    pub dir: String,
    pub dbfilename: String,
    pub port: u16,
    pub host: String,
    pub hf_mills: u64,
    pub ttl_mills: u128,
    pub append_only: bool,
    pub topology_path: String,
}

impl Environment {
    pub fn new() -> Self {
        env_var!(
            {
                replicaof
            }
            {
                port = 6379,
                host = "127.0.0.1".to_string(),
                dir = ".".to_string(),
                dbfilename = "dump.rdb".to_string(),
                hf = 1000,
                ttl = 60000,
                append_only = false,
                tpp = "duva.tp".to_string() // topology path
            }
        );
        let replicaof = replicaof.and_then(|host_and_port| {
            host_and_port.split_once(':').map(|(host, port)| format!("{}:{}", host, port).into())
        });

        // read topology path
        let tops = read_and_prioritize_nodes(&tpp);
        // TODO sort by priority

        Self {
            seed_server: replicaof,
            dir,
            dbfilename,
            port,
            host,
            hf_mills: hf,
            ttl_mills: ttl,
            append_only,
            topology_path: dbg!(tpp),
            pre_connected_peers: vec![],
        }
    }
}

#[derive(Debug, Clone)]
struct NodeInfo {
    address: String,
    id: String,
    is_myself: bool,
    priority: u8, // lower value = higher priority
}

/// 127.0.0.1:6000 0196477d-f227-72f2-81eb-6a3703076de8 0
/// 127.0.0.1:6001 0196477d-f227-72f2-81eb-6a3703076de8 0
/// 127.0.0.1:6002 myself,0196477d-f227-72f2-81eb-6a3703076de8 0
fn parse_node_info(line: &str, myself_id: &str) -> Option<NodeInfo> {
    let parts: Vec<&str> = line.trim().split_whitespace().collect();
    if parts.len() != 3 {
        return None;
    }

    let address = parts[0].to_string();
    let raw_id = parts[1];
    let id_parts: Vec<&str> = raw_id.split(',').collect();

    let (is_myself, id) = if id_parts.len() == 2 {
        (id_parts[0] == "myself", id_parts[1].to_string())
    } else {
        (false, id_parts[0].to_string())
    };

    let priority = if id == myself_id { 0 } else { 1 };

    Some(NodeInfo { address, id, is_myself, priority })
}

fn read_and_prioritize_nodes(path: &str) -> Vec<NodeInfo> {
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
        Some(id) => id,
        None => return vec![], // No myself ID, no valid peers
    };

    let mut nodes: Vec<NodeInfo> = lines
        .into_iter()
        .filter_map(|line| parse_node_info(line, &my_repl_id))
        .filter(|node| !node.is_myself) // 🧼 Exclude self
        .collect();

    nodes.sort_by_key(|n| n.priority);
    nodes
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
    let nodes = read_and_prioritize_nodes(temp_file.path().to_str().unwrap());

    // There should be 3 nodes, all with priority 0 (same ID as myself)
    assert_eq!(nodes.len(), 4);
    assert_eq!(nodes.iter().filter(|n| n.priority == 0).count(), 2);
    assert_eq!(nodes.iter().filter(|n| n.priority == 1).count(), 2);

    // Optionally print for debugging
    for node in nodes {
        println!("{:?}", node);
    }
}
