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
        let tops = read_and_sort_nodes(&tpp);
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

fn parse_node_info(line: &str) -> Option<NodeInfo> {
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

    let priority = if is_myself { 0 } else { 1 };

    Some(NodeInfo { address, id, is_myself, priority })
}

fn read_and_sort_nodes(path: &str) -> Vec<NodeInfo> {
    let contents = std::fs::read_to_string(path).unwrap_or_default();

    let mut nodes: Vec<NodeInfo> = contents
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(parse_node_info)
        .collect();

    nodes.sort_by_key(|n| n.priority);
    nodes
}
