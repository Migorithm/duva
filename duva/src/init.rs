use crate::{domains::peers::cluster_peer::ClusterNode, env_var, prelude::PeerIdentifier};

pub struct Environment {
    pub seed_server: Option<PeerIdentifier>,
    pub pre_connected_peers: Vec<ClusterNode>,
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
        let pre_connected_peers = ClusterNode::from_file(&tpp);

        Self {
            seed_server: replicaof,
            dir,
            dbfilename,
            port,
            host,
            hf_mills: hf,
            ttl_mills: ttl,
            append_only,
            topology_path: tpp,
            pre_connected_peers,
        }
    }
}
