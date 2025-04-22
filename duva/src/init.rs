use crate::{
    domains::{
        cluster_actors::replication::{ReplicationId, ReplicationRole},
        peers::cluster_peer::{ClusterNode, NodeKind},
    },
    env_var,
    prelude::PeerIdentifier,
};
use uuid::Uuid;

pub struct Environment {
    pub seed_server: Option<PeerIdentifier>,
    pub pre_connected_peers: Vec<ClusterNode>,
    pub(crate) repl_id: ReplicationId,
    pub(crate) role: ReplicationRole,
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
            defaults: {
                port: u16 = 6379,
                host: String = "127.0.0.1".to_string(),
                dir: String = ".".to_string(),
                dbfilename: String = "dump.rdb".to_string(),
                hf: u64 = 1000,
                ttl: u128 = 60000,
                append_only: bool = false,
                tpp: String = "duva.tp".to_string()
            },
            optional: {
                replicaof
            }
        );

        let replicaof = replicaof.and_then(|s| {
            s.split_once(':').map(|(host, port)| format!("{}:{}", host, port).into())
        });

        let pre_connected_peers = ClusterNode::from_file(&tpp);

        let repl_id = if replicaof.is_none() {
            ReplicationId::Key(
                pre_connected_peers
                    .iter()
                    .find(|p| p.priority == NodeKind::Replica)
                    .map(|p| p.repl_id.clone())
                    .unwrap_or_else(|| Uuid::now_v7().to_string()),
            )
        } else {
            ReplicationId::Undecided
        };

        let role = if replicaof.is_none() && pre_connected_peers.is_empty() {
            ReplicationRole::Leader
        } else {
            ReplicationRole::Follower
        };

        Self {
            role,
            seed_server: replicaof,
            repl_id,
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
