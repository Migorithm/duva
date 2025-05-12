use crate::{
    domains::{
        cluster_actors::replication::{ReplicationId, ReplicationRole},
        peers::peer::{NodeKind, PeerState},
    },
    env_var,
    prelude::PeerIdentifier,
};
use tokio::fs::OpenOptions;
use uuid::Uuid;

pub struct Environment {
    pub seed_server: Option<PeerIdentifier>,
    pub pre_connected_peers: Vec<PeerState>,
    pub(crate) repl_id: ReplicationId,
    pub(crate) role: ReplicationRole,
    pub dir: String,
    pub dbfilename: String,
    pub port: u16,
    pub host: String,
    pub hf_mills: u64,
    pub ttl_mills: u128,
    pub append_only: bool,
    pub topology_writer: Option<tokio::fs::File>,
    pub log_level: tracing::Level,
}

impl Environment {
    pub async fn init() -> Self {
        env_var!(
            defaults: {
                port: u16 = 6379,
                host: String = "127.0.0.1".to_string(),
                dir: String = ".".to_string(),
                dbfilename: String = "dump.rdb".to_string(),
                hf: u64 = 1000,
                ttl: u128 = 60000,
                append_only: bool = false,
                tpp: String = "duva.tp".to_string(),
                log_level : tracing::Level = tracing::Level::INFO,
            },
            optional: {
                replicaof
            }
        );

        let replicaof = Self::parse_replicaof(replicaof);
        let pre_connected_peers = PeerState::from_file(&tpp);
        let repl_id = Self::determine_repl_id(replicaof.as_ref(), &pre_connected_peers);
        let role = Self::determine_role(replicaof.as_ref(), &pre_connected_peers);
        let topology_writer = Self::open_topology_file(tpp).await;

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
            topology_writer: Some(topology_writer),
            pre_connected_peers,
            log_level,
        }
    }

    fn determine_repl_id(
        replicaof: Option<&PeerIdentifier>,
        pre_connected_peers: &[PeerState],
    ) -> ReplicationId {
        if replicaof.is_none() {
            ReplicationId::Key(
                pre_connected_peers
                    .iter()
                    .find(|p| p.kind == NodeKind::Replica)
                    .map(|p| p.replid.to_string())
                    .unwrap_or_else(|| Uuid::now_v7().to_string()),
            )
        } else {
            ReplicationId::Undecided
        }
    }

    fn determine_role(
        replicaof: Option<&PeerIdentifier>,
        pre_connected_peers: &[PeerState],
    ) -> ReplicationRole {
        if replicaof.is_none() && pre_connected_peers.is_empty() {
            ReplicationRole::Leader
        } else {
            ReplicationRole::Follower
        }
    }

    async fn open_topology_file(tpp: String) -> tokio::fs::File {
        OpenOptions::new().create(true).write(true).truncate(true).open(tpp).await.unwrap()
    }

    fn parse_replicaof(replicaof: Option<String>) -> Option<PeerIdentifier> {
        replicaof
            .and_then(|s| s.split_once(':').map(|(host, port)| format!("{}:{}", host, port).into()))
    }
}
