use std::sync::LazyLock;

use crate::{
    domains::{
        cluster_actors::replication::ReplicationRole,
        peers::{identifier::TPeerAddress, peer::PeerState},
    },
    env_var,
    prelude::PeerIdentifier,
};
use std::fs::OpenOptions;

pub struct Environment {
    pub seed_server: Option<PeerIdentifier>,
    pub stored_peer_states: Vec<PeerState>,
    pub(crate) role: ReplicationRole,
    pub dir: String,
    pub dbfilename: String,
    pub port: u16,
    pub host: String,
    pub hf_mills: u64,
    pub append_only: bool,
    pub tpp: String,
    pub log_level: tracing::Level,
}

impl Environment {
    pub fn init() -> Self {
        env_var!(
            defaults: {
                port: u16 = 6379,
                host: String = "127.0.0.1".to_string(),
                dir: String = ".".to_string(),
                dbfilename: String = "dump.rdb".to_string(),
                hf: u64 = 1000,
                append_only: bool = false,
                tpp: String = "duva.tp".to_string(),
                log_level : tracing::Level = tracing::Level::INFO,
            },
            optional: {
                replicaof
            }
        );

        let replicaof = replicaof.map(|s| PeerIdentifier(s.bind_addr().unwrap()));
        let stored_peer_states = PeerState::from_file(&tpp);
        let role = Self::determine_role(replicaof.as_ref(), &stored_peer_states);

        Self {
            role,
            seed_server: replicaof,
            dir,
            dbfilename,
            port,
            host,
            hf_mills: hf,
            append_only,
            tpp,
            stored_peer_states,
            log_level,
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

    pub async fn open_topology_file(tpp: String) -> std::fs::File {
        OpenOptions::new().create(true).write(true).truncate(true).open(tpp).unwrap()
    }

    pub(crate) fn get_filepath(&self) -> String {
        format!("{}/{}", self.dir, self.dbfilename)
    }

    pub(crate) fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub(crate) fn peer_bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port + 10000)
    }
}

pub static ENV: LazyLock<Environment> = LazyLock::new(Environment::init);
