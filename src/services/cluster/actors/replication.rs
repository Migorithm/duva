use crate::services::config::init::get_env;
use std::sync::atomic::AtomicBool;

use super::types::PeerAddr;
pub static IS_MASTER_MODE: AtomicBool = AtomicBool::new(true);

#[derive(Debug, Clone)]
pub struct Replication {
    pub(crate) connected_slaves: u16, // The number of connected replicas
    pub(crate) master_replid: String, // The replication ID of the master example: 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb
    pub(crate) master_repl_offset: u64, // The server's current replication offset. Example: 0
    second_repl_offset: i16,          // -1
    repl_backlog_active: usize,       // 0
    repl_backlog_size: usize,         // 1048576
    repl_backlog_first_byte_offset: usize, // 0
    role: String,
    //If the instance is a replica, these additional fields are provided:
    pub(crate) master_host: Option<String>,
    pub(crate) master_port: Option<u16>,
}

impl Default for Replication {
    fn default() -> Self {
        let replication = Replication::new(get_env().replicaof.clone());
        IS_MASTER_MODE
            .store(replication.master_port.is_none(), std::sync::atomic::Ordering::Relaxed);
        replication
    }
}

impl Replication {
    pub fn new(replicaof: Option<(String, String)>) -> Self {
        Replication {
            connected_slaves: 0, // dynamically configurable
            master_replid: if replicaof.is_none() {
                "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string()
            } else {
                "?".to_string()
            },
            master_repl_offset: 0,
            second_repl_offset: -1,
            repl_backlog_active: 0,
            repl_backlog_size: 1048576,
            repl_backlog_first_byte_offset: 0,
            role: if replicaof.is_some() { "slave".to_string() } else { "master".to_string() },
            master_host: replicaof.as_ref().cloned().map(|(host, _)| host),
            master_port: replicaof
                .map(|(_, port)| port.parse().expect("Invalid port number of given")),
        }
    }
    pub fn vectorize(&self) -> Vec<String> {
        vec![
            format!("role:{}", self.role),
            format!("connected_slaves:{}", self.connected_slaves),
            format!("master_replid:{}", self.master_replid),
            format!("master_repl_offset:{}", self.master_repl_offset),
            format!("second_repl_offset:{}", self.second_repl_offset),
            format!("repl_backlog_active:{}", self.repl_backlog_active),
            format!("repl_backlog_size:{}", self.repl_backlog_size),
            format!("repl_backlog_first_byte_offset:{}", self.repl_backlog_first_byte_offset),
        ]
    }

    pub fn master_cluster_bind_addr(&self) -> PeerAddr {
        format!("{}:{}", self.master_host.as_ref().unwrap(), self.master_port.unwrap() + 10000)
            .into()
    }
}
