use crate::{domains::cluster_actors::replication::IS_LEADER_MODE, env_var};

pub struct Environment {
    pub replicaof: Option<(String, String)>,
    pub dir: String,
    pub dbfilename: String,
    pub port: u16,
    pub host: String,
    pub hf_mills: u64,
    pub ttl_mills: u128,
    pub append_only: bool,
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
                append_only = false
            }
        );
        let replicaof = replicaof.map(|host_port| {
            host_port
                .split_once(':')
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .into_iter()
                .collect::<(_, _)>()
        });
        IS_LEADER_MODE.store(replicaof.is_none(), std::sync::atomic::Ordering::Relaxed);

        Self { replicaof, dir, dbfilename, port, host, hf_mills: hf, ttl_mills: ttl, append_only }
    }
}
