use crate::env_var;

pub struct Environment {
    pub replicaof: Option<(String, String)>,
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
        let replicaof = replicaof.map(|host_port| {
            host_port
                .split_once(':')
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .into_iter()
                .collect::<(_, _)>()
        });

        Self {
            replicaof,
            dir,
            dbfilename,
            port,
            host,
            hf_mills: hf,
            ttl_mills: ttl,
            append_only,
            topology_path: tpp,
        }
    }
}
