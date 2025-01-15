use std::sync::OnceLock;

use crate::{env_var, services::cluster::actors::replication::IS_MASTER_MODE};

pub(crate) struct Environment {
    pub(crate) replicaof: Option<(String, String)>,
    pub(crate) dir: String,
    pub(crate) dbfilename: String,
}

impl Environment {
    pub fn new() -> Self {
        env_var!(
            {
                replicaof
            }
            {
                dir = ".".to_string(),
                dbfilename = "dump.rdb".to_string()
            }
        );

        let replicaof = replicaof.map(|host_port| {
            host_port
                .split_once(':')
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .into_iter()
                .collect::<(_, _)>()
        });
        IS_MASTER_MODE.store(replicaof.is_none(), std::sync::atomic::Ordering::Relaxed);

        Self { replicaof, dir, dbfilename }
    }
}

static E: OnceLock<Environment> = OnceLock::new();

pub(crate) fn get_env() -> &'static Environment {
    E.get_or_init(Environment::new)
}
