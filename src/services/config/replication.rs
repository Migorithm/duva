use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct Replication {
    pub connected_slaves: u16,             // The number of connected replicas
    master_replid: String, // The replication ID of the master example: 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb
    master_repl_offset: u64, // The replication offset of the master example: 0
    second_repl_offset: i16, // -1
    repl_backlog_active: usize, // 0
    repl_backlog_size: usize, // 1048576
    repl_backlog_first_byte_offset: usize, // 0
    repl_info_map: HashMap<String, HashMap<String, String>>, // map of replication info

    pub master_host: Option<String>,
    pub master_port: Option<u16>,
}

impl Replication {
    pub fn new(replicaof: Option<(String, String)>) -> Self {
        Replication {
            connected_slaves: 0, // dynamically configurable
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            master_repl_offset: 0,
            second_repl_offset: -1,
            repl_backlog_active: 0,
            repl_backlog_size: 1048576,
            repl_backlog_first_byte_offset: 0,
            master_host: replicaof.as_ref().cloned().map(|(host, _)| host),
            master_port: replicaof
                .map(|(_, port)| port.parse().expect("Invalid port number of given")),
            repl_info_map: HashMap::new(),
        }
    }
    pub fn info(&self) -> Vec<String> {
        vec![
            self.role(),
            format!("connected_slaves:{}", self.connected_slaves),
            format!("master_replid:{}", self.master_replid),
            format!("master_repl_offset:{}", self.master_repl_offset),
            format!("second_repl_offset:{}", self.second_repl_offset),
            format!("repl_backlog_active:{}", self.repl_backlog_active),
            format!("repl_backlog_size:{}", self.repl_backlog_size),
            format!(
                "repl_backlog_first_byte_offset:{}",
                self.repl_backlog_first_byte_offset
            ),
        ]
    }
    pub fn role(&self) -> String {
        self.master_host
            .is_some()
            .then(|| "role:slave")
            .unwrap_or("role:master")
            .to_string()
    }

    pub fn set_new_replica(&mut self, replica_id: String) {
        self.repl_info_map.insert(replica_id, HashMap::new());
    }

    pub fn set_replica_info(&mut self, replica_id: String, key: String, value: String) {
        self.repl_info_map
            .entry(replica_id)
            .or_insert_with(HashMap::new)
            .insert(key, value);
    }

    pub fn get_single_replica_info(&self, replica_id: &str) -> HashMap<String, String> {
        self.repl_info_map.get(replica_id).unwrap_or(&HashMap::new()).clone()
    }
}
