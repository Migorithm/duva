use std::time::SystemTime;

use tokio::sync::mpsc::{Receiver, Sender};

use super::command::ConfigCommand;

pub struct Config {
    pub port: u16,
    pub host: String,
    pub(crate) dir: String,
    pub(crate) dbfilename: Option<String>,
    pub replication: Replication,
    pub(crate) startup_time: SystemTime,
}

impl Config {
    // perhaps, set operation is needed
    pub fn handle_config(&self, cmd: ConfigCommand) -> Option<String> {
        match cmd {
            ConfigCommand::Dir => Some(self.get_dir().to_string()),
            ConfigCommand::DbFileName => self.get_db_filename(),
        }
    }

    pub fn run() -> Sender<ConfigCommand> {
        let config = Config::default();
        let (tx, inbox) = tokio::sync::mpsc::channel(20);
        tokio::spawn(config.handle(inbox));

        tx
    }
    pub async fn handle(self, inbox: Receiver<ConfigCommand>) {}
}

#[derive(Debug, Clone, Default)]
pub struct Replication {
    pub connected_slaves: u16,             // The number of connected replicas
    master_replid: String, // The replication ID of the master example: 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb
    master_repl_offset: u64, // The replication offset of the master example: 0
    second_repl_offset: i16, // -1
    repl_backlog_active: usize, // 0
    repl_backlog_size: usize, // 1048576
    repl_backlog_first_byte_offset: usize, // 0

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
}

#[test]
fn feature() {
    let replicaof: Option<String> = None;
    let replicaof: Option<(String, String)> = replicaof.map(|host_port| {
        host_port
            .split_once(' ')
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .into_iter()
            .collect::<(_, _)>()
    });

    println!("{:?}", replicaof);
}
