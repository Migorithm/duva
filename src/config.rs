use anyhow::Result;
use std::time::SystemTime;
use tokio::fs::try_exists;

pub enum ConfigCommand {
    Dir,
    DbFileName,
}

macro_rules! env_var {
    (
        {
            $($env_name:ident),*
        }
        $({
            $($default:ident = $default_value:expr),*
        })?
    ) => {
        $(
            // Initialize the variable with the environment variable or the default value.
            let mut $env_name = std::env::var(stringify!($env_name))
                .ok();
        )*

        let mut args = std::env::args().skip(1);
        $(
            $(let mut $default = $default_value;)*
        )?

        while let Some(arg) = args.next(){
            match arg.as_str(){
                $(
                    concat!("--", stringify!($env_name)) => {
                    if let Some(value) = args.next(){
                        $env_name = Some(value.parse().unwrap());
                    }
                })*
                $(
                    $(
                        concat!("--", stringify!($default)) => {
                        if let Some(value) = args.next(){
                            $default = value.parse().expect("Default value must be given");
                        }
                    })*
                )?


                _ => {
                    eprintln!("Unexpected argument: {}", arg);
                }
            }
        }
    };
}

impl TryFrom<(&str, &str)> for ConfigCommand {
    type Error = anyhow::Error;
    fn try_from((cmd, resource): (&str, &str)) -> anyhow::Result<Self> {
        match (
            cmd.to_lowercase().as_str(),
            resource.to_lowercase().as_str(),
        ) {
            ("get", "dir") => Ok(ConfigCommand::Dir),
            ("get", "dbfilename") => Ok(ConfigCommand::DbFileName),
            _ => Err(anyhow::anyhow!("Invalid arguments")),
        }
    }
}

pub struct Config {
    pub port: u16,
    pub host: String,
    pub(crate) dir: String,
    pub(crate) dbfilename: Option<String>,
    pub replication: Replication,
    pub(crate) startup_time: SystemTime,
}

impl Default for Config {
    fn default() -> Self {
        env_var!(
            {
                dbfilename,
                replicaof
            }
            {
                dir = ".".to_string(),
                port = 6379,
                host = "localhost".to_string()
            }
        );

        let replicaof = replicaof.map(|host_port| {
            host_port
                .split_once(' ')
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .into_iter()
                .collect::<(_, _)>()
        });

        Config {
            port,
            host,
            dir,
            dbfilename,
            replication: Replication::new(replicaof),
            startup_time: SystemTime::now(),
        }
    }
}

impl Config {
    pub fn set_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }
    pub fn set_host(mut self, host: String) -> Self {
        self.host = host;
        self
    }
    fn get_dir(&self) -> &str {
        self.dir.as_str()
    }
    pub fn set_dir(mut self, dir: String) -> Self {
        self.dir = dir;
        self
    }

    fn get_db_filename(&self) -> Option<String> {
        self.dbfilename.clone()
    }
    pub fn set_dbfilename(mut self, dbfilename: String) -> Self {
        self.dbfilename = Some(dbfilename);
        self
    }

    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
    // The following is used on startup and check if the file exists
    pub async fn try_filepath(&self) -> Result<Option<String>> {
        match (&self.dir, &self.dbfilename) {
            (dir, Some(db_filename)) => {
                let file_path = format!("{}/{}", dir, db_filename);
                if try_exists(&file_path).await? {
                    println!("The file exists.");
                    Ok(Some(file_path))
                } else {
                    println!("The file does NOT exist.");
                    Ok(None)
                }
            }
            _ => Err(anyhow::anyhow!("dir and db_filename not given")),
        }
    }
    pub fn get_filepath(&self) -> Option<String> {
        match (&self.dir, &self.dbfilename) {
            (dir, Some(db_filename)) => {
                let file_path = format!("{}/{}", dir, db_filename);
                Some(file_path)
            }
            _ => None,
        }
    }
    // perhaps, set operation is needed
    pub fn handle_config(&self, cmd: ConfigCommand) -> Option<String> {
        match cmd {
            ConfigCommand::Dir => Some(self.get_dir().to_string()),
            ConfigCommand::DbFileName => self.get_db_filename(),
        }
    }

    pub fn replication_info(&self) -> Vec<String> {
        self.replication.info()
    }
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
