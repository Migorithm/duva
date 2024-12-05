use super::command::{ConfigQuery, ConfigResource, ConfigResponse};
use crate::services::config::replication::Replication;
use std::time::SystemTime;
use tokio::{fs::try_exists, sync::mpsc::Receiver};

#[derive(Clone)]
pub struct Config {
    pub port: u16,
    pub host: String,
    pub(crate) dir: String,
    pub dbfilename: Option<String>,
    pub replication: Replication,
    pub(crate) startup_time: SystemTime,
}

impl Config {
    pub async fn handle(self, mut inbox: Receiver<ConfigQuery>) {
        // inner state

        while let Some(cmd) = inbox.recv().await {
            match cmd.resource {
                ConfigResource::Dir => {
                    let _ = cmd.callback.send(ConfigResponse::Dir(self.dir.clone()));
                }
                ConfigResource::DbFileName => {
                    let _ = cmd
                        .callback
                        .send(ConfigResponse::DbFileName(self.dbfilename.clone()));
                }
                ConfigResource::FilePath => {
                    let _ = cmd
                        .callback
                        .send(ConfigResponse::FilePath(self.get_filepath()));
                }
                ConfigResource::ReplicationInfo => {
                    let _ = cmd
                        .callback
                        .send(ConfigResponse::ReplicationInfo(self.replication.info()));
                }
            }
        }
    }

    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
    // The following is used on startup and check if the file exists
    pub async fn try_filepath(&self) -> Option<String> {
        match (&self.dir, &self.dbfilename) {
            (dir, Some(db_filename)) => {
                let file_path = format!("{}/{}", dir, db_filename);

                match try_exists(&file_path).await {
                    Ok(true) => Some(file_path),
                    Ok(false) => {
                        println!("File does not exist");
                        None
                    }
                    Err(_) => {
                        println!("Error in try_filepath");
                        None
                    }
                }
            }
            // Not given a dbfilename
            _ => None,
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
