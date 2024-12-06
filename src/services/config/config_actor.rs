use super::command::{ConfigQuery, ConfigResource, ConfigResponse};
use crate::services::config::replication::Replication;
use std::time::SystemTime;
use tokio::{fs::try_exists, sync::mpsc::Receiver};

#[derive(Clone)]
pub struct Config {
    pub port: u16,
    pub host: &'static str,
    pub(crate) dir: &'static str,
    pub dbfilename: &'static str,
    pub replication: Replication,
    pub(crate) startup_time: SystemTime,
}

impl Config {
    pub async fn handle(self, mut inbox: Receiver<ConfigQuery>) {
        // inner state

        while let Some(cmd) = inbox.recv().await {
            match cmd.resource {
                ConfigResource::Dir => {
                    let _ = cmd.callback.send(ConfigResponse::Dir(self.dir.into()));
                }
                ConfigResource::DbFileName => {
                    let _ = cmd
                        .callback
                        .send(ConfigResponse::DbFileName(self.dbfilename.into()));
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
    pub fn replication_bind_addr(&self) -> String {
        // TODO basic validation required for port number so it doesn't go over 65535
        format!("{}:{}", self.host, self.port + 10000)
    }

    pub fn set_dir(&mut self, dir: &str) {
        unsafe {
            // Get the pointer to the str
            let ptr: *const str = self.dir;
            // Recreate the Box from the pointer and drop it
            let _reclaimed_box: Box<str> = Box::from_raw(ptr as *mut str);
            self.dir = Box::leak(dir.into());
        }
    }
    // The following is used on startup and check if the file exists
    pub async fn try_filepath(&self) -> Option<String> {
        let file_path = self.get_filepath();
        match try_exists(&file_path).await {
            Ok(true) => Some(file_path),
            Ok(false) => {
                println!("File does not exist");
                None
            }
            Err(_) => {
                println!("Error in try_filepath");
                None
            } // Not given a dbfilename
        }
    }
    pub fn get_filepath(&self) -> String {
        format!("{}/{}", self.dir, self.dbfilename)
    }

    pub(crate) fn is_replica(&self) -> bool {
        self.replication.role() == "slave"
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
                replicaof
            }
            {
                dir = ".".to_string(),
                dbfilename = "dump.rdb".to_string(),
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
            host: Box::leak(host.into_boxed_str()),
            dir: Box::leak(dir.into_boxed_str()),
            dbfilename: Box::leak(dbfilename.into_boxed_str()),
            replication: Replication::new(replicaof),
            startup_time: SystemTime::now(),
        }
    }
}

#[test]
fn test_set_dir() {
    let mut config = Config::default();
    config.set_dir("test");
    assert_eq!(config.dir, "test");
    config.set_dir("test2");
    assert_eq!(config.dir, "test2");
}
