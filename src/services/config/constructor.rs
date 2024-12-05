use crate::services::config::config_actor::{Config, Replication};
use anyhow::Result;
use std::time::SystemTime;
use tokio::fs::try_exists;
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

impl Config {
    pub fn set_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }
    pub fn set_host(mut self, host: String) -> Self {
        self.host = host;
        self
    }
    pub fn set_dir(mut self, dir: String) -> Self {
        self.dir = dir;
        self
    }
    pub fn set_dbfilename(mut self, dbfilename: String) -> Self {
        self.dbfilename = Some(dbfilename);
        self
    }

    pub(super) fn get_dir(&self) -> &str {
        self.dir.as_str()
    }

    pub(super) fn get_db_filename(&self) -> Option<String> {
        self.dbfilename.clone()
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
}
