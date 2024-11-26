use std::sync::OnceLock;

use anyhow::Result;
use tokio::fs::try_exists;

use crate::services::config_manager::command::{ConfigCommand, ConfigResource};
pub(crate) struct Config {
    pub(crate) port: u16,
    pub(crate) host: &'static str,
    pub(crate) dir: Option<String>,
    pub(crate) dbfilename: Option<String>,
}

macro_rules! env_var {
    ($($env_name:ident),*) => {
        $(let mut $env_name =  std::env::var(stringify!($env_name)).ok();)*

        let mut args = std::env::args().skip(1); // Skip the program name
        while let Some(arg) = args.next(){
            match arg.as_str(){
                $(
                    stringify!(-- $env_name) => {
                    if let Some(value) = args.next(){
                        $env_name = Some(value);
                    }
                })*
                _ => {
                    eprintln!("Unexpected argument: {}", arg);
                }
            }
        }
    };
}

impl Config {
    pub fn new() -> Self {
        env_var!(dir, dbfilename);

        Config {
            port: 6379,
            host: "localhost",
            dir,
            dbfilename,
        }
    }
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    // The following is used on startup and check if the file exists
    pub async fn try_filepath(&self) -> Result<Option<String>> {
        match (&self.dir, &self.dbfilename) {
            (Some(dir), Some(db_filename)) => {
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
            (Some(dir), Some(db_filename)) => {
                let file_path = format!("{}/{}", dir, db_filename);
                Some(file_path)
            }
            _ => None,
        }
    }
    // perhaps, set operation is needed
    pub fn handle_config(&self, cmd: ConfigCommand) -> Option<String> {
        match cmd {
            ConfigCommand::Get(ConfigResource::Dir) => self.get_dir(),
            ConfigCommand::Get(ConfigResource::DbFileName) => self.get_db_filename(),
            ConfigCommand::FilePath => self.get_filepath(),
        }
    }
    fn get_dir(&self) -> Option<String> {
        self.dir.clone()
    }

    fn get_db_filename(&self) -> Option<String> {
        self.dbfilename.clone()
    }
}

static CONFIG: OnceLock<Config> = OnceLock::new();

pub fn config() -> &'static Config {
    CONFIG.get_or_init(|| Config::new())
}
