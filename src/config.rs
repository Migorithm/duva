use anyhow::Result;
use tokio::fs::{read, try_exists};
pub(crate) struct Config {
    pub(crate) port: u16,
    pub(crate) host: &'static str,
    pub(crate) dir: Option<String>,
    pub(crate) db_filename: Option<String>,
}

impl Config {
    pub fn new() -> Self {
        let mut dir = None;
        let mut db_filename = None;

        let mut args = std::env::args().skip(1); // Skip the program name
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--dir" => {
                    if let Some(value) = args.next() {
                        dir = Some(value);
                    }
                }
                "--dbfilename" => {
                    if let Some(value) = args.next() {
                        db_filename = Some(value);
                    }
                }
                _ => {
                    eprintln!("Unexpected argument: {}", arg);
                }
            }
        }

        Config {
            port: 6379,
            host: "localhost",
            dir,
            db_filename,
        }
    }
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub async fn parse_filepath(&self) -> Result<String> {
        match (&self.dir, &self.db_filename) {
            (Some(dir), Some(db_filename)) => {
                let file_path = format!("{}/{}", dir, db_filename);
                if try_exists(&file_path).await? {
                    println!("The file exists.");
                } else {
                    println!("The file does not exist.");
                }
                Ok(file_path)
            }
            _ => Err(anyhow::anyhow!("dir and db_filename not given")),
        }
    }
}
