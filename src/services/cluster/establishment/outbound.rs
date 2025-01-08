use anyhow::Context;

use crate::services::query_io::QueryIO;

#[derive(Debug, PartialEq)]
pub enum ConnectionResponse {
    PONG,
    OK,
    FULLRESYNC { repl_id: String, offset: i64 },
    PEERS(Vec<String>),
}

impl TryFrom<String> for ConnectionResponse {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "pong" => Ok(ConnectionResponse::PONG),
            "ok" => Ok(ConnectionResponse::OK),

            var if var.starts_with("fullresync") => {
                let mut iter = var.split_whitespace();
                let _ = iter.next();
                let repl_id = iter
                    .next()
                    .context("replication_id must be given")?
                    .to_string();
                let offset = iter
                    .next()
                    .context("offset must be given")?
                    .parse::<i64>()?;
                Ok(ConnectionResponse::FULLRESYNC { repl_id, offset })
            }

            peer_msg if peer_msg.starts_with("peers ") => {
                let res = peer_msg
                    .trim_start_matches("peers ")
                    .split_whitespace()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>();

                Ok(ConnectionResponse::PEERS(res))
            }

            invalid_value => {
                eprintln!("Invalid command,{}", invalid_value);
                Err(anyhow::anyhow!("Invalid command"))
            }
        }
    }
}

impl TryFrom<QueryIO> for ConnectionResponse {
    type Error = anyhow::Error;
    fn try_from(value: QueryIO) -> Result<Self, Self::Error> {
        match value {
            QueryIO::SimpleString(value) => Ok(value.try_into()?),
            _ => {
                eprintln!("Invalid command");
                Err(anyhow::anyhow!("Invalid command"))
            }
        }
    }
}
