use anyhow::Context;

use crate::domains::query_parsers::QueryIO;

#[derive(Debug, PartialEq)]
pub enum ConnectionResponse {
    PONG,
    OK,
    FULLRESYNC { repl_id: String, offset: u64 },
    PEERS(Vec<String>),
}

impl TryFrom<String> for ConnectionResponse {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "pong" => Ok(ConnectionResponse::PONG),
            "ok" => Ok(ConnectionResponse::OK),

            var if var.starts_with("fullresync") => {
                let [_, repl_id, offset] = var
                    .split_whitespace()
                    .take(3)
                    .collect::<Vec<_>>()
                    .as_slice()
                    .try_into()
                    .context("Must have command, replication_id and offset")?;

                let offset = offset.parse::<u64>()?;

                Ok(ConnectionResponse::FULLRESYNC { repl_id: repl_id.to_string(), offset })
            },

            peer_msg if peer_msg.starts_with("peers ") => {
                let res = peer_msg
                    .trim_start_matches("peers ")
                    .split_whitespace()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>();

                Ok(ConnectionResponse::PEERS(res))
            },

            invalid_value => {
                eprintln!("Invalid command,{}", invalid_value);
                Err(anyhow::anyhow!("Invalid command"))
            },
        }
    }
}

impl TryFrom<QueryIO> for ConnectionResponse {
    type Error = anyhow::Error;
    fn try_from(value: QueryIO) -> Result<Self, Self::Error> {
        match value {
            QueryIO::SimpleString(value) => Ok(String::from_utf8(value.into())?.try_into()?),
            _ => {
                eprintln!("Invalid command");
                Err(anyhow::anyhow!("Invalid command"))
            },
        }
    }
}
