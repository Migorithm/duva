use crate::domains::{QueryIO, replications::*};
use anyhow::Context;

#[derive(Debug, PartialEq)]
pub enum ConnectionResponse {
    Pong,
    Ok,
    FullResync { id: String, repl_id: String, offset: u64, role: ReplicationRole },
}

impl TryFrom<String> for ConnectionResponse {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            | "pong" => Ok(ConnectionResponse::Pong),
            | "ok" => Ok(ConnectionResponse::Ok),

            | var if var.starts_with("fullresync") => {
                let [_, id, repl_id, offset, role] = var
                    .split_whitespace()
                    .take(5)
                    .collect::<Vec<_>>()
                    .as_slice()
                    .try_into()
                    .context("Must have command, replication_id and offset")?;

                let offset = offset.parse::<u64>()?;

                Ok(ConnectionResponse::FullResync {
                    id: id.to_string(),
                    repl_id: repl_id.to_string(),
                    offset,
                    role: role.to_string().into(),
                })
            },

            | invalid_value => {
                eprintln!("Invalid command,{invalid_value}");
                Err(anyhow::anyhow!("Invalid command"))
            },
        }
    }
}

impl TryFrom<QueryIO> for ConnectionResponse {
    type Error = anyhow::Error;
    fn try_from(value: QueryIO) -> Result<Self, Self::Error> {
        match value {
            | QueryIO::SimpleString(value) => Ok(String::from_utf8(value.into())?.try_into()?),
            | _ => {
                eprintln!("Invalid command");
                Err(anyhow::anyhow!("Invalid command"))
            },
        }
    }
}
