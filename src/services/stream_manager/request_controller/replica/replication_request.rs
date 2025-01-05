use anyhow::Context;

use crate::services::stream_manager::query_io::QueryIO;

use super::arguments::QueryArguments;

pub enum ReplicationRequest {}

pub(crate) struct HandShakeCommand {
    pub(crate) command: HandShakeRequest,
    pub(crate) args: QueryArguments,
}

impl HandShakeCommand {
    pub(crate) fn new(command: HandShakeRequest, args: QueryArguments) -> Self {
        HandShakeCommand { command, args }
    }
    pub(crate) fn match_query(&self, request: HandShakeRequest) -> anyhow::Result<()> {
        if self.command == request {
            Ok(())
        } else {
            Err(anyhow::anyhow!("{:?} not given during handshake", request))
        }
    }

    pub(crate) fn extract_listening_port(&mut self) -> anyhow::Result<u16> {
        self.match_query(HandShakeRequest::ReplConf)?;

        if self.args.len() < 2 {
            return Err(anyhow::anyhow!("Not enough arguments"));
        }

        match self.args.as_mut_slice() {
            [QueryIO::BulkString(key), QueryIO::BulkString(port), ..]
                if key == "listening-port" =>
            {
                Ok(port.parse::<u16>()?)
            }
            _ => Err(anyhow::anyhow!("Invalid listening-port arguments")),
        }
    }

    pub(crate) fn extract_capa(&mut self) -> anyhow::Result<Vec<(String, String)>> {
        self.match_query(HandShakeRequest::ReplConf)?;
        if self.args.is_empty() || self.args.len() % 2 != 0 {
            return Err(anyhow::anyhow!("Invalid number of arguments"));
        }

        // Process pairs directly using chunks_exact
        let capabilities: Vec<(String, String)> = self
            .args
            .chunks_exact(2)
            .filter_map(|chunk| match (&chunk[0], &chunk[1]) {
                (QueryIO::BulkString(capa), QueryIO::BulkString(value)) if capa == "capa" => {
                    Some((capa.clone(), value.clone()))
                }
                _ => None,
            })
            .collect();

        // Validate last capability is psync2
        if capabilities.last().context("No capabilities given")?.1 != "psync2" {
            return Err(anyhow::anyhow!(
                "psync2 must be given as the last capability"
            ));
        }
        Ok(capabilities)
    }
    pub(crate) fn extract_psync(&mut self) -> anyhow::Result<(String, i64)> {
        self.match_query(HandShakeRequest::Psync)?;

        let replica_id = self
            .args
            .get(0)
            .map(|v| v.clone().unpack_bulk_str())
            .ok_or(anyhow::anyhow!("No replica id"))??;
        let offset = self
            .args
            .get(1)
            .map(|v| v.clone().unpack_bulk_str().map(|s| s.parse::<i64>()))
            .ok_or(anyhow::anyhow!("No offset"))???;
        Ok((replica_id, offset))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum HandShakeRequest {
    Ping,
    ReplConf,
    Psync,
}

impl TryFrom<String> for ReplicationRequest {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            _ => Err(anyhow::anyhow!("Invalid command")),
        }
    }
}

impl TryFrom<String> for HandShakeRequest {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "ping" => Ok(HandShakeRequest::Ping),
            "replconf" => Ok(HandShakeRequest::ReplConf),
            "psync" => Ok(HandShakeRequest::Psync),

            invalid_value => {
                eprintln!("Invalid command,{}", invalid_value);
                Err(anyhow::anyhow!("Invalid command"))
            }
        }
    }
}

pub enum HandShakeResponse {
    PONG,
    OK,
    FULLRESYNC { repl_id: String, offset: i64 },
}

impl TryFrom<String> for HandShakeResponse {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "pong" => Ok(HandShakeResponse::PONG),
            "ok" => Ok(HandShakeResponse::OK),

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
                Ok(HandShakeResponse::FULLRESYNC { repl_id, offset })
            }

            invalid_value => {
                eprintln!("Invalid command,{}", invalid_value);
                Err(anyhow::anyhow!("Invalid command"))
            }
        }
    }
}
