use crate::services::cluster::inbound::arguments::QueryArguments;
use crate::services::query_io::QueryIO;
use anyhow::Context;

pub(crate) struct HandShakeRequest {
    pub(crate) command: HandShakeRequestEnum,
    pub(crate) args: QueryArguments,
}

impl HandShakeRequest {
    pub(crate) fn new(query_io: QueryIO) -> anyhow::Result<Self> {
        match query_io {
            QueryIO::Array(value_array) => Ok(Self {
                command: value_array
                    .first()
                    .context("request not given")?
                    .clone()
                    .unpack_bulk_str()?
                    .try_into()?,
                args: QueryArguments::new(value_array.into_iter().skip(1).collect()),
            }),
            _ => Err(anyhow::anyhow!("Unexpected command format")),
        }
    }

    pub(crate) fn match_query(&self, request: HandShakeRequestEnum) -> anyhow::Result<()> {
        if self.command == request {
            Ok(())
        } else {
            Err(anyhow::anyhow!("{:?} not given during handshake", request))
        }
    }

    pub(crate) fn extract_listening_port(&mut self) -> anyhow::Result<u16> {
        self.match_query(HandShakeRequestEnum::ReplConf)?;

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
        self.match_query(HandShakeRequestEnum::ReplConf)?;
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
            return Err(anyhow::anyhow!("psync2 must be given as the last capability"));
        }
        Ok(capabilities)
    }
    pub(crate) fn extract_psync(&mut self) -> anyhow::Result<(String, i64)> {
        self.match_query(HandShakeRequestEnum::Psync)?;

        let replica_id = self
            .args
            .first()
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
pub enum HandShakeRequestEnum {
    Ping,
    ReplConf,
    Psync,
}

impl TryFrom<String> for HandShakeRequestEnum {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "ping" => Ok(HandShakeRequestEnum::Ping),
            "replconf" => Ok(HandShakeRequestEnum::ReplConf),
            "psync" => Ok(HandShakeRequestEnum::Psync),

            invalid_value => {
                eprintln!("Invalid command,{}", invalid_value);
                Err(anyhow::anyhow!("Invalid command"))
            }
        }
    }
}
