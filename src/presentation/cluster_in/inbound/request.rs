use crate::{from_to, make_smart_pointer, services::query_io::QueryIO};
use anyhow::Context;
use bytes::Bytes;

pub(crate) struct HandShakeRequest {
    pub(crate) command: HandShakeRequestEnum,
    pub(crate) args: QueryArguments,
}
#[derive(Debug, Clone)]
pub struct QueryArguments(pub Vec<QueryIO>);

make_smart_pointer!(QueryArguments, Vec<QueryIO>);
from_to!(Vec<QueryIO>, QueryArguments);

impl HandShakeRequest {
    pub(crate) fn new(query_io: QueryIO) -> anyhow::Result<Self> {
        match query_io {
            QueryIO::Array(value_array) => {
                let mut iter = value_array.into_iter();

                Ok(Self {
                    command: iter
                        .next()
                        .context("request not given")?
                        .clone()
                        .unpack_single_entry::<String>()?
                        .try_into()?,
                    args: iter.collect::<Vec<_>>().into(),
                })
            }
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
                let value = String::from_utf8(port.to_vec())?.parse()?;
                Ok(value)
            }
            _ => Err(anyhow::anyhow!("Invalid listening-port arguments")),
        }
    }

    pub(crate) fn extract_capa(&mut self) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        self.match_query(HandShakeRequestEnum::ReplConf)?;
        if self.args.is_empty() || self.args.len() % 2 != 0 {
            return Err(anyhow::anyhow!("Invalid number of arguments"));
        }

        // Process pairs directly using chunks_exact
        let capabilities: Vec<(Bytes, Bytes)> = self
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

        let Some([repl_id, offset]) = self.args.get_mut(..2) else {
            return Err(anyhow::anyhow!("Invalid number of arguments"));
        };

        let replica_id = std::mem::take(repl_id).unpack_single_entry()?;
        let offset = std::mem::take(offset).unpack_single_entry()?;

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
