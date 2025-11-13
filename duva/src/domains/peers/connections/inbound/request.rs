use crate::domains::replications::*;
use crate::{err, from_to, make_smart_pointer};
use anyhow::Context;

pub(crate) struct HandShakeRequest {
    pub(crate) command: HandShakeRequestEnum,
    pub(crate) args: QueryArguments,
}
#[derive(Debug)]
pub struct QueryArguments(pub Vec<String>);

make_smart_pointer!(QueryArguments, Vec<String>);
from_to!(Vec<String>, QueryArguments);

impl HandShakeRequest {
    pub(crate) fn new(msg: String) -> anyhow::Result<Self> {
        let mut iter =
            msg.split_whitespace().map(|x| x.to_string()).collect::<Vec<String>>().into_iter();

        Ok(Self {
            command: iter.next().context("request not given")?.try_into()?,
            args: iter.collect::<Vec<_>>().into(),
        })
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
            [key, port, ..] if key == "listening-port" => {
                let value = port.parse()?;
                Ok(value)
            },
            _ => Err(anyhow::anyhow!("Invalid listening-port arguments")),
        }
    }

    pub(crate) fn extract_capa(&self) -> anyhow::Result<Vec<(String, String)>> {
        self.match_query(HandShakeRequestEnum::ReplConf)?;
        if self.args.is_empty() || !self.args.len().is_multiple_of(2) {
            return Err(anyhow::anyhow!("Invalid number of arguments"));
        }

        // Process pairs directly using chunks_exact
        let capabilities: Vec<(String, String)> = self
            .args
            .chunks_exact(2)
            .filter_map(|chunk| match (&chunk[0], &chunk[1]) {
                (capa, value) if capa == "capa" => Some((capa.clone(), value.clone())),
                _ => None,
            })
            .collect();

        // Validate last capability is psync2
        if capabilities.last().context("No capabilities given")?.1 != "psync2" {
            return Err(anyhow::anyhow!("psync2 must be given as the last capability"));
        }
        Ok(capabilities)
    }
    pub(crate) fn extract_psync(
        &mut self,
    ) -> anyhow::Result<(ReplicationId, u64, ReplicationRole)> {
        self.match_query(HandShakeRequestEnum::Psync)?;

        let Some([replica_id, offset, role]) = self.args.get_mut(..3) else {
            return Err(anyhow::anyhow!("Invalid number of arguments"));
        };

        Ok((replica_id.to_string().into(), offset.parse()?, role.to_string().into()))
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
                err!("Invalid command, {}", invalid_value);
                Err(anyhow::anyhow!("Invalid command"))
            },
        }
    }
}
