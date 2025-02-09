use std::time::SystemTime;

use super::request::ClientRequest;
use crate::{
    make_smart_pointer,
    services::{interface::TRead, query_io::QueryIO},
};
use anyhow::Context;
use tokio::net::TcpStream;

pub struct ClientStream(pub(crate) TcpStream);

make_smart_pointer!(ClientStream, TcpStream);

impl ClientStream {
    pub(crate) async fn extract_query(&mut self) -> anyhow::Result<Vec<ClientRequest>> {
        let query_ios = self.read_values().await?;
        Ok(query_ios
            .into_iter()
            .map(|query_io| match query_io {
                QueryIO::Array(value_array) => {
                    let mut values =
                        value_array.into_iter().flat_map(|v| v.unpack_single_entry::<String>());

                    let command = values.next().context("Command not given")?.to_lowercase();

                    self.parse_query(command, values.collect())
                }
                _ => Err(anyhow::anyhow!("Unexpected command format")),
            })
            .flatten()
            .collect::<Vec<_>>()
            .into())
    }

    fn parse_query(&self, cmd: String, args: Vec<String>) -> anyhow::Result<ClientRequest> {
        match (cmd.as_str(), args.as_slice()) {
            ("ping", []) => Ok(ClientRequest::Ping),
            ("get", [key]) => Ok(ClientRequest::Get { key: key.to_string() }),
            ("set", [key, value]) => {
                Ok(ClientRequest::Set { key: key.to_string(), value: value.to_string() })
            }
            ("set", [key, value, px, expiry]) if px.to_lowercase() == "px" => {
                Ok(ClientRequest::SetWithExpiry {
                    key: key.to_string(),
                    value: value.to_string(),
                    expiry: Self::extract_expiry(expiry)?,
                })
            }
            ("delete", [key]) => Ok(ClientRequest::Delete { key: key.to_string() }),
            ("echo", [value]) => Ok(ClientRequest::Echo(value.to_string())),
            ("config", [key, value]) => {
                Ok(ClientRequest::Config { key: key.to_string(), value: value.to_string() })
            }

            ("keys", [var]) if !var.is_empty() => {
                if var == "*" {
                    return Ok(ClientRequest::Keys { pattern: None });
                }
                Ok(ClientRequest::Keys { pattern: Some(var.to_string()) })
            }
            ("save", []) => Ok(ClientRequest::Save),
            ("info", [_unused_value]) => Ok(ClientRequest::Info),
            ("cluster", val) if !val.is_empty() => match val[0].to_lowercase().as_str() {
                "info" => Ok(ClientRequest::ClusterInfo),
                "forget" => {
                    Ok(ClientRequest::ClusterForget(val.get(1).cloned().context("Must")?.into()))
                }
                _ => Err(anyhow::anyhow!("Invalid command")),
            },

            _ => Err(anyhow::anyhow!("Invalid command")),
        }
    }

    fn extract_expiry(expiry: &str) -> anyhow::Result<SystemTime> {
        let systime =
            std::time::SystemTime::now() + std::time::Duration::from_millis(expiry.parse::<u64>()?);
        Ok(systime)
    }
}
