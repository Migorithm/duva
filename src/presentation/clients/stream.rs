use super::request::ClientAction;
use crate::{
    domains::{IoError, query_parsers::QueryIO},
    make_smart_pointer,
    services::interface::TRead,
};
use anyhow::Context;
use chrono::{DateTime, Utc};
use tokio::net::TcpStream;

pub struct ClientStream(pub(crate) TcpStream);

make_smart_pointer!(ClientStream, TcpStream);

impl ClientStream {
    pub(crate) async fn extract_query(&mut self) -> Result<Vec<ClientAction>, IoError> {
        let query_ios = self.read_values().await?;

        query_ios
            .into_iter()
            .map(|query_io| match query_io {
                QueryIO::Array(value_array) => {
                    let mut values =
                        value_array.into_iter().flat_map(|v| v.unpack_single_entry::<String>());

                    let Some(command) = values.next() else {
                        return Err(IoError::Custom("Unexpected command format".to_string()));
                    };

                    self.parse_query(command.to_lowercase(), values.collect())
                        .map_err(|e| IoError::Custom(e.to_string()))
                },
                _ => Err(IoError::Custom("Unexpected command format".to_string())),
            })
            .collect()
    }

    /// Analyze the command and arguments to create a `ClientRequest`
    fn parse_query(&self, cmd: String, args: Vec<String>) -> anyhow::Result<ClientAction> {
        match (cmd.as_str(), args.as_slice()) {
            ("ping", []) => Ok(ClientAction::Ping),
            ("get", [key]) => Ok(ClientAction::Get { key: key.to_string() }),
            ("get", [key, index]) => {
                Ok(ClientAction::IndexGet { key: key.to_string(), index: index.parse()? })
            },
            ("set", [key, value]) => {
                Ok(ClientAction::Set { key: key.to_string(), value: value.to_string() })
            },
            ("set", [key, value, px, expiry]) if px.to_lowercase() == "px" => {
                Ok(ClientAction::SetWithExpiry {
                    key: key.to_string(),
                    value: value.to_string(),
                    expiry: Self::extract_expiry(expiry)?,
                })
            },
            ("delete", [key]) => Ok(ClientAction::Delete { key: key.to_string() }),
            ("echo", [value]) => Ok(ClientAction::Echo(value.to_string())),
            ("config", [key, value]) => {
                Ok(ClientAction::Config { key: key.to_string(), value: value.to_string() })
            },

            ("keys", [var]) if !var.is_empty() => {
                if var == "*" {
                    return Ok(ClientAction::Keys { pattern: None });
                }
                Ok(ClientAction::Keys { pattern: Some(var.to_string()) })
            },
            ("save", []) => Ok(ClientAction::Save),
            ("info", [_unused_value]) => Ok(ClientAction::Info),
            ("cluster", val) if !val.is_empty() => match val[0].to_lowercase().as_str() {
                "info" => Ok(ClientAction::ClusterInfo),
                "nodes" => Ok(ClientAction::ClusterNodes),
                "forget" => {
                    Ok(ClientAction::ClusterForget(val.get(1).cloned().context("Must")?.into()))
                },
                _ => Err(anyhow::anyhow!("Invalid command")),
            },

            _ => Err(anyhow::anyhow!("Invalid command")),
        }
    }

    fn extract_expiry(expiry: &str) -> anyhow::Result<DateTime<Utc>> {
        let expiry = expiry.parse::<i64>().context("Invalid expiry")?;
        Ok(Utc::now() + chrono::Duration::milliseconds(expiry))
    }
}
