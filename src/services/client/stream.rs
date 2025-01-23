use std::time::SystemTime;

use super::request::ClientRequest;
use crate::{
    make_smart_pointer,
    services::{interface::TStream, query_io::QueryIO},
};
use tokio::net::TcpStream;

pub struct ClientStream(pub(crate) TcpStream);
make_smart_pointer!(ClientStream, TcpStream);

impl ClientStream {
    pub(crate) async fn extract_query(&mut self) -> anyhow::Result<ClientRequest> {
        let query_io = self.read_value().await?;
        match query_io {
            QueryIO::Array(value_array) => {
                let values: Vec<String> = value_array
                    .into_iter()
                    .map(|v| v.unpack_single_entry::<String>())
                    .collect::<Result<_, _>>()?;

                match values.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
                    ["ping"] => Ok(ClientRequest::Ping),
                    ["get", key] => Ok(ClientRequest::Get { key: key.to_string() }),
                    ["set", key, value] => {
                        Ok(ClientRequest::Set { key: key.to_string(), value: value.to_string() })
                    }
                    ["set", key, value, "px", expiry] => Ok(ClientRequest::SetWithExpiry {
                        key: key.to_string(),
                        value: value.to_string(),
                        expiry: Self::extract_expiry(expiry)?,
                    }),
                    ["delete", key] => Ok(ClientRequest::Delete { key: key.to_string() }),
                    ["echo", value] => Ok(ClientRequest::Echo(value.to_string())),
                    ["config", key, value] => {
                        Ok(ClientRequest::Config { key: key.to_string(), value: value.to_string() })
                    }

                    ["keys", var] if var != &"" => {
                        let &"*" = var else { return Ok(ClientRequest::Keys { pattern: None }) };
                        Ok(ClientRequest::Keys { pattern: Some(var.to_string()) })
                    }
                    ["save"] => Ok(ClientRequest::Save),
                    ["info", _unused_value] => Ok(ClientRequest::Info),
                    _ => Err(anyhow::anyhow!("Invalid command")),
                }
            }
            _ => Err(anyhow::anyhow!("Unexpected command format")),
        }
    }

    fn extract_expiry(expiry: &str) -> anyhow::Result<SystemTime> {
        let systime =
            std::time::SystemTime::now() + std::time::Duration::from_millis(expiry.parse::<u64>()?);
        Ok(systime)
    }
}
