use super::request::{ClientAction, ClientRequest};
use crate::{
    domains::{IoError, cluster_actors::session::SessionRequest, query_parsers::QueryIO},
    make_smart_pointer,
    services::interface::TRead,
};
use anyhow::Context;
use chrono::{DateTime, Utc};
use tokio::net::TcpStream;

pub struct ClientStream(pub(crate) TcpStream);

make_smart_pointer!(ClientStream, TcpStream);

impl ClientStream {
    pub(crate) async fn extract_query(&mut self) -> Result<Vec<ClientRequest>, IoError> {
        let query_ios = self.read_values().await?;

        query_ios
            .into_iter()
            .map(|query_io| match query_io {
                QueryIO::Array(value) => {
                    let (command, args) = Self::extract_command_args(value)?;
                    self.parse_query(None, command.to_lowercase(), args)
                        .map_err(|e| IoError::Custom(e.to_string()))
                },
                QueryIO::SessionRequest { request_id, value } => {
                    let (command, args) = Self::extract_command_args(value)?;
                    self.parse_query(
                        Some(SessionRequest::new(request_id, self.client_id())),
                        command.to_lowercase(),
                        args,
                    )
                    .map_err(|e| IoError::Custom(e.to_string()))
                },
                _ => Err(IoError::Custom("Unexpected command format".to_string())),
            })
            .collect()
    }

    fn extract_command_args(values: Vec<QueryIO>) -> Result<(String, Vec<String>), IoError> {
        let mut values = values.into_iter().flat_map(|v| v.unpack_single_entry::<String>());
        let command =
            values.next().ok_or(IoError::Custom("Unexpected command format".to_string()))?;
        Ok((command, values.collect()))
    }

    /// Analyze the command and arguments to create a `ClientRequest`
    fn parse_query(
        &self,
        session_req: Option<SessionRequest>,
        cmd: String,
        args: Vec<String>,
    ) -> anyhow::Result<ClientRequest> {
        let action = match (cmd.as_str(), args.as_slice()) {
            ("ping", []) => ClientAction::Ping,
            ("get", [key]) => ClientAction::Get { key: key.to_string() },
            ("get", [key, index]) => {
                ClientAction::IndexGet { key: key.to_string(), index: index.parse()? }
            },
            ("set", [key, value]) => {
                ClientAction::Set { key: key.to_string(), value: value.to_string() }
            },
            ("set", [key, value, px, expiry]) if px.to_lowercase() == "px" => {
                ClientAction::SetWithExpiry {
                    key: key.to_string(),
                    value: value.to_string(),
                    expiry: Self::extract_expiry(expiry)?,
                }
            },
            ("delete", [key]) => ClientAction::Delete { key: key.to_string() },
            ("echo", [value]) => ClientAction::Echo(value.to_string()),
            ("config", [key, value]) => {
                ClientAction::Config { key: key.to_string(), value: value.to_string() }
            },

            ("keys", [var]) if !var.is_empty() => {
                if var == "*" {
                    ClientAction::Keys { pattern: None }
                } else {
                    ClientAction::Keys { pattern: Some(var.to_string()) }
                }
            },
            ("save", []) => ClientAction::Save,
            ("info", [_unused_value]) => ClientAction::Info,
            ("cluster", val) if !val.is_empty() => match val[0].to_lowercase().as_str() {
                "info" => ClientAction::ClusterInfo,
                "nodes" => ClientAction::ClusterNodes,
                "forget" => {
                    ClientAction::ClusterForget(val.get(1).cloned().context("Must")?.into())
                },
                _ => return Err(anyhow::anyhow!("Invalid command")),
            },

            _ => return Err(anyhow::anyhow!("Invalid command")),
        };

        Ok(ClientRequest { action, session_req })
    }

    fn extract_expiry(expiry: &str) -> anyhow::Result<DateTime<Utc>> {
        let expiry = expiry.parse::<i64>().context("Invalid expiry")?;
        Ok(Utc::now() + chrono::Duration::milliseconds(expiry))
    }

    fn client_id(&self) -> uuid::Uuid {
        todo!()
    }
}
