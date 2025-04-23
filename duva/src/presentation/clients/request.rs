use crate::domains::{
    append_only_files::WriteRequest, cluster_actors::session::SessionRequest,
    peers::identifier::PeerIdentifier, query_parsers::QueryIO,
};
use anyhow::Context;
use chrono::{DateTime, Utc};

#[derive(Clone, Debug)]
pub enum ClientAction {
    Ping,
    Echo(String),
    Config { key: String, value: String },
    Get { key: String },
    IndexGet { key: String, index: u64 },
    Set { key: String, value: String },
    SetWithExpiry { key: String, value: String, expiry: DateTime<Utc> },
    Keys { pattern: Option<String> },
    Delete { keys: Vec<String> },
    Save,
    Info,
    ClusterInfo,
    ClusterNodes,
    ClusterForget(PeerIdentifier),
    ReplicaOf(PeerIdentifier),
    Exists { keys: Vec<String> },
    Role,
    Incr { key: String },
    Decr { key: String },
    Ttl { key: String },
}

impl ClientAction {
    pub fn to_write_request(&self) -> Option<WriteRequest> {
        match self {
            ClientAction::Set { key, value } => {
                Some(WriteRequest::Set { key: key.clone(), value: value.clone() })
            },
            ClientAction::SetWithExpiry { key, value, expiry } => {
                let expires_at = expiry.timestamp_millis() as u64;

                Some(WriteRequest::SetWithExpiry {
                    key: key.clone(),
                    value: value.clone(),
                    expires_at,
                })
            },
            ClientAction::Delete { keys } => Some(WriteRequest::Delete { keys: keys.clone() }),
            _ => None,
        }
    }
    pub(crate) fn delta(&self) -> i64 {
        match self {
            ClientAction::Incr { .. } => 1,
            ClientAction::Decr { .. } => -1,
            _ => 0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClientRequest {
    pub(crate) action: ClientAction,
    pub(crate) session_req: Option<SessionRequest>,
}

impl ClientRequest {
    pub fn from_user_input(
        value: Vec<QueryIO>,
        session_req: Option<SessionRequest>,
    ) -> anyhow::Result<Self> {
        let mut values = value.into_iter().flat_map(|v| v.unpack_single_entry::<String>());
        let command = values.next().ok_or(anyhow::anyhow!("Unexpected command format"))?;
        let (command, args) = (command, values.collect::<Vec<_>>());

        Ok(ClientRequest {
            action: extract_action(&command, &args.iter().map(|s| s.as_str()).collect::<Vec<_>>())
                .map_err(|e| anyhow::anyhow!(e))?,
            session_req,
        })
    }
}

pub fn extract_action(action: &str, args: &[&str]) -> anyhow::Result<ClientAction> {
    // Check for invalid characters in command parts
    // Command-specific validation

    match action.to_uppercase().as_str() {
        "SET" => {
            if !(args.len() == 2 || (args.len() == 4 && args[2].eq_ignore_ascii_case("PX"))) {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for 'set' command"
                ));
            }
            if args.len() == 2 {
                return Ok(ClientAction::Set {
                    key: args[0].to_string(),
                    value: args[1].to_string(),
                });
            }
            Ok(ClientAction::SetWithExpiry {
                key: args[0].to_string(),
                value: args[1].to_string(),
                expiry: extract_expiry(args[3])?,
            })
        },

        "GET" => {
            if args.len() == 1 {
                Ok(ClientAction::Get { key: args[0].to_string() })
            } else if args.len() == 2 {
                Ok(ClientAction::IndexGet { key: args[0].to_string(), index: args[1].parse()? })
            } else {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for 'get' command"
                ));
            }
        },

        "KEYS" => {
            if args.len() != 1 {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for 'keys' command"
                ));
            }
            if args[0] == "*" {
                Ok(ClientAction::Keys { pattern: None })
            } else {
                Ok(ClientAction::Keys { pattern: Some(args[0].to_string()) })
            }
        },
        "DEL" => {
            if args.is_empty() {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for 'exists' command"
                ));
            }
            Ok(ClientAction::Delete { keys: args.iter().map(|s| s.to_string()).collect() })
        },
        "EXISTS" => {
            if args.is_empty() {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for 'exists' command"
                ));
            }
            Ok(ClientAction::Exists { keys: args.iter().map(|s| s.to_string()).collect() })
        },

        "PING" => {
            if !args.is_empty() {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for 'ping' command"
                ));
            }
            Ok(ClientAction::Ping)
        },
        "ECHO" => {
            if args.len() != 1 {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for 'echo' command"
                ));
            }
            Ok(ClientAction::Echo(args[0].to_string()))
        },
        "INFO" => {
            if args.is_empty() {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for 'info' command"
                ));
            }
            Ok(ClientAction::Info)
        },

        "CLUSTER" => {
            if args.is_empty() {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for 'cluster' command"
                ));
            }

            match args[0].to_uppercase().as_str() {
                "NODES" => Ok(ClientAction::ClusterNodes),
                "INFO" => Ok(ClientAction::ClusterInfo),
                "FORGET" => {
                    if args.len() != 2 {
                        return Err(anyhow::anyhow!(
                            "(error) ERR wrong number of arguments for 'cluster forget' command"
                        ));
                    }
                    Ok(ClientAction::ClusterForget(args[1].to_string().into()))
                },
                _ => Err(anyhow::anyhow!("(error) ERR unknown subcommand")),
            }
        },
        "REPLICAOF" => {
            if args.len() != 2 {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for 'replicaof' command"
                ));
            }
            Ok(ClientAction::ReplicaOf(PeerIdentifier::new(args[0], args[1].parse()?)))
        },
        "ROLE" => {
            if !args.is_empty() {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of argument for 'role' command"
                ));
            }
            Ok(ClientAction::Role)
        },
        "CONFIG" => {
            if args.len() != 2 {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for 'config' command"
                ));
            }
            Ok(ClientAction::Config { key: args[0].to_string(), value: args[1].to_string() })
        },
        "SAVE" => {
            if !args.is_empty() {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for 'save' command"
                ));
            }
            Ok(ClientAction::Save)
        },
        "INCR" => {
            if args.len() != 1 {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for 'incr' command"
                ));
            }
            Ok(ClientAction::Incr { key: args[0].to_string() })
        },
        "DECR" => {
            if args.len() != 1 {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for 'decr' command"
                ));
            }
            Ok(ClientAction::Decr { key: args[0].to_string() })
        },
        "TTL" => {
            if args.len() != 1 {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for 'ttl' command"
                ));
            }
            Ok(ClientAction::Ttl { key: args[0].to_string() })
        },
        // Add other commands as needed
        unknown_cmd => Err(anyhow::anyhow!(
            "(error) ERR unknown command '{unknown_cmd}', with args beginning with {}",
            args.iter().map(|s| format!("'{s}'")).collect::<Vec<_>>().join(" ")
        )),
    }
}

pub fn extract_expiry(expiry: &str) -> anyhow::Result<DateTime<Utc>> {
    let expiry = expiry.parse::<i64>().context("Invalid expiry")?;
    Ok(Utc::now() + chrono::Duration::milliseconds(expiry))
}
