use crate::domains::{
    cluster_actors::session::SessionRequest, operation_logs::WriteRequest,
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
    ClusterMeet(PeerIdentifier),
}

impl ClientAction {
    pub fn to_write_request(&self) -> Option<WriteRequest> {
        match self {
            ClientAction::Set { key, value } => {
                Some(WriteRequest::Set { key: key.clone(), value: value.clone(), expires_at: None })
            },
            ClientAction::SetWithExpiry { key, value, expiry } => {
                let expires_at = expiry.timestamp_millis() as u64;

                Some(WriteRequest::Set {
                    key: key.clone(),
                    value: value.clone(),
                    expires_at: Some(expires_at),
                })
            },
            ClientAction::Delete { keys } => Some(WriteRequest::Delete { keys: keys.clone() }),
            ClientAction::Incr { key } => Some(WriteRequest::Incr { key: key.clone(), delta: 1 }),
            ClientAction::Decr { key } => Some(WriteRequest::Decr { key: key.clone(), delta: 1 }),
            _ => None,
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
    let cmd = action.to_uppercase();

    let require_exact_args = |count: usize| {
        if args.len() != count {
            Err(anyhow::anyhow!(
                "(error) ERR wrong number of arguments for '{}' command",
                cmd.to_lowercase()
            ))
        } else {
            Ok(())
        }
    };
    let require_non_empty_args = || {
        if args.is_empty() {
            Err(anyhow::anyhow!(
                "(error) ERR wrong number of arguments for '{}' command",
                cmd.to_lowercase()
            ))
        } else {
            Ok(())
        }
    };

    match cmd.as_str() {
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
            require_exact_args(1)?;

            if args[0] == "*" {
                Ok(ClientAction::Keys { pattern: None })
            } else {
                Ok(ClientAction::Keys { pattern: Some(args[0].to_string()) })
            }
        },
        "DEL" => {
            require_non_empty_args()?;
            Ok(ClientAction::Delete { keys: args.iter().map(|s| s.to_string()).collect() })
        },
        "EXISTS" => {
            require_non_empty_args()?;
            Ok(ClientAction::Exists { keys: args.iter().map(|s| s.to_string()).collect() })
        },

        "PING" => {
            require_exact_args(0)?;
            Ok(ClientAction::Ping)
        },
        "ECHO" => {
            require_exact_args(1)?;
            Ok(ClientAction::Echo(args[0].to_string()))
        },
        "INFO" => {
            require_non_empty_args()?;
            Ok(ClientAction::Info)
        },

        "CLUSTER" => {
            require_non_empty_args()?;
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
                "MEET" => {
                    if args.len() != 2 {
                        return Err(anyhow::anyhow!(
                            "(error) ERR wrong number of arguments for 'cluster meet' command"
                        ));
                    }
                    Ok(ClientAction::ClusterMeet(args[1].to_string().into()))
                },
                _ => Err(anyhow::anyhow!("(error) ERR unknown subcommand")),
            }
        },
        "REPLICAOF" => {
            require_exact_args(2)?;
            Ok(ClientAction::ReplicaOf(PeerIdentifier::new(args[0], args[1].parse()?)))
        },
        "ROLE" => {
            require_exact_args(0)?;
            Ok(ClientAction::Role)
        },
        "CONFIG" => {
            require_exact_args(2)?;
            Ok(ClientAction::Config { key: args[0].to_string(), value: args[1].to_string() })
        },
        "SAVE" => {
            require_exact_args(0)?;
            Ok(ClientAction::Save)
        },
        "INCR" => {
            require_exact_args(1)?;
            Ok(ClientAction::Incr { key: args[0].to_string() })
        },
        "DECR" => {
            require_exact_args(1)?;
            Ok(ClientAction::Decr { key: args[0].to_string() })
        },
        "TTL" => {
            require_exact_args(1)?;
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
