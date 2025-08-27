use std::str::FromStr;

use crate::domains::{
    cluster_actors::{LazyOption, SessionRequest},
    operation_logs::WriteRequest,
    peers::identifier::{PeerIdentifier, TPeerAddress},
};
use anyhow::Context;
use chrono::Utc;

#[derive(Clone, Debug, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub enum ClientAction {
    Ping,
    Echo(String),
    Config { key: String, value: String },
    Get { key: String },
    MGet { keys: Vec<String> },
    IndexGet { key: String, index: u64 },
    Set { key: String, value: String },
    Append { key: String, value: String },
    SetWithExpiry { key: String, value: String, expires_at: i64 },
    Keys { pattern: Option<String> },
    Delete { keys: Vec<String> },
    Save,
    Info,
    ClusterInfo,
    ClusterNodes,
    ClusterForget(PeerIdentifier),
    ClusterReshard,
    ReplicaOf(PeerIdentifier),
    Exists { keys: Vec<String> },
    Role,
    Ttl { key: String },
    ClusterMeet(PeerIdentifier, LazyOption),
    IncrBy { key: String, delta: i64 },
    DecrBy { key: String, delta: i64 },
    LPush { key: String, value: Vec<String> },
    LPushX { key: String, value: Vec<String> },
    LPop { key: String, count: usize },
    RPush { key: String, value: Vec<String> },
    RPushX { key: String, value: Vec<String> },
    RPop { key: String, count: usize },
    LSet { key: String, index: isize, value: String },
    LTrim { key: String, start: isize, end: isize },
    LLen { key: String },
    LRange { key: String, start: isize, end: isize },
    LIndex { key: String, index: isize },
}

impl ClientAction {
    pub fn to_write_request(&self) -> Option<WriteRequest> {
        let write_req = match self {
            | ClientAction::Set { key, value } => {
                WriteRequest::Set { key: key.clone(), value: value.clone(), expires_at: None }
            },
            | ClientAction::SetWithExpiry { key, value, expires_at } => WriteRequest::Set {
                key: key.clone(),
                value: value.clone(),
                expires_at: Some(*expires_at),
            },
            | ClientAction::Append { key, value } => {
                WriteRequest::Append { key: key.clone(), value: value.clone() }
            },
            | ClientAction::Delete { keys } => WriteRequest::Delete { keys: keys.clone() },
            | ClientAction::IncrBy { key, delta } => {
                WriteRequest::IncrBy { key: key.clone(), delta: *delta }
            },
            | ClientAction::DecrBy { key, delta } => {
                WriteRequest::DecrBy { key: key.clone(), delta: *delta }
            },
            | ClientAction::LPush { key, value } => {
                WriteRequest::LPush { key: key.clone(), value: value.clone() }
            },
            | ClientAction::LPushX { key, value } => {
                WriteRequest::LPushX { key: key.clone(), value: value.clone() }
            },
            | ClientAction::LPop { key, count } => {
                WriteRequest::LPop { key: key.clone(), count: *count }
            },
            | ClientAction::RPush { key, value } => {
                WriteRequest::RPush { key: key.clone(), value: value.clone() }
            },
            | ClientAction::RPushX { key, value } => {
                WriteRequest::RPushX { key: key.clone(), value: value.clone() }
            },
            | ClientAction::RPop { key, count } => {
                WriteRequest::RPop { key: key.clone(), count: *count }
            },
            | ClientAction::LTrim { key, start, end } => {
                WriteRequest::LTrim { key: key.clone(), start: *start, end: *end }
            },
            | ClientAction::LSet { key, index, value } => {
                WriteRequest::LSet { key: key.clone(), index: *index, value: value.clone() }
            },

            | _ => return None,
        };
        Some(write_req)
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
        | "SET" => {
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
                expires_at: extract_expiry(args[3])?,
            })
        },

        | "APPEND" => {
            require_exact_args(2)?;
            Ok(ClientAction::Append { key: args[0].to_string(), value: args[1].to_string() })
        },

        | "GET" => {
            if args.len() == 1 {
                Ok(ClientAction::Get { key: args[0].to_string() })
            } else if args.len() == 2 {
                Ok(ClientAction::IndexGet { key: args[0].to_string(), index: args[1].parse()? })
            } else {
                Err(anyhow::anyhow!("(error) ERR wrong number of arguments for 'get' command"))
            }
        },

        | "KEYS" => {
            require_exact_args(1)?;

            if args[0] == "*" {
                Ok(ClientAction::Keys { pattern: None })
            } else {
                Ok(ClientAction::Keys { pattern: Some(args[0].to_string()) })
            }
        },
        | "DEL" => {
            require_non_empty_args()?;
            Ok(ClientAction::Delete { keys: args.iter().map(|s| s.to_string()).collect() })
        },
        | "EXISTS" => {
            require_non_empty_args()?;
            Ok(ClientAction::Exists { keys: args.iter().map(|s| s.to_string()).collect() })
        },

        | "PING" => {
            require_exact_args(0)?;
            Ok(ClientAction::Ping)
        },
        | "ECHO" => {
            require_exact_args(1)?;
            Ok(ClientAction::Echo(args[0].to_string()))
        },
        | "INFO" => {
            require_non_empty_args()?;
            Ok(ClientAction::Info)
        },

        | "CLUSTER" => {
            require_non_empty_args()?;
            match args[0].to_uppercase().as_str() {
                | "NODES" => Ok(ClientAction::ClusterNodes),
                | "INFO" => Ok(ClientAction::ClusterInfo),
                | "FORGET" => {
                    if args.len() != 2 {
                        return Err(anyhow::anyhow!(
                            "(error) ERR wrong number of arguments for 'cluster forget' command"
                        ));
                    }
                    Ok(ClientAction::ClusterForget(PeerIdentifier(args[1].bind_addr()?)))
                },
                | "MEET" => {
                    if args.len() == 2 {
                        return Ok(ClientAction::ClusterMeet(
                            PeerIdentifier(args[1].bind_addr()?),
                            LazyOption::Lazy,
                        ));
                    }
                    if args.len() == 3 {
                        // args[2].parse()? should be either lazy or eager
                        let lazy_option:LazyOption =FromStr::from_str(args[2]).context(
                            "(error) ERR wrong arguments for 'cluster meet' command, expected 'lazy' or 'eager'"
                        )?;

                        Ok(ClientAction::ClusterMeet(
                            PeerIdentifier(args[1].bind_addr()?),
                            lazy_option,
                        ))
                    } else {
                        Err(anyhow::anyhow!(
                            "(error) ERR wrong number of arguments for 'cluster meet' command"
                        ))
                    }
                },
                | "RESHARD" => Ok(ClientAction::ClusterReshard),
                | _ => Err(anyhow::anyhow!("(error) ERR unknown subcommand")),
            }
        },
        | "REPLICAOF" => {
            require_exact_args(2)?;
            Ok(ClientAction::ReplicaOf(PeerIdentifier::new(args[0], args[1].parse()?)))
        },
        | "ROLE" => {
            require_exact_args(0)?;
            Ok(ClientAction::Role)
        },
        | "CONFIG" => {
            require_exact_args(2)?;
            Ok(ClientAction::Config { key: args[0].to_string(), value: args[1].to_string() })
        },
        | "SAVE" => {
            require_exact_args(0)?;
            Ok(ClientAction::Save)
        },
        | "INCR" => {
            require_exact_args(1)?;
            Ok(ClientAction::IncrBy { key: args[0].to_string(), delta: 1 })
        },
        | "DECR" => {
            require_exact_args(1)?;
            Ok(ClientAction::DecrBy { key: args[0].to_string(), delta: 1 })
        },
        | "TTL" => {
            require_exact_args(1)?;
            Ok(ClientAction::Ttl { key: args[0].to_string() })
        },
        | "INCRBY" => {
            require_exact_args(2)?;
            Ok(ClientAction::IncrBy { key: args[0].to_string(), delta: args[1].parse()? })
        },
        | "DECRBY" => {
            require_exact_args(2)?;
            Ok(ClientAction::DecrBy { key: args[0].to_string(), delta: args[1].parse()? })
        },
        | "MGET" => {
            require_non_empty_args()?;
            Ok(ClientAction::MGet { keys: args.iter().map(|s| s.to_string()).collect() })
        },
        | "LPUSH" | "RPUSH" => {
            require_non_empty_args()?;

            let key = args[0].to_string();
            let values: Vec<String> = args[1..].iter().map(|s| s.to_string()).collect();
            if values.is_empty() {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for '{}' command",
                    action.to_uppercase()
                ));
            }
            if action.to_uppercase() == "LPUSH" {
                return Ok(ClientAction::LPush { key, value: values });
            }
            Ok(ClientAction::RPush { key, value: values })
        },
        | "LPUSHX" | "RPUSHX" => {
            require_non_empty_args()?;
            let key = args[0].to_string();
            let values: Vec<String> = args[1..].iter().map(|s| s.to_string()).collect();
            if values.is_empty() {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for '{}' command",
                    action.to_uppercase()
                ));
            }
            if action.to_uppercase() == "LPUSHX" {
                return Ok(ClientAction::LPushX { key, value: values });
            }
            Ok(ClientAction::RPushX { key, value: values })
        },
        | "LPOP" | "RPOP" => {
            require_non_empty_args()?;
            let key = args[0].to_string();
            let count = args.get(1).and_then(|s| s.parse::<usize>().ok()).unwrap_or(1);

            if action.to_uppercase() == "LPOP" {
                return Ok(ClientAction::LPop { key, count });
            }
            Ok(ClientAction::RPop {
                key,
                count: args.get(1).and_then(|s| s.parse::<usize>().ok()).unwrap_or(1),
            })
        },
        | "LTRIM" => {
            require_exact_args(3)?;
            Ok(ClientAction::LTrim {
                key: args[0].to_string(),
                start: args[1].parse::<isize>()?,
                end: args[2].parse::<isize>()?,
            })
        },
        | "LLEN" => {
            require_exact_args(1)?;
            Ok(ClientAction::LLen { key: args[0].to_string() })
        },

        | "LRANGE" => {
            require_exact_args(3)?;
            Ok(ClientAction::LRange {
                key: args[0].to_string(),
                start: args[1].parse::<isize>()?,
                end: args[2].parse::<isize>()?,
            })
        },
        | "LINDEX" => {
            require_exact_args(2)?;
            Ok(ClientAction::LIndex { key: args[0].to_string(), index: args[1].parse::<isize>()? })
        },
        | "LSET" => {
            require_exact_args(3)?;
            Ok(ClientAction::LSet {
                key: args[0].to_string(),
                index: args[1].parse::<isize>()?,
                value: args[2].to_string(),
            })
        },

        // Add other commands as needed
        | unknown_cmd => Err(anyhow::anyhow!(
            "(error) ERR unknown command '{unknown_cmd}', with args beginning with {}",
            args.iter().map(|s| format!("'{s}'")).collect::<Vec<_>>().join(" ")
        )),
    }
}

pub fn extract_expiry(expiry: &str) -> anyhow::Result<i64> {
    Ok((Utc::now() + chrono::Duration::milliseconds(expiry.parse::<i64>()?)).timestamp_millis())
}

#[derive(Clone, Debug)]
pub struct ClientRequest {
    pub(crate) action: ClientAction,
    pub(crate) session_req: SessionRequest,
}
