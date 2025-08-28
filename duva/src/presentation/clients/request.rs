use crate::domains::{
    cluster_actors::{LazyOption, SessionRequest},
    operation_logs::LogEntry,
    peers::identifier::{PeerIdentifier, TPeerAddress},
};
use anyhow::Context;
use chrono::Utc;
use std::str::FromStr;

#[derive(Clone, Debug, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub enum ClientAction {
    Ping,
    Echo(String),
    Config { key: String, value: String },
    Get { key: String },
    MGet { keys: Vec<String> },
    IndexGet { key: String, index: u64 },
    Keys { pattern: Option<String> },
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
    LLen { key: String },
    LRange { key: String, start: isize, end: isize },
    LIndex { key: String, index: isize },
    Mutating(LogEntry),
}

impl ClientAction {
    pub fn to_write_request(&self) -> LogEntry {
        if let Self::Mutating(log_entry) = self {
            return log_entry.clone();
        }
        //TODO will revisit
        panic!()
    }
}

impl From<LogEntry> for ClientAction {
    fn from(value: LogEntry) -> Self {
        Self::Mutating(value)
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

    let entry: ClientAction = match cmd.as_str() {
        | "SET" => {
            if !(args.len() == 2 || (args.len() == 4 && args[2].eq_ignore_ascii_case("PX"))) {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for 'set' command"
                ));
            }
            if args.len() == 2 {
                LogEntry::Set {
                    key: args[0].to_string(),
                    value: args[1].to_string(),
                    expires_at: None,
                }
                .into()
            } else {
                LogEntry::Set {
                    key: args[0].to_string(),
                    value: args[1].to_string(),
                    expires_at: Some(extract_expiry(args[3])?),
                }
                .into()
            }
        },
        | "DEL" => {
            require_non_empty_args()?;
            LogEntry::Delete { keys: args.iter().map(|s| s.to_string()).collect() }.into()
        },

        | "APPEND" => {
            require_exact_args(2)?;
            LogEntry::Append { key: args[0].to_string(), value: args[1].to_string() }.into()
        },

        | "INCR" => {
            require_exact_args(1)?;
            LogEntry::IncrBy { key: args[0].to_string(), delta: 1 }.into()
        },
        | "DECR" => {
            require_exact_args(1)?;
            LogEntry::DecrBy { key: args[0].to_string(), delta: 1 }.into()
        },
        | "INCRBY" => {
            require_exact_args(2)?;
            LogEntry::IncrBy { key: args[0].to_string(), delta: args[1].parse()? }.into()
        },
        | "DECRBY" => {
            require_exact_args(2)?;
            LogEntry::DecrBy { key: args[0].to_string(), delta: args[1].parse()? }.into()
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
                return Ok(LogEntry::LPush { key, value: values }.into());
            }
            LogEntry::RPush { key, value: values }.into()
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
                LogEntry::LPushX { key, value: values }.into()
            } else {
                LogEntry::RPushX { key, value: values }.into()
            }
        },
        | "LPOP" | "RPOP" => {
            require_non_empty_args()?;
            let key = args[0].to_string();
            let count = args.get(1).and_then(|s| s.parse::<usize>().ok()).unwrap_or(1);

            if action.to_uppercase() == "LPOP" {
                LogEntry::LPop { key, count }.into()
            } else {
                LogEntry::RPop {
                    key,
                    count: args.get(1).and_then(|s| s.parse::<usize>().ok()).unwrap_or(1),
                }
                .into()
            }
        },
        | "LTRIM" => {
            require_exact_args(3)?;
            LogEntry::LTrim {
                key: args[0].to_string(),
                start: args[1].parse::<isize>()?,
                end: args[2].parse::<isize>()?,
            }
            .into()
        },

        | "LSET" => {
            require_exact_args(3)?;
            LogEntry::LSet {
                key: args[0].to_string(),
                index: args[1].parse::<isize>()?,
                value: args[2].to_string(),
            }
            .into()
        },

        | "GET" => {
            if args.len() == 1 {
                ClientAction::Get { key: args[0].to_string() }
            } else if args.len() == 2 {
                ClientAction::IndexGet { key: args[0].to_string(), index: args[1].parse()? }
            } else {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for 'get' command"
                ));
            }
        },

        | "KEYS" => {
            require_exact_args(1)?;

            if args[0] == "*" {
                ClientAction::Keys { pattern: None }
            } else {
                ClientAction::Keys { pattern: Some(args[0].to_string()) }
            }
        },

        | "EXISTS" => {
            require_non_empty_args()?;
            ClientAction::Exists { keys: args.iter().map(|s| s.to_string()).collect() }
        },

        | "PING" => {
            require_exact_args(0)?;
            ClientAction::Ping
        },
        | "ECHO" => {
            require_exact_args(1)?;
            ClientAction::Echo(args[0].to_string())
        },
        | "INFO" => {
            require_non_empty_args()?;
            ClientAction::Info
        },

        | "CLUSTER" => {
            require_non_empty_args()?;
            match args[0].to_uppercase().as_str() {
                | "NODES" => ClientAction::ClusterNodes,
                | "INFO" => ClientAction::ClusterInfo,
                | "FORGET" => {
                    if args.len() != 2 {
                        return Err(anyhow::anyhow!(
                            "(error) ERR wrong number of arguments for 'cluster forget' command"
                        ));
                    }
                    ClientAction::ClusterForget(PeerIdentifier(args[1].bind_addr()?))
                },
                | "MEET" => {
                    if args.len() == 2 {
                        ClientAction::ClusterMeet(
                            PeerIdentifier(args[1].bind_addr()?),
                            LazyOption::Lazy,
                        )
                    } else if args.len() == 3 {
                        // args[2].parse()? should be either lazy or eager
                        let lazy_option:LazyOption =FromStr::from_str(args[2]).context(
                            "(error) ERR wrong arguments for 'cluster meet' command, expected 'lazy' or 'eager'"
                        )?;

                        ClientAction::ClusterMeet(PeerIdentifier(args[1].bind_addr()?), lazy_option)
                    } else {
                        return Err(anyhow::anyhow!(
                            "(error) ERR wrong number of arguments for 'cluster meet' command"
                        ));
                    }
                },
                | "RESHARD" => ClientAction::ClusterReshard,
                | _ => {
                    return Err(anyhow::anyhow!("(error) ERR unknown subcommand"));
                },
            }
        },
        | "REPLICAOF" => {
            require_exact_args(2)?;
            ClientAction::ReplicaOf(PeerIdentifier::new(args[0], args[1].parse()?))
        },
        | "ROLE" => {
            require_exact_args(0)?;
            ClientAction::Role
        },
        | "CONFIG" => {
            require_exact_args(2)?;
            ClientAction::Config { key: args[0].to_string(), value: args[1].to_string() }
        },
        | "SAVE" => {
            require_exact_args(0)?;
            ClientAction::Save
        },

        | "TTL" => {
            require_exact_args(1)?;
            ClientAction::Ttl { key: args[0].to_string() }
        },

        | "MGET" => {
            require_non_empty_args()?;
            ClientAction::MGet { keys: args.iter().map(|s| s.to_string()).collect() }
        },

        | "LLEN" => {
            require_exact_args(1)?;
            ClientAction::LLen { key: args[0].to_string() }
        },

        | "LRANGE" => {
            require_exact_args(3)?;
            ClientAction::LRange {
                key: args[0].to_string(),
                start: args[1].parse::<isize>()?,
                end: args[2].parse::<isize>()?,
            }
        },
        | "LINDEX" => {
            require_exact_args(2)?;
            ClientAction::LIndex { key: args[0].to_string(), index: args[1].parse::<isize>()? }
        },

        // Add other commands as needed
        | unknown_cmd => {
            return Err(anyhow::anyhow!(
                "(error) ERR unknown command '{unknown_cmd}', with args beginning with {}",
                args.iter().map(|s| format!("'{s}'")).collect::<Vec<_>>().join(" ")
            ));
        },
    };

    Ok(entry)
}

pub fn extract_expiry(expiry: &str) -> anyhow::Result<i64> {
    Ok((Utc::now() + chrono::Duration::milliseconds(expiry.parse::<i64>()?)).timestamp_millis())
}

#[derive(Clone, Debug)]
pub struct ClientRequest {
    pub(crate) action: ClientAction,
    pub(crate) session_req: SessionRequest,
}
