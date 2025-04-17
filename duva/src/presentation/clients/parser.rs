use anyhow::Context;
use chrono::{DateTime, Utc};

use super::request::{ClientAction, ClientRequest};
use crate::domains::cluster_actors::session::SessionRequest;
use crate::prelude::PeerIdentifier;

/// Analyze the command and arguments to create a `ClientRequest`
pub fn parse_query(
    session_req: Option<SessionRequest>,
    cmd: String,
    args: Vec<String>,
) -> anyhow::Result<ClientRequest> {
    let action = extract_client_action(&cmd, &args.iter().map(|s| s.as_str()).collect::<Vec<_>>())
        .with_context(|| format!("Failed to parse command: {cmd}"))?;
    Ok(ClientRequest { action, session_req })
}

pub fn extract_client_action(action: &str, args: &[&str]) -> anyhow::Result<ClientAction> {
    // Check for invalid characters in command parts
    // Command-specific validation
    match action.to_uppercase().as_str() {
        "SET" => {
            if !(args.len() == 2 || args.len() == 4) {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for 'set' command"
                ));
            }
            if args.len() == 4 && args[2].to_uppercase() == "EX" {
                return Err(anyhow::anyhow!("(error) ERR syntax error"));
            }
            if args.len() == 2 {
                Ok(ClientAction::Set { key: args[0].to_string(), value: args[1].to_string() })
            } else {
                if args[2].to_lowercase() == "px" {
                    Ok(ClientAction::SetWithExpiry {
                        key: args[0].to_string(),
                        value: args[1].to_string(),
                        expiry: extract_expiry(&args[3])?,
                    })
                } else {
                    return Err(anyhow::anyhow!("(error) ERR syntax error"));
                }
            }
        },
        "GET" => {
            if args.len() != 1 {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for 'get' command"
                ));
            }
            Ok(ClientAction::Get { key: args[0].to_string() })
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
            Ok(ClientAction::Delete { keys: args.into_iter().map(|s| s.to_string()).collect() })
        },
        "EXISTS" => {
            if args.is_empty() {
                return Err(anyhow::anyhow!(
                    "(error) ERR wrong number of arguments for 'exists' command"
                ));
            }
            Ok(ClientAction::Exists { keys: args.into_iter().map(|s| s.to_string()).collect() })
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
                _ => return Err(anyhow::anyhow!("(error) ERR unknown subcommand")),
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
        // Add other commands as needed
        unknown_cmd => Err(anyhow::anyhow!(
            "(error) ERR unknown command '{unknown_cmd}', with args beginning with {}",
            args.into_iter().map(|s| format!("'{s}'")).collect::<Vec<_>>().join(" ")
        )),
    }
}

pub fn extract_expiry(expiry: &str) -> anyhow::Result<DateTime<Utc>> {
    let expiry = expiry.parse::<i64>().context("Invalid expiry")?;
    Ok(Utc::now() + chrono::Duration::milliseconds(expiry))
}
