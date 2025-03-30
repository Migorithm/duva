use anyhow::Context;
use chrono::{DateTime, Utc};

use crate::domains::cluster_actors::session::SessionRequest;

use super::request::{ClientAction, ClientRequest};

/// Analyze the command and arguments to create a `ClientRequest`
pub fn parse_query(
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
                expiry: extract_expiry(expiry)?,
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
            "forget" => ClientAction::ClusterForget(val.get(1).cloned().context("Must")?.into()),
            _ => return Err(anyhow::anyhow!("Invalid command")),
        },

        _ => return Err(anyhow::anyhow!("Invalid command")),
    };

    Ok(ClientRequest { action, session_req })
}

pub fn extract_expiry(expiry: &str) -> anyhow::Result<DateTime<Utc>> {
    let expiry = expiry.parse::<i64>().context("Invalid expiry")?;
    Ok(Utc::now() + chrono::Duration::milliseconds(expiry))
}
