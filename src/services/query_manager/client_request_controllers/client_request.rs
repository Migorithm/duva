use std::str::FromStr;
pub enum ClientRequest {
    Ping,
    Echo,
    Config,
    Get,
    Set,
    Keys,
    Delete,
    Save,
    Info,
}

impl FromStr for ClientRequest {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ping" => Ok(ClientRequest::Ping),
            "get" => Ok(ClientRequest::Get),
            "set" => Ok(ClientRequest::Set),
            "delete" => Ok(ClientRequest::Delete),
            "echo" => Ok(ClientRequest::Echo),
            "config" => Ok(ClientRequest::Config),
            "keys" => Ok(ClientRequest::Keys),
            "save" => Ok(ClientRequest::Save),
            "info" => Ok(ClientRequest::Info),
            _ => Err(anyhow::anyhow!("Invalid command")),
        }
    }
}
