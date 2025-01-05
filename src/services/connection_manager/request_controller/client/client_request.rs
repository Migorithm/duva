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

impl TryFrom<String> for ClientRequest {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
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
