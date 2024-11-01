use std::str::FromStr;
pub enum UserRequest {
    Ping,
    Echo,
    Config,
    Get,
    Set,
    Keys,
    Delete,
}

impl FromStr for UserRequest {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ping" => Ok(UserRequest::Ping),
            "get" => Ok(UserRequest::Get),
            "set" => Ok(UserRequest::Set),
            "delete" => Ok(UserRequest::Delete),
            "echo" => Ok(UserRequest::Echo),
            "config" => Ok(UserRequest::Config),
            "keys" => Ok(UserRequest::Keys),
            _ => Err(anyhow::anyhow!("Invalid command")),
        }
    }
}
