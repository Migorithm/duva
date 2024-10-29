use std::str::FromStr;
pub enum ControllerCommand {
    Ping,
    Echo,
    Config,
    Get,
    Set,
    Delete,
}

impl FromStr for ControllerCommand {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ping" => Ok(ControllerCommand::Ping),
            "get" => Ok(ControllerCommand::Get),
            "set" => Ok(ControllerCommand::Set),
            "delete" => Ok(ControllerCommand::Delete),
            "echo" => Ok(ControllerCommand::Echo),
            "config" => Ok(ControllerCommand::Config),
            _ => Err(anyhow::anyhow!("Invalid command")),
        }
    }
}
