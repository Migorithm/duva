pub enum ReplicationRequest {
    Ping,
    ReplConf,
    Psync,
}

impl TryFrom<String> for ReplicationRequest {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "ping" => Ok(ReplicationRequest::Ping),
            "replconf" => Ok(ReplicationRequest::ReplConf),
            "psync" => Ok(ReplicationRequest::Psync),

            _ => Err(anyhow::anyhow!("Invalid command")),
        }
    }
}
