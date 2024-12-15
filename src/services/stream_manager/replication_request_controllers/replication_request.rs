pub enum ReplicationRequest {}

pub enum HandShakeRequest {
    Ping,
    ReplConf,
    Psync,
}

impl TryFrom<String> for ReplicationRequest {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            _ => Err(anyhow::anyhow!("Invalid command")),
        }
    }
}

impl TryFrom<String> for HandShakeRequest {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "ping" => Ok(HandShakeRequest::Ping),
            "replconf" => Ok(HandShakeRequest::ReplConf),
            "psync" => Ok(HandShakeRequest::Psync),

            _ => Err(anyhow::anyhow!("Invalid command")),
        }
    }
}
