use anyhow::Context;

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

            invalid_value => {
                eprintln!("Invalid command,{}", invalid_value);
                Err(anyhow::anyhow!("Invalid command"))
            }
        }
    }
}

pub enum HandShakeResponse {
    PONG,
    OK,
    FULLRESYNC { repl_id: String, offset: i64 },
}

impl TryFrom<String> for HandShakeResponse {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "pong" => Ok(HandShakeResponse::PONG),
            "ok" => Ok(HandShakeResponse::OK),

            var if var.starts_with("fullresync") => {
                let mut iter = var.split_whitespace();
                let _ = iter.next();
                let repl_id = iter
                    .next()
                    .context("replication_id must be given")?
                    .to_string();
                let offset = iter
                    .next()
                    .context("offset must be given")?
                    .parse::<i64>()?;
                Ok(HandShakeResponse::FULLRESYNC { repl_id, offset })
            }

            invalid_value => {
                eprintln!("Invalid command,{}", invalid_value);
                Err(anyhow::anyhow!("Invalid command"))
            }
        }
    }
}
