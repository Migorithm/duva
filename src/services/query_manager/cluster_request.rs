use std::str::FromStr;
pub enum ClusterRequest {
    Ping,
}

impl FromStr for ClusterRequest {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ping" => Ok(ClusterRequest::Ping),
            _ => Err(anyhow::anyhow!("Invalid command")),
        }
    }
}
