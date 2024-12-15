#[derive(Debug)]
pub enum IoError {
    ConnectionRefused,
    ConnectionReset,
    ConnectionAborted,
    NotConnected,
    BrokenPipe,
    TimedOut,
    Unknown,
}

impl IoError {
    pub fn should_break(self) -> bool {
        match self {
            IoError::ConnectionRefused
            | IoError::ConnectionReset
            | IoError::ConnectionAborted
            | IoError::NotConnected
            | IoError::BrokenPipe
            | IoError::TimedOut => true,
            _ => false,
        }
    }
}

impl From<IoError> for anyhow::Error {
    fn from(value: IoError) -> Self {
        match value {
            IoError::ConnectionRefused => anyhow::anyhow!("ConnectionRefused"),
            IoError::ConnectionReset => anyhow::anyhow!("ConnectionReset"),
            IoError::ConnectionAborted => anyhow::anyhow!("ConnectionAborted"),
            IoError::NotConnected => anyhow::anyhow!("NotConnected"),
            IoError::BrokenPipe => anyhow::anyhow!("BrokenPipe"),
            IoError::TimedOut => anyhow::anyhow!("TimedOut"),
            IoError::Unknown => anyhow::anyhow!("Unknown"),
        }
    }
}
