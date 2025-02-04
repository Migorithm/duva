use thiserror::Error;

#[derive(Error, Debug)]
pub enum IoError {
    #[error("ConnectionRefused")]
    ConnectionRefused,
    #[error("ConnectionReset")]
    ConnectionReset,
    #[error("ConnectionAborted")]
    ConnectionAborted,
    #[error("NotConnected")]
    NotConnected,
    #[error("BrokenPipe")]
    BrokenPipe,
    #[error("TimedOut")]
    TimedOut,
    #[error("Unknown")]
    Unknown,
}

impl IoError {
    pub fn should_break(self) -> bool {
        matches!(
            self,
            IoError::ConnectionRefused
                | IoError::ConnectionReset
                | IoError::ConnectionAborted
                | IoError::NotConnected
                | IoError::BrokenPipe
                | IoError::TimedOut
        )
    }
}
