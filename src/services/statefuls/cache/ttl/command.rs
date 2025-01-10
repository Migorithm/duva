use std::time::SystemTime;
use tokio::sync::oneshot;

pub enum TtlCommand {
    ScheduleTtl { expiry: SystemTime, key: String },
    Peek(oneshot::Sender<Option<String>>),
    StopSentinel,
}

impl TtlCommand {
    pub fn get_expiration(self) -> Option<(SystemTime, String)> {
        let (expire_in_mills, key) = match self {
            TtlCommand::ScheduleTtl { expiry, key } => (expiry, key),
            _ => return None,
        };

        Some((expire_in_mills, key))
    }
}
