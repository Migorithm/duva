use std::time::{Duration, SystemTime};

//TODO move to a separate file
pub enum TtlCommand {
    Expiry { expiry: u64, key: String },
    StopSentinel,
}

impl TtlCommand {
    pub fn get_expiration(self) -> Option<(SystemTime, String)> {
        let (expire_in_mills, key) = match self {
            TtlCommand::Expiry { expiry, key } => (expiry, key),
            TtlCommand::StopSentinel => return None,
        };
        let expire_at = SystemTime::now() + Duration::from_millis(expire_in_mills);
        Some((expire_at, key))
    }
}
