use std::{
    cmp::Reverse,
    time::{Duration, SystemTime},
};

use tokio::sync::mpsc::Receiver;

use crate::make_smart_pointer;

use super::ttl_queue;

pub struct TtlSetActor {
    pub inbox: Receiver<TtlCommand>,
}
impl TtlSetActor {
    pub fn run() -> TtlInbox {
        let (tx, inbox) = tokio::sync::mpsc::channel(100);
        tokio::spawn(Self { inbox }.handle());
        TtlInbox(tx)
    }

    async fn handle(mut self) {
        while let Some(command) = self.inbox.recv().await {
            let mut queue = ttl_queue().write().await;
            let Some((expire_at, key)) = command.get_expiration() else {
                break;
            };
            queue.push((Reverse(expire_at), key));
        }
    }
}

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

#[derive(Clone)]
pub struct TtlInbox(tokio::sync::mpsc::Sender<TtlCommand>);

impl TtlInbox {
    pub async fn set_ttl(&self, key: String, expiry: u64) {
        let _ = self
            .send(TtlCommand::Expiry {
                key,
                expiry: expiry,
            })
            .await;
    }
}

make_smart_pointer!(TtlInbox, tokio::sync::mpsc::Sender<TtlCommand>);
