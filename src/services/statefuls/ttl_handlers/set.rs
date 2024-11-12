use std::cmp::Reverse;

use tokio::sync::mpsc::Receiver;

use crate::make_smart_pointer;

use super::{command::TtlCommand, pr_queue};

pub struct TtlSetActor {
    pub inbox: Receiver<TtlCommand>,
}
impl TtlSetActor {
    pub fn run() -> TtlHandler {
        let (tx, inbox) = tokio::sync::mpsc::channel(100);
        tokio::spawn(Self { inbox }.handle());
        TtlHandler(tx)
    }

    async fn handle(mut self) {
        while let Some(command) = self.inbox.recv().await {
            let mut queue = pr_queue().write().await;
            let Some((expire_at, key)) = command.get_expiration() else {
                break;
            };
            queue.push((Reverse(expire_at), key));
        }
    }
}

#[derive(Clone)]
pub struct TtlHandler(tokio::sync::mpsc::Sender<TtlCommand>);

impl TtlHandler {
    pub async fn set_ttl(&self, key: String, expiry: u64) {
        let _ = self
            .send(TtlCommand::Expiry {
                key,
                expiry: expiry,
            })
            .await;
    }
}

make_smart_pointer!(TtlHandler, tokio::sync::mpsc::Sender<TtlCommand>);
