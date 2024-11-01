use std::cmp::Reverse;

use tokio::sync::mpsc::Receiver;

use anyhow::Result;

use crate::make_smart_pointer;

use super::{command::TtlCommand, pr_queue};

async fn set_ttl_actor(mut recv: Receiver<TtlCommand>) -> Result<()> {
    while let Some(command) = recv.recv().await {
        let mut queue = pr_queue().write().await;
        let Some((expire_at, key)) = command.get_expiration() else {
            break;
        };
        queue.push((Reverse(expire_at), key));
    }
    Ok(())
}

pub fn run_set_ttl_actor() -> TtlSetter {
    let (tx, rx) = tokio::sync::mpsc::channel(100);
    tokio::spawn(set_ttl_actor(rx));
    TtlSetter(tx)
}

#[derive(Clone)]
pub struct TtlSetter(tokio::sync::mpsc::Sender<TtlCommand>);

impl TtlSetter {
    pub async fn set_ttl(&self, key: String, expiry: u64) {
        let _ = self
            .send(TtlCommand::Expiry {
                key,
                expiry: expiry,
            })
            .await;
    }
}

make_smart_pointer!(TtlSetter, tokio::sync::mpsc::Sender<TtlCommand>);
