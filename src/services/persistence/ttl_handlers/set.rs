use std::cmp::Reverse;

use tokio::sync::mpsc::Receiver;

use anyhow::Result;

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

pub fn run_set_ttl_actor() -> tokio::sync::mpsc::Sender<TtlCommand> {
    let (tx, rx) = tokio::sync::mpsc::channel(100);
    tokio::spawn(set_ttl_actor(rx));
    tx
}
