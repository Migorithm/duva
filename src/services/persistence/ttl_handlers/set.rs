use std::{
    cmp::Reverse,
    time::{Duration, SystemTime},
};

use tokio::sync::mpsc::Receiver;

use anyhow::Result;

use super::{command::TtlCommand, pr_queue};

async fn set_ttl_actor(mut recv: Receiver<TtlCommand>) -> Result<()> {
    while let Some(command) = recv.recv().await {
        let mut queue = pr_queue().write().await;
        let (expire_in_mills, key) = match command {
            TtlCommand::Expiry { expiry, key } => (expiry, key),
            TtlCommand::StopSentinel => break,
        };
        let expire_at = SystemTime::now() + Duration::from_millis(expire_in_mills);
        queue.push((Reverse(expire_at), key));
    }
    Ok(())
}

pub fn run_set_ttl_actor() -> tokio::sync::mpsc::Sender<TtlCommand> {
    let (tx, rx) = tokio::sync::mpsc::channel(100);
    tokio::spawn(set_ttl_actor(rx));
    tx
}
