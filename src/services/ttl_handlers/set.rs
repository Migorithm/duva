use std::{
    cmp::Reverse,
    time::{Duration, SystemTime},
};

use tokio::sync::mpsc::Receiver;

use crate::services::query_manager::value::TtlCommand;
use anyhow::Result;

use super::pr_queue;

pub async fn set_ttl_actor(mut recv: Receiver<TtlCommand>) -> Result<()> {
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
