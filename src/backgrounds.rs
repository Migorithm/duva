use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    sync::OnceLock,
    time::{Duration, SystemTime},
};

use anyhow::Result;
use tokio::{
    sync::{mpsc::Receiver, RwLock},
    time::interval,
};

use crate::services::{interface::Database, parser::value::TtlCommand};

// actor to expire key after a certain duration
// argument : channel
static PRIORITY_QUEUE: OnceLock<RwLock<BinaryHeap<(Reverse<SystemTime>, String)>>> =
    OnceLock::new();

fn pr_queue() -> &'static RwLock<BinaryHeap<(Reverse<SystemTime>, String)>> {
    PRIORITY_QUEUE.get_or_init(|| RwLock::new(BinaryHeap::new()))
}

pub async fn delete_actor(db: impl Database) -> Result<()> {
    let mut cleanup_interval = interval(Duration::from_millis(1));
    loop {
        cleanup_interval.tick().await;
        let mut queue = pr_queue().write().await;
        while let Some((Reverse(expiry), key)) = queue.peek() {
            if expiry <= &SystemTime::now() {
                db.delete(key).await;
                queue.pop();
            } else {
                break;
            }
        }
    }
}

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
