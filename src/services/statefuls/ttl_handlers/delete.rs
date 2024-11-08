use super::pr_queue;
use crate::services::statefuls::{command::PersistCommand, router::CacheDbMessageRouter};
use anyhow::Result;
use std::{
    cmp::Reverse,
    time::{Duration, SystemTime},
};
use tokio::time::interval;

async fn delete_actor(persistence_router: CacheDbMessageRouter) -> Result<()> {
    //TODO interval period should be configurable
    let mut cleanup_interval = interval(Duration::from_millis(1));
    loop {
        cleanup_interval.tick().await;
        let mut queue = pr_queue().write().await;
        while let Some((Reverse(expiry), key)) = queue.peek() {
            if expiry <= &SystemTime::now() {
                let shard_key = persistence_router.take_shard_key_from_str(key);
                let db = &persistence_router[shard_key];
                db.send(PersistCommand::Delete(key.clone())).await?;

                queue.pop();
            } else {
                break;
            }
        }
    }
}

pub fn run_delete_expired_key_actor(senders_to_persistent_actors: CacheDbMessageRouter) {
    tokio::spawn(async move {
        delete_actor(senders_to_persistent_actors).await.unwrap();
    });
}
