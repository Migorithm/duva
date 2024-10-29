use std::{
    cmp::Reverse,
    time::{Duration, SystemTime},
};

use crate::services::persistence::{hasher::get_hash, router::PersistenceRouter, PersistEnum};
use anyhow::Result;
use tokio::time::interval;

use super::pr_queue;
async fn delete_actor(persistence_router: PersistenceRouter) -> Result<()> {
    //TODO interval period should be configurable
    let mut cleanup_interval = interval(Duration::from_millis(1));
    loop {
        cleanup_interval.tick().await;
        let mut queue = pr_queue().write().await;
        while let Some((Reverse(expiry), key)) = queue.peek() {
            if expiry <= &SystemTime::now() {
                let shard_key = get_hash(key).shard_key(persistence_router.len());

                let db = &persistence_router[shard_key];
                db.send(PersistEnum::Delete(key.clone())).await?;

                queue.pop();
            } else {
                break;
            }
        }
    }
}

pub fn run_delete_expired_key_actor(senders_to_persistent_actors: PersistenceRouter) {
    tokio::spawn(async move {
        delete_actor(senders_to_persistent_actors).await.unwrap();
    });
}
