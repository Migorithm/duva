use std::{
    cmp::Reverse,
    time::{Duration, SystemTime},
};

use crate::services::{hasher::get_hash, persistence::PersistEnum};
use anyhow::Result;
use tokio::{sync::mpsc::Sender, time::interval};

use super::pr_queue;
async fn delete_actor(senders_to_persistent_actors: Vec<Sender<PersistEnum>>) -> Result<()> {
    //TODO interval period should be configurable
    let mut cleanup_interval = interval(Duration::from_millis(1));
    loop {
        cleanup_interval.tick().await;
        let mut queue = pr_queue().write().await;
        while let Some((Reverse(expiry), key)) = queue.peek() {
            if expiry <= &SystemTime::now() {
                let shard_key = get_hash(key).shard_key(senders_to_persistent_actors.len());

                let db = &senders_to_persistent_actors[shard_key];
                db.send(PersistEnum::Delete(key.clone())).await?;

                queue.pop();
            } else {
                break;
            }
        }
    }
}

pub fn run_delete_expired_key_actor(senders_to_persistent_actors: Vec<Sender<PersistEnum>>) {
    tokio::spawn(async move {
        delete_actor(senders_to_persistent_actors).await.unwrap();
    });
}
