use std::{
    cmp::Reverse,
    time::{Duration, SystemTime},
};

use crate::services::interface::Database;
use anyhow::Result;
use tokio::time::interval;

use super::pr_queue;
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
