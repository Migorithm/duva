use std::{cmp::Reverse, collections::BinaryHeap, sync::OnceLock, time::SystemTime};

use tokio::sync::RwLock;

pub mod delete_actor;
pub mod set;

static TTL_QUEUE: OnceLock<RwLock<BinaryHeap<(Reverse<SystemTime>, String)>>> = OnceLock::new();

fn ttl_queue() -> &'static RwLock<BinaryHeap<(Reverse<SystemTime>, String)>> {
    TTL_QUEUE.get_or_init(|| RwLock::new(BinaryHeap::new()))
}
