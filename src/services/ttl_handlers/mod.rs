use std::{cmp::Reverse, collections::BinaryHeap, sync::OnceLock, time::SystemTime};

use tokio::sync::RwLock;

pub mod delete;
pub mod set;

static PRIORITY_QUEUE: OnceLock<RwLock<BinaryHeap<(Reverse<SystemTime>, String)>>> =
    OnceLock::new();

fn pr_queue() -> &'static RwLock<BinaryHeap<(Reverse<SystemTime>, String)>> {
    PRIORITY_QUEUE.get_or_init(|| RwLock::new(BinaryHeap::new()))
}
