use std::{cmp::Reverse, collections::BinaryHeap, sync::OnceLock, time::SystemTime};

use delete_actor::TtlDeleteActor;
use set::{TtlSetActor, TtlSetter};
use tokio::sync::RwLock;

use super::routers::inmemory_router::CacheDispatcher;

pub mod command;
pub mod delete_actor;
pub mod set;

static PRIORITY_QUEUE: OnceLock<RwLock<BinaryHeap<(Reverse<SystemTime>, String)>>> =
    OnceLock::new();

fn pr_queue() -> &'static RwLock<BinaryHeap<(Reverse<SystemTime>, String)>> {
    PRIORITY_QUEUE.get_or_init(|| RwLock::new(BinaryHeap::new()))
}

pub fn run_ttl_actors(persistence_router: &CacheDispatcher) -> TtlSetter {
    TtlDeleteActor::run(persistence_router.clone());
    let ttl_setter = TtlSetActor::run();
    ttl_setter
}
