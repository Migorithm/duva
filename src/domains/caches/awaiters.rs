use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::oneshot::Sender;

use crate::domains::query_parsers::QueryIO;

pub struct Awaiters {
    hwm: Arc<AtomicU64>,
    inner: HashMap<u64, Sender<QueryIO>>,
}

impl Awaiters {
    pub fn new(hwm: Arc<AtomicU64>) -> Self {
        Awaiters {
            hwm,
            inner: Default::default(),
        }
    }
}
