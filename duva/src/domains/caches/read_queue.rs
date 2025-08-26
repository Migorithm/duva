use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::types::Callback;

use super::cache_objects::CacheValue;

pub struct ReadQueue {
    pub(crate) con_idx: Arc<AtomicU64>,
    inner: HashMap<u64, Vec<DeferredRead>>,
}

pub(crate) struct DeferredRead {
    pub(crate) key: String,
    pub(crate) callback: Callback<CacheValue>,
}

impl ReadQueue {
    pub fn new(con_idx: Arc<AtomicU64>) -> Self {
        ReadQueue { con_idx, inner: Default::default() }
    }

    fn push(&mut self, index: u64, deferred_read: DeferredRead) {
        self.inner.entry(index).or_default().push(deferred_read);
    }
    pub(crate) fn defer_if_stale(
        &mut self,
        read_stat_idx: u64,
        key: &str,
        callback: Callback<CacheValue>,
    ) -> Option<Callback<CacheValue>> {
        // ! comparison should be amde on stat index
        let current_con_idx = self.con_idx.load(Ordering::Relaxed);
        if current_con_idx < read_stat_idx {
            self.push(read_stat_idx, DeferredRead { key: key.into(), callback });
            None
        } else {
            Some(callback)
        }
    }

    pub(crate) fn take_pending_requests(&mut self) -> Option<Vec<DeferredRead>> {
        let current_con_idx = self.con_idx.load(Ordering::Relaxed);
        self.inner.remove(&current_con_idx)
    }
}

#[test]
fn test_push() {
    //GIVEN
    let (tx1, _) = Callback::create();
    let (tx2, _) = Callback::create();
    let con_idx = Arc::new(AtomicU64::new(1));
    let mut rq = ReadQueue::new(con_idx.clone());

    //WHEN
    rq.push(1, DeferredRead { key: "migo".into(), callback: tx1 });
    rq.push(1, DeferredRead { key: "migo2".into(), callback: tx2 });

    //THEN
    assert_eq!(rq.inner[&1].len(), 2)
}
