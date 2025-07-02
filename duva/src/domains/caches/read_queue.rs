use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::oneshot::Sender;

use super::cache_objects::CacheValue;

pub struct ReadQueue {
    pub(crate) hwm: Arc<AtomicU64>,
    inner: HashMap<u64, Vec<DeferredRead>>,
}

pub(crate) struct DeferredRead {
    pub(crate) key: String,
    pub(crate) callback: Sender<CacheValue>,
}

impl ReadQueue {
    pub fn new(hwm: Arc<AtomicU64>) -> Self {
        ReadQueue { hwm, inner: Default::default() }
    }

    fn push(&mut self, index: u64, deferred_read: DeferredRead) {
        self.inner.entry(index).or_default().push(deferred_read);
    }
    pub(crate) fn defer_if_stale(
        &mut self,
        read_idx: u64,
        key: &str,
        callback: Sender<CacheValue>,
    ) -> Option<Sender<CacheValue>> {
        let current_hwm = self.hwm.load(Ordering::Relaxed);
        if current_hwm < read_idx {
            self.push(read_idx, DeferredRead { key: key.into(), callback });
            None
        } else {
            Some(callback)
        }
    }

    pub(crate) fn take_pending_requests(&mut self) -> Option<Vec<DeferredRead>> {
        let current_hwm = self.hwm.load(Ordering::Relaxed);
        self.inner.remove(&current_hwm)
    }
}

#[test]
fn test_push() {
    //GIVEN
    let (tx1, _) = tokio::sync::oneshot::channel();
    let (tx2, _) = tokio::sync::oneshot::channel();
    let hwm = Arc::new(AtomicU64::new(1));
    let mut rq = ReadQueue::new(hwm.clone());

    //WHEN
    rq.push(1, DeferredRead { key: "migo".into(), callback: tx1 });
    rq.push(1, DeferredRead { key: "migo2".into(), callback: tx2 });

    //THEN
    assert_eq!(rq.inner[&1].len(), 2)
}
