use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::oneshot::Sender;

use crate::domains::query_parsers::QueryIO;

pub struct ReadQueue {
    pub(crate) hwm: Arc<AtomicU64>,
    inner: HashMap<u64, Vec<DeferredRead>>,
}

struct DeferredRead {
    key: String,
    callback: Sender<QueryIO>,
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
        callback: Sender<QueryIO>,
    ) -> Option<Sender<QueryIO>> {
        let current_hwm = self.hwm.load(Ordering::Relaxed);
        if current_hwm < read_idx {
            self.push(read_idx, DeferredRead { key: key.into(), callback });
            None
        } else {
            Some(callback)
        }
    }
}

#[test]
fn test_push() {
    //GIVEN
    let (tx1, rx1) = tokio::sync::oneshot::channel();
    let (tx2, rx2) = tokio::sync::oneshot::channel();
    let hwm = Arc::new(AtomicU64::new(1));
    let mut rq = ReadQueue::new(hwm.clone());

    //WHEN
    rq.push(1, DeferredRead { key: "migo".into(), callback: tx1 });
    rq.push(1, DeferredRead { key: "migo2".into(), callback: tx2 });

    //THEN
    assert_eq!(rq.inner[&1].len(), 2)
}
