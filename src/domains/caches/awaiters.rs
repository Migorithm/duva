use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::oneshot::Sender;

use crate::domains::query_parsers::QueryIO;

pub struct Awaiters {
    pub(crate) hwm: Arc<AtomicU64>,
    inner: HashMap<u64, Vec<Sender<QueryIO>>>,
}

impl Awaiters {
    pub fn new(hwm: Arc<AtomicU64>) -> Self {
        Awaiters { hwm, inner: Default::default() }
    }

    pub(crate) fn push(&mut self, index: u64, callback: Sender<QueryIO>) {
        self.inner.entry(index).or_default().push(callback);
    }
}

#[test]
fn test_push() {
    //GIVEN
    let (tx1, rx1) = tokio::sync::oneshot::channel();
    let (tx2, rx2) = tokio::sync::oneshot::channel();
    let hwm = Arc::new(AtomicU64::new(1));
    let mut awaiters = Awaiters::new(hwm.clone());

    //WHEN
    awaiters.push(1, tx1);
    awaiters.push(1, tx2);

    //THEN
    assert_eq!(awaiters.inner[&1].len(), 2)
}
