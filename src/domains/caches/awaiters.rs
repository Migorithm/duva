use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicI64},
};

use tokio::sync::oneshot::Sender;

use crate::domains::query_parsers::QueryIO;

#[derive(Default)]
pub struct Awaiters {
    hwm: Arc<AtomicI64>,
    inner: HashMap<u64, Sender<QueryIO>>,
}
