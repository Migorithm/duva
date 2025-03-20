use std::collections::HashMap;

use tokio::sync::oneshot::Sender;

use crate::domains::query_parsers::QueryIO;

#[derive(Default)]
pub struct Awaiters(HashMap<u64, Sender<QueryIO>>);
