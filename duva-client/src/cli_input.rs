use std::collections::VecDeque;

use duva::{domains::query_parsers::query_io::QueryIO, prelude::tokio::sync::oneshot};

use crate::command::ClientInputKind;
use ClientInputKind::*;

#[derive(Debug, Default)]
pub struct InputQueue {
    pub queue: VecDeque<Input>,
}
impl InputQueue {
    pub fn push(&mut self, input: Input) {
        self.queue.push_back(input);
    }

    pub fn pop(&mut self) -> Option<Input> {
        self.queue.pop_front()
    }
}

#[derive(Debug)]
pub struct Input {
    pub kind: ClientInputKind,
    pub callback: oneshot::Sender<(ClientInputKind, QueryIO)>,
}

impl Input {
    pub fn new(
        input: ClientInputKind,
        callback: oneshot::Sender<(ClientInputKind, QueryIO)>,
    ) -> Self {
        Self { kind: input, callback }
    }
}
