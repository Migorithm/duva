use std::collections::VecDeque;

use crate::command::InputKind;

#[derive(Debug, Default)]
pub struct InputQueue {
    pub queue: VecDeque<InputKind>,
}
impl InputQueue {
    pub fn push(&mut self, input: InputKind) {
        self.queue.push_back(input);
    }

    pub fn pop(&mut self) -> Option<InputKind> {
        self.queue.pop_front()
    }
}
