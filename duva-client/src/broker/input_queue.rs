use std::collections::VecDeque;

use crate::command::Input;

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
