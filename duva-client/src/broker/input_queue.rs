use crate::command::InputContext;
use std::collections::VecDeque;

#[derive(Debug, Default)]
pub struct InputQueue {
    pub queue: VecDeque<InputContext>,
}
impl InputQueue {
    pub fn push(&mut self, input_context: InputContext) {
        self.queue.push_back(input_context);
    }

    pub fn pop(&mut self) -> Option<InputContext> {
        self.queue.pop_front()
    }
}
