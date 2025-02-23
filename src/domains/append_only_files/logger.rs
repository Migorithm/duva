use super::{WriteOperation, WriteRequest, interfaces::TAof};

pub(crate) struct Logger<T: TAof> {
    pub(crate) target: T,
    pub(crate) current_offset: u64,
}

impl<T: TAof> Logger<T> {
    pub fn new(target: T) -> Self {
        Self { target, current_offset: 0 }
    }

    pub(crate) async fn create_log_entry(
        &mut self,
        log: &WriteRequest,
    ) -> anyhow::Result<WriteOperation> {
        let op = WriteOperation { op: log.clone(), offset: (self.current_offset + 1).into() };
        self.target.append(op.clone()).await?;
        self.current_offset += 1;
        Ok(op)
    }
}
