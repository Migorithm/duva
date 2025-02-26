use super::{WriteOperation, WriteRequest, interfaces::TAof};

pub(crate) struct Logger<T: TAof> {
    pub(crate) target: T,
    pub(crate) log_index: u64,
}

impl<T: TAof> Logger<T> {
    pub fn new(target: T) -> Self {
        Self { target, log_index: 0 }
    }

    pub(crate) async fn create_log_entry(
        &mut self,
        log: &WriteRequest,
    ) -> anyhow::Result<WriteOperation> {
        let op = WriteOperation { request: log.clone(), log_index: (self.log_index + 1).into() };
        self.target.append(op.clone()).await?;
        self.log_index += 1;
        Ok(op)
    }

    pub(crate) async fn range_logs(
        &self,
        current_commit_idx: u64,
        leader_commit_idx: u64,
    ) -> anyhow::Result<Vec<WriteOperation>> {
        self.target.range(current_commit_idx, leader_commit_idx).await
    }
}
