use super::{WriteOperation, WriteRequest, interfaces::TAof, log::LogIndex};

pub(crate) struct Logger<T: TAof> {
    pub(crate) target: T,
    pub(crate) log_index: LogIndex,
}

impl<T: TAof> Logger<T> {
    pub fn new(target: T) -> Self {
        Self { target, log_index: 0.into() }
    }

    pub(crate) async fn create_log_entries(
        &mut self,
        log: &WriteRequest,
        lowest_follower_index: u64,
    ) -> anyhow::Result<Vec<WriteOperation>> {
        self.write_log_entry(log).await?;

        let mut logs = Vec::with_capacity((*self.log_index - lowest_follower_index) as usize);
        logs.extend(self.from(lowest_follower_index));

        Ok(logs)
    }
    pub(crate) async fn write_log_entry(&mut self, log: &WriteRequest) -> anyhow::Result<()> {
        let op = WriteOperation { request: log.clone(), log_index: (*self.log_index + 1).into() };
        self.target.append(op).await?;
        *self.log_index += 1;
        Ok(())
    }

    pub(crate) fn range(&self, start_exclusive: u64, end_inclusive: u64) -> Vec<WriteOperation> {
        self.target.range(start_exclusive, end_inclusive)
    }

    // from commit index
    fn from(&self, from: u64) -> Vec<WriteOperation> {
        self.target.range(from, *self.log_index)
    }
}
