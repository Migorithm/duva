use super::{WriteOperation, WriteRequest, interfaces::TWriteAheadLog, log::LogIndex};

pub(crate) struct Logger<T: TWriteAheadLog> {
    pub(crate) target: T,
    pub(crate) log_index: LogIndex,

    // need to store
    pub(crate) log_term: u64,
}

impl<T: TWriteAheadLog> Logger<T> {
    pub fn new(target: T) -> Self {
        Self { target, log_index: 0.into(), log_term: 0 }
    }

    pub(crate) async fn create_log_entries(
        &mut self,
        log: &WriteRequest,

        lowest_follower_index: Option<u64>,
    ) -> anyhow::Result<Vec<WriteOperation>> {
        let current_idx = self.log_index;
        self.write_log_entry(log).await?;

        if lowest_follower_index.is_none() {
            return Ok(self.from(current_idx.into()));
        }

        let mut logs =
            Vec::with_capacity((*self.log_index - lowest_follower_index.unwrap()) as usize);
        logs.extend(self.from(lowest_follower_index.unwrap()));

        Ok(logs)
    }

    pub(crate) async fn write_log_entry(&mut self, log: &WriteRequest) -> anyhow::Result<()> {
        let op = WriteOperation { request: log.clone(), log_index: (*self.log_index + 1).into() };
        self.target.append(op).await?;
        *self.log_index += 1;
        Ok(())
    }

    pub(crate) async fn write_log_entries(
        &mut self,
        append_entries: Vec<WriteOperation>,
    ) -> anyhow::Result<LogIndex> {
        // Filter and append entries in a single operation
        let new_entries: Vec<_> =
            append_entries.into_iter().filter(|log| log.log_index > self.log_index).collect();

        let cnt = new_entries.len();
        self.target.append_many(new_entries).await?;
        *self.log_index += cnt as u64;

        println!("[INFO] Received log entry with log index up to {}", self.log_index);
        Ok(self.log_index.into())
    }

    pub(crate) async fn overwrite(&mut self, ops: Vec<WriteOperation>) -> anyhow::Result<()> {
        let last_index = ops.len() as u64;
        self.target.overwrite(ops).await?;
        self.log_index = LogIndex(last_index);
        Ok(())
    }
    pub(crate) fn range(&self, start_exclusive: u64, end_inclusive: u64) -> Vec<WriteOperation> {
        self.target.range(start_exclusive, end_inclusive)
    }

    fn from(&self, start_exclusive: u64) -> Vec<WriteOperation> {
        self.target.range(start_exclusive, *self.log_index)
    }
}
