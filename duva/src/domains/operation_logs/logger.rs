use super::{WriteOperation, WriteRequest, interfaces::TWriteAheadLog};
use tracing::debug;

#[derive(Debug)]
pub(crate) struct ReplicatedLogs<T> {
    pub(crate) target: T,
    pub(crate) last_log_index: u64,
    pub(crate) last_log_term: u64,
}
impl<T> ReplicatedLogs<T> {
    pub fn new(target: T, last_log_index: u64, last_log_term: u64) -> Self {
        Self { target, last_log_index, last_log_term }
    }
}

impl<T: TWriteAheadLog> ReplicatedLogs<T> {
    pub(crate) async fn list_append_log_entries(
        &self,
        low_watermark: Option<u64>,
    ) -> Vec<WriteOperation> {
        let current_idx = self.last_log_index;
        if low_watermark.is_none() {
            return self.from(current_idx).await;
        }
        let mut logs = Vec::with_capacity((self.last_log_index - low_watermark.unwrap()) as usize);
        logs.extend(self.from(low_watermark.unwrap()).await);

        logs
    }

    pub(crate) async fn write_single_entry(
        &mut self,
        log: &WriteRequest,
        current_term: u64,
    ) -> anyhow::Result<()> {
        let op = WriteOperation {
            request: log.clone(),
            log_index: (self.last_log_index + 1),
            term: current_term,
        };

        self.target.append(op).await?;
        self.last_log_index += 1;

        // ! Last log term must be updated because
        // ! log consistency check is based on previous log term and index
        self.last_log_term = current_term;
        Ok(())
    }

    // FOLLOWER side operation
    pub(crate) async fn follower_write_entries(
        &mut self,
        entries: Vec<WriteOperation>,
    ) -> anyhow::Result<u64> {
        // Filter and append entries in a single operation
        self.update_metadata(&entries);

        self.target.append_many(entries).await?;

        debug!("Received log entry with log index up to {}", self.last_log_index);
        Ok(self.last_log_index)
    }

    pub(crate) async fn follower_full_sync(
        &mut self,
        ops: Vec<WriteOperation>,
    ) -> anyhow::Result<()> {
        self.update_metadata(&ops);
        self.target.follower_full_sync(ops).await?;
        Ok(())
    }

    pub(crate) async fn range(
        &self,
        start_exclusive: u64,
        end_inclusive: u64,
    ) -> Vec<WriteOperation> {
        self.target.range(start_exclusive, end_inclusive).await
    }

    async fn from(&self, start_exclusive: u64) -> Vec<WriteOperation> {
        self.target.range(start_exclusive, self.last_log_index).await
    }

    pub(crate) async fn read_at(&self, at: u64) -> Option<WriteOperation> {
        self.target.read_at(at).await
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.target.is_empty()
    }

    pub(crate) async fn truncate_after(&mut self, log_index: u64) {
        self.target.truncate_after(log_index).await;
    }

    fn update_metadata(&mut self, new_entries: &[WriteOperation]) {
        if new_entries.is_empty() {
            return;
        }
        let last_entry = new_entries.last().unwrap();
        self.last_log_index = last_entry.log_index;
        self.last_log_term = last_entry.term;
    }

    pub(crate) async fn reset(&mut self) {
        self.last_log_index = 0;
        self.last_log_term = 0;
        self.truncate_after(0).await
    }
}
