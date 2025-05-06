use tracing::debug;

use crate::domains::cluster_actors::replication;

use super::{WriteOperation, WriteRequest, interfaces::TWriteAheadLog};

pub(crate) struct ReplicatedLogs<T: TWriteAheadLog> {
    pub(crate) target: T,
    pub(crate) last_log_index: u64,
    pub(crate) last_log_term: u64,
}

impl<T: TWriteAheadLog> ReplicatedLogs<T> {
    pub fn new(target: T, last_log_index: u64, last_log_term: u64) -> Self {
        Self { target, last_log_index, last_log_term }
    }

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
        repl_state: &replication::ReplicationState,
    ) -> anyhow::Result<()> {
        if !repl_state.is_leader_mode {
            return Err(anyhow::anyhow!("Write given to follower"));
        }

        let op = WriteOperation {
            request: log.clone(),
            log_index: (self.last_log_index + 1),
            term: repl_state.term,
        };
        self.target.append(op).await?;
        self.last_log_index += 1;

        // ! Last log term must be updated because
        // ! log consistency check is based on previous log term and index
        self.last_log_term = repl_state.term;
        Ok(())
    }

    // FOLLOWER side operation
    pub(crate) async fn follower_write_entries(
        &mut self,
        append_entries: Vec<WriteOperation>,
    ) -> anyhow::Result<u64> {
        // Filter and append entries in a single operation
        let new_entries: Vec<_> =
            append_entries.into_iter().filter(|log| log.log_index > self.last_log_index).collect();

        self.update_metadata(&new_entries);

        self.target.append_many(new_entries).await?;

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

    pub(crate) fn log_start_index(&self) -> u64 {
        self.target.log_start_index()
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
}
