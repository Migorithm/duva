use std::sync::{Arc, atomic::AtomicU64};

use crate::domains::cluster_actors::SessionRequest;

use super::{WriteOperation, WriteRequest, interfaces::TWriteAheadLog};
use tracing::debug;

#[derive(Debug)]
pub(crate) struct ReplicatedLogs<T> {
    pub(crate) target: T,
    pub(crate) last_log_index: u64,
    pub(crate) last_log_term: u64,
    pub(crate) con_idx: Arc<AtomicU64>, // high water mark (commit idx)
}
impl<T> ReplicatedLogs<T> {
    pub fn new(target: T, last_log_index: u64, last_log_term: u64) -> Self {
        Self { target, last_log_index, last_log_term, con_idx: Arc::new(last_log_index.into()) }
    }
}

impl<T: TWriteAheadLog> ReplicatedLogs<T> {
    pub(crate) fn list_append_log_entries(
        &self,
        low_watermark: Option<u64>,
    ) -> Vec<WriteOperation> {
        let Some(low_watermark) = low_watermark else {
            return self.from(self.last_log_index);
        };

        let mut logs = Vec::with_capacity((self.last_log_index - low_watermark) as usize);
        logs.extend(self.from(low_watermark));

        logs
    }

    pub(crate) fn write_single_entry(
        &mut self,
        req: &WriteRequest,
        current_term: u64,
        session_req: Option<SessionRequest>,
    ) -> anyhow::Result<()> {
        let op = WriteOperation {
            request: req.clone(),
            log_index: (self.last_log_index + 1),
            term: current_term,
            session_req,
        };

        self.target.append(op)?;
        self.last_log_index += 1;

        // ! Last log term must be updated because
        // ! log consistency check is based on previous log term and index
        self.last_log_term = current_term;
        Ok(())
    }

    // FOLLOWER side operation
    pub(crate) fn follower_write_entries(
        &mut self,
        entries: Vec<WriteOperation>,
    ) -> anyhow::Result<u64> {
        // Filter and append entries in a single operation
        self.update_metadata(&entries);

        self.target.append_many(entries)?;

        debug!("Received log entry with log index up to {}", self.last_log_index);
        Ok(self.last_log_index)
    }

    pub(crate) fn follower_full_sync(&mut self, ops: Vec<WriteOperation>) -> anyhow::Result<()> {
        self.update_metadata(&ops);
        self.target.follower_full_sync(ops)?;
        Ok(())
    }

    pub(crate) fn range(&self, start_exclusive: u64, end_inclusive: u64) -> Vec<WriteOperation> {
        self.target.range(start_exclusive, end_inclusive)
    }

    fn from(&self, start_exclusive: u64) -> Vec<WriteOperation> {
        self.target.range(start_exclusive, self.last_log_index)
    }

    pub(crate) fn read_at(&self, at: u64) -> Option<WriteOperation> {
        self.target.read_at(at)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.target.is_empty()
    }

    pub(crate) fn truncate_after(&mut self, log_index: u64) {
        self.target.truncate_after(log_index);
    }

    fn update_metadata(&mut self, new_entries: &[WriteOperation]) {
        if new_entries.is_empty() {
            return;
        }
        let last_entry = new_entries.last().unwrap();
        self.last_log_index = last_entry.log_index;
        self.last_log_term = last_entry.term;
    }

    pub(crate) fn reset(&mut self) {
        self.last_log_index = 0;
        self.last_log_term = 0;
        self.truncate_after(0);
    }
}
