use super::*;
use crate::domains::{cluster_actors::SessionRequest, replications::state::ReplicationState};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Debug)]
pub(crate) struct ReplicatedLogs<T> {
    pub(super) target: T,
    pub(super) last_log_term: u64,
    pub(super) con_idx: Arc<AtomicU64>, // high water mark (commit idx)
    pub(super) state: ReplicationState,
    pub(super) last_applied: u64,
}

impl<T: TWriteAheadLog> ReplicatedLogs<T> {
    pub fn new(target: T, state: ReplicationState) -> Self {
        Self {
            target,
            last_log_term: state.term,
            con_idx: Arc::new(state.last_log_index.into()),
            state,
            last_applied: 0,
        }
    }

    pub(crate) fn list_append_log_entries(
        &self,
        low_watermark: Option<u64>,
    ) -> Vec<WriteOperation> {
        let Some(low_watermark) = low_watermark else {
            return self.from(self.state.last_log_index);
        };

        self.from(low_watermark)
    }

    pub(crate) fn write_single_entry(
        &mut self,
        entry: LogEntry,
        current_term: u64,
        session_req: Option<SessionRequest>,
    ) -> anyhow::Result<()> {
        let op = WriteOperation {
            entry,
            log_index: (self.state.last_log_index + 1),
            term: current_term,
            session_req,
        };

        self.write_many(vec![op])?;

        // ! Last log term must be updated because
        // ! log consistency check is based on previous log term and index
        self.last_log_term = current_term;
        Ok(())
    }

    // FOLLOWER side operation
    pub(crate) fn write_many(&mut self, entries: Vec<WriteOperation>) -> anyhow::Result<u64> {
        if entries.is_empty() {
            return Ok(self.state.last_log_index);
        }

        self.update_metadata(&entries);
        self.target.write_many(entries)?;
        Ok(self.state.last_log_index)
    }

    pub(crate) fn range(&self, start_exclusive: u64, end_inclusive: u64) -> Vec<WriteOperation> {
        self.target.range(start_exclusive, end_inclusive)
    }

    fn from(&self, start_exclusive: u64) -> Vec<WriteOperation> {
        self.target.range(start_exclusive, self.state.last_log_index)
    }

    pub(crate) fn read_at(&mut self, at: u64) -> Option<WriteOperation> {
        self.target.read_at(at)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.target.is_empty()
    }

    pub(crate) fn truncate_after(&mut self, log_index: u64) {
        self.target.truncate_after(log_index);
    }

    fn update_metadata(&mut self, new_entries: &[WriteOperation]) {
        if let Some(last_entry) = new_entries.last() {
            self.state.last_log_index = last_entry.log_index;
            self.last_log_term = last_entry.term;
        }
    }

    pub(crate) fn reset(&mut self) {
        self.con_idx.store(0, Ordering::Release);
        self.state.last_log_index = 0;
        self.last_log_term = 0;
        self.truncate_after(0);
    }
}
