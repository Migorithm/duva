//! A local write-ahead-lof file (WAL) adapter.
use crate::domains::replications::WriteOperation;
use crate::domains::replications::interfaces::TWriteAheadLog;
use anyhow::Result;

#[derive(Default, Clone)]
pub struct MemoryOpLogs {
    pub writer: Vec<WriteOperation>,
}

impl TWriteAheadLog for MemoryOpLogs {
    fn write_many(&mut self, ops: Vec<WriteOperation>) -> Result<()> {
        self.writer.extend(ops);
        Ok(())
    }

    fn replay<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(WriteOperation) + Send,
    {
        for op in self.writer.iter() {
            f(op.clone());
        }
        Ok(())
    }

    fn range(&self, start_exclusive: u64, end_inclusive: u64) -> Vec<WriteOperation> {
        self.writer
            .iter()
            .filter(|op| start_exclusive < op.log_index && op.log_index <= end_inclusive)
            .cloned()
            .collect()
    }

    fn read_at(&mut self, prev_log_index: u64) -> Option<WriteOperation> {
        self.writer.iter().find(|op| op.log_index == prev_log_index).cloned()
    }

    fn is_empty(&self) -> bool {
        self.writer.is_empty()
    }

    fn truncate_after(&mut self, log_index: u64) {
        self.writer.retain(|op| op.log_index <= log_index);
    }
}
