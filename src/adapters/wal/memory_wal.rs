//! A local write-ahead-lof file (WAL) adapter.
use crate::domains::append_only_files::WriteOperation;
use crate::domains::append_only_files::interfaces::TWriteAheadLog;
use anyhow::Result;

#[derive(Default, Clone)]
pub struct InMemoryWAL {
    writer: Vec<WriteOperation>,
}

impl TWriteAheadLog for InMemoryWAL {
    async fn append(&mut self, op: WriteOperation) -> Result<()> {
        self.writer.push(op);
        Ok(())
    }
    async fn append_many(&mut self, ops: Vec<WriteOperation>) -> Result<()> {
        self.writer.extend(ops);
        Ok(())
    }

    async fn replay<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(WriteOperation) + Send,
    {
        for op in self.writer.iter() {
            f(op.clone());
        }
        Ok(())
    }

    async fn fsync(&mut self) -> Result<()> {
        Ok(())
    }

    async fn overwrite(&mut self, ops: Vec<WriteOperation>) -> Result<()> {
        self.writer = ops;
        Ok(())
    }

    fn range(&self, start_exclusive: u64, end_inclusive: u64) -> Vec<WriteOperation> {
        self.writer
            .iter()
            .filter(|op| start_exclusive < op.log_index && op.log_index <= end_inclusive)
            .cloned()
            .collect()
    }

    async fn read_at(&self, prev_log_index: u64) -> Option<WriteOperation> {
        self.writer.iter().find(|op| op.log_index == prev_log_index).cloned()
    }
}
