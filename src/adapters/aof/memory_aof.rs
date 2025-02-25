//! A local append-only file (AOF) adapter.

use crate::domains::append_only_files::WriteOperation;
use crate::domains::append_only_files::interfaces::TAof;
use anyhow::Result;

/// A local append-only file (AOF) implementation.
#[derive(Default)]
pub struct InMemoryAof {
    writer: Vec<WriteOperation>,
}

impl TAof for InMemoryAof {
    async fn append(&mut self, op: WriteOperation) -> Result<()> {
        self.writer.push(op);
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
}
