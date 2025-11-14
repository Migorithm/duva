use crate::domains::replications::WriteOperation;

mod disk_based;
mod memory_based;
pub mod op_logs;

trait TWriteAheadLog: Send + Sync + 'static {
    fn write_many(&mut self, ops: Vec<WriteOperation>) -> anyhow::Result<()>;

    fn range(&self, start_exclusive: u64, end_inclusive: u64) -> Vec<WriteOperation>;

    fn replay(&mut self, f: &mut dyn FnMut(WriteOperation)) -> anyhow::Result<()>;

    fn read_at(&mut self, at: u64) -> Option<WriteOperation>;

    fn is_empty(&self) -> bool;

    fn truncate_after(&mut self, log_index: u64);
}
