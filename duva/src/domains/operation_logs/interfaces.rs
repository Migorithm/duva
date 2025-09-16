use super::WriteOperation;
use anyhow::Result;

/// Trait for a write-ahead log (WAL) abstraction.
pub trait TWriteAheadLog: Send + Sync + 'static {
    /// Append one or more `WriteOperation`s to the log.
    fn append_many(&mut self, ops: Vec<WriteOperation>) -> Result<()>;

    /// Retrieve logs that fall between the current 'commit' index and target 'log' index.
    /// This is NOT async as it is expected to be infallible and an in-memory operation.
    fn range(&self, start_exclusive: u64, end_inclusive: u64) -> Vec<WriteOperation>;

    /// Replays all logged operations from the beginning of the WAL, calling the provided callback `f` for each operation.
    /// The callback `f(WriteOperation)` receives each operation in the order it was appended.
    fn replay<F>(&mut self, f: F) -> Result<()>
    where
        F: FnMut(WriteOperation) + Send;

    /// Retrieves the log at a given index.
    fn read_at(&mut self, at: u64) -> Option<WriteOperation>;

    /// Returns true if there are no logs. Otherwise, returns false.
    fn is_empty(&self) -> bool;

    /// Truncate logs that are positioned after `log_index`.
    fn truncate_after(&mut self, log_index: u64);
}
