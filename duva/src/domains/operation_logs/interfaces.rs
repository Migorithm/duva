use super::WriteOperation;
use anyhow::Result;

/// Trait for a write-ahead log (WAL) abstraction.
pub trait TWriteAheadLog: Send + Sync + 'static {
    /// Appends a single `WriteOperation` to the log.
    fn append(&mut self, op: WriteOperation) -> impl Future<Output = Result<()>> + Send;

    /// Append one or more `WriteOperation`s to the log.
    fn append_many(&mut self, ops: Vec<WriteOperation>) -> impl Future<Output = Result<()>> + Send;

    /// Retrieve logs that fall between the current 'commit' index and target 'log' index.
    /// This is NOT async as it is expected to be infallible and an in-memory operation.
    fn range(
        &self,
        start_exclusive: u64,
        end_inclusive: u64,
    ) -> impl Future<Output = Vec<WriteOperation>> + Send;

    /// Replays all logged operations from the beginning of the WAL, calling the provided callback `f` for each operation.
    /// The callback `f(WriteOperation)` receives each operation in the order it was appended.
    fn replay<F>(&mut self, f: F) -> impl Future<Output = Result<()>> + Send
    where
        F: FnMut(WriteOperation) + Send;

    /// Forces pending writes to be physically recorded on disk.
    fn fsync(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Replicate all logs from the leader. This is intended to only be called from a follower.
    fn follower_full_sync(
        &mut self,
        ops: Vec<WriteOperation>,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Retrieves the log at a given index.
    fn read_at(&self, prev_log_index: u64) -> impl Future<Output = Option<WriteOperation>> + Send;

    /// If the log has been compacted (e.g., via snapshots), `log_start_index` will be greater than 1, meaning earlier entries have been processed.
    fn log_start_index(&self) -> u64;

    /// Returns true if there are no logs. Otherwise, returns false.
    fn is_empty(&self) -> bool;

    /// Truncate logs that are positioned after `log_index`.
    fn truncate_after(&mut self, log_index: u64) -> impl Future<Output = ()> + Send;
}
