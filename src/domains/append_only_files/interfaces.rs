use super::WriteOperation;
use anyhow::Result;

/// Trait for an Append-Only File (AOF) abstraction.
pub trait TAof: Send + Sync + 'static {
    /// Appends a single `WriteOperation` to the log.
    fn append(
        &mut self,
        op: WriteOperation,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    // Retrieve logs that fall between the current 'log' index of this node and leader 'commit' idx
    // note that there is a chance that this node hasn't received the log entries from the leader that matches the given commit idx.
    // in that case, we simply get the latest possible value and apply it to the state machine
    fn range(&self, start: u64, end: u64) -> Vec<WriteOperation>;

    /// Replays all logged operations from the beginning of the AOF, calling the provided callback `f` for each operation.
    ///
    /// The callback `f(WriteOperation)` receives each operation in the order it was appended.
    fn replay<F>(&mut self, f: F) -> impl std::future::Future<Output = Result<()>> + Send
    where
        F: FnMut(WriteOperation) + Send;

    /// Forces pending writes to be physically recorded on disk.
    fn fsync(&mut self) -> impl std::future::Future<Output = Result<()>> + Send;
}
