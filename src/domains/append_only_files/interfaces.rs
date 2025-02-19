use super::WriteOperation;
use anyhow::Result;

/// Trait for an Append-Only File (AOF) abstraction.
pub trait TAof {
    /// Appends a single `WriteOperation` to the log.
    fn append(
        &mut self,
        op: WriteOperation,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Replays all logged operations from the beginning of the AOF, calling the provided callback `f` for each operation.
    ///
    /// The callback `f(WriteOperation)` receives each operation in the order it was appended.
    fn replay<F>(&mut self, f: F) -> impl std::future::Future<Output = Result<()>> + Send
    where
        F: FnMut(WriteOperation) + Send;

    /// Forces pending writes to be physically recorded on disk.
    fn fsync(&mut self) -> impl std::future::Future<Output = Result<()>> + Send;
}
