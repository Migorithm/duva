use crate::{
    ENV,
    adapters::loggers::{TWriteAheadLog, disk_based::FileOpLogs, memory_based::MemoryOpLogs},
    domains::replications::WriteOperation,
};

#[derive(Debug)]
pub enum OperationLogs {
    Memory(MemoryOpLogs),
    OnDisk(FileOpLogs),
}

impl OperationLogs {
    pub fn new_inmemory() -> Self {
        Self::Memory(Default::default())
    }
    pub fn new_ondisk() -> Self {
        Self::OnDisk(FileOpLogs::new(ENV.dir.clone()).unwrap())
    }
    /// Append one or more `WriteOperation`s to the log.
    pub(crate) fn write_many(&mut self, ops: Vec<WriteOperation>) -> anyhow::Result<()> {
        match self {
            OperationLogs::Memory(memory_op_logs) => memory_op_logs.write_many(ops),
            OperationLogs::OnDisk(file_op_logs) => file_op_logs.write_many(ops),
        }
    }

    /// Retrieve logs that fall between the current 'commit' index and target 'log' index.
    /// This is NOT async as it is expected to be infallible and an in-memory operation.
    pub(crate) fn range(&self, start_exclusive: u64, end_inclusive: u64) -> Vec<WriteOperation> {
        match self {
            OperationLogs::Memory(memory_op_logs) => {
                memory_op_logs.range(start_exclusive, end_inclusive)
            },
            OperationLogs::OnDisk(file_op_logs) => {
                file_op_logs.range(start_exclusive, end_inclusive)
            },
        }
    }

    /// Replays all logged operations from the beginning of the WAL, calling the provided callback `f` for each operation.
    /// The callback `f(WriteOperation)` receives each operation in the order it was appended.
    pub(crate) fn replay<F>(&mut self, f: F) -> anyhow::Result<()>
    where
        F: FnMut(WriteOperation) + Send,
    {
        match self {
            OperationLogs::Memory(memory_op_logs) => memory_op_logs.replay(f),
            OperationLogs::OnDisk(file_op_logs) => file_op_logs.replay(f),
        }
    }

    /// Retrieves the log at a given index.
    pub(crate) fn read_at(&mut self, at: u64) -> Option<WriteOperation> {
        match self {
            OperationLogs::Memory(memory_op_logs) => memory_op_logs.read_at(at),
            OperationLogs::OnDisk(file_op_logs) => file_op_logs.read_at(at),
        }
    }

    /// Returns true if there are no logs. Otherwise, returns false.
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            OperationLogs::Memory(memory_op_logs) => memory_op_logs.is_empty(),
            OperationLogs::OnDisk(file_op_logs) => file_op_logs.is_empty(),
        }
    }

    /// Truncate logs that are positioned after `log_index`.
    pub(crate) fn truncate_after(&mut self, log_index: u64) {
        match self {
            OperationLogs::Memory(memory_op_logs) => memory_op_logs.truncate_after(log_index),
            OperationLogs::OnDisk(file_op_logs) => file_op_logs.truncate_after(log_index),
        }
    }
}
