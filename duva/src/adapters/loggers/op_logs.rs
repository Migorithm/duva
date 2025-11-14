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
    fn get_mut(&mut self) -> &mut dyn TWriteAheadLog {
        match self {
            OperationLogs::Memory(m) => m as &mut dyn TWriteAheadLog,
            OperationLogs::OnDisk(f) => f as &mut dyn TWriteAheadLog,
        }
    }
    fn get(&self) -> &dyn TWriteAheadLog {
        match self {
            OperationLogs::Memory(m) => m as &dyn TWriteAheadLog,
            OperationLogs::OnDisk(f) => f as &dyn TWriteAheadLog,
        }
    }

    /// Append one or more `WriteOperation`s to the log.
    pub(crate) fn write_many(&mut self, ops: Vec<WriteOperation>) -> anyhow::Result<()> {
        self.get_mut().write_many(ops)
    }

    /// Retrieve logs that fall between the current 'commit' index and target 'log' index.
    /// This is NOT async as it is expected to be infallible and an in-memory operation.
    pub(crate) fn range(&self, start_exclusive: u64, end_inclusive: u64) -> Vec<WriteOperation> {
        self.get().range(start_exclusive, end_inclusive)
    }

    /// Replays all logged operations from the beginning of the WAL, calling the provided callback `f` for each operation.
    /// The callback `f(WriteOperation)` receives each operation in the order it was appended.
    pub(crate) fn replay(&mut self, f: &mut dyn FnMut(WriteOperation)) -> anyhow::Result<()> {
        self.get_mut().replay(f)
    }

    /// Retrieves the log at a given index.
    pub(crate) fn read_at(&mut self, at: u64) -> Option<WriteOperation> {
        self.get_mut().read_at(at)
    }

    /// Returns true if there are no logs. Otherwise, returns false.
    pub(crate) fn is_empty(&self) -> bool {
        self.get().is_empty()
    }

    /// Truncate logs that are positioned after `log_index`.
    pub(crate) fn truncate_after(&mut self, log_index: u64) {
        self.get_mut().truncate_after(log_index)
    }
}
