use crate::{
    adapters::loggers::{disk_based::FileOpLogs, memory_based::MemoryOpLogs},
    domains::replications::{TWriteAheadLog, WriteOperation},
};

pub enum OperationLogs {
    Memory(MemoryOpLogs),
    OnDisk(FileOpLogs),
}

macro_rules! delegate_wal_method {
    ($self:ident.$method:ident($($arg:ident),*)) => {
        match $self {
            Self::Memory(wal) => wal.$method($($arg),*),
            Self::OnDisk(wal) => wal.$method($($arg),*),
        }
    };
}

impl OperationLogs {
    pub fn write_many(&mut self, ops: Vec<WriteOperation>) -> anyhow::Result<()> {
        delegate_wal_method!(self.write_many(ops))
    }

    /// Retrieve logs that fall between the current 'commit' index and target 'log' index.
    /// This is NOT async as it is expected to be infallible and an in-memory operation.
    fn range(&self, start_exclusive: u64, end_inclusive: u64) -> Vec<WriteOperation> {
        delegate_wal_method!(self.range(start_exclusive, end_inclusive))
    }

    /// Replays all logged operations from the beginning of the WAL, calling the provided callback `f` for each operation.
    /// The callback `f(WriteOperation)` receives each operation in the order it was appended.
    fn replay<F>(&mut self, f: F) -> anyhow::Result<()>
    where
        F: FnMut(WriteOperation) + Send,
    {
        delegate_wal_method!(self.replay(f))
    }

    /// Retrieves the log at a given index.
    fn read_at(&mut self, at: u64) -> Option<WriteOperation> {
        delegate_wal_method!(self.read_at(at))
    }

    /// Returns true if there are no logs. Otherwise, returns false.
    fn is_empty(&self) -> bool {
        delegate_wal_method!(self.is_empty())
    }

    /// Truncate logs that are positioned after `log_index`.
    fn truncate_after(&mut self, log_index: u64) {
        delegate_wal_method!(self.truncate_after(log_index))
    }
}
