use crate::ReplicationId;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MigrationTask {
    pub(crate) task_id: (u64, u64), // (start_hash, end_hash)
    pub(crate) to: ReplicationId,
    pub(crate) keys_to_migrate: Vec<String>, // actual keys in this range
}

impl MigrationTask {
    pub(crate) fn key_len(&self) -> usize {
        self.keys_to_migrate.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BatchId(Uuid);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MigrationBatch {
    pub(crate) id: BatchId,
    pub(crate) tasks: Vec<MigrationTask>,
}
impl MigrationBatch {
    pub(crate) fn new(tasks: Vec<MigrationTask>) -> Self {
        Self { id: BatchId(uuid::Uuid::now_v7()), tasks }
    }
}
