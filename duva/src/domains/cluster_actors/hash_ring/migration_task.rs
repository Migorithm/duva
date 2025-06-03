use crate::ReplicationId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MigrationTask {
    pub(super) task_id: TaskId, // (start_hash, end_hash)
    pub(super) from_node: ReplicationId,
    pub(super) to_node: ReplicationId,
    pub(super) keys_to_migrate: Vec<String>, // actual keys in this range
}

impl MigrationTask {
    pub(crate) fn len(&self) -> usize {
        self.keys_to_migrate.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TaskId(u64, u64);
impl TaskId {
    pub(crate) fn new(partition_start: u64, partition_end: u64) -> Self {
        Self(partition_start, partition_end)
    }
}
