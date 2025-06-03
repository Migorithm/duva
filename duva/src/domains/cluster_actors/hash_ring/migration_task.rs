use crate::ReplicationId;

#[derive(Debug, Clone, bincode::Decode, bincode::Encode, PartialEq, Eq)]
pub(crate) struct MigrationTask {
    pub(super) partition_range: (u64, u64), // (start_hash, end_hash)
    pub(super) from_node: ReplicationId,
    pub(super) to_node: ReplicationId,
    pub(super) keys_to_migrate: Vec<String>, // actual keys in this range
}

impl MigrationTask {
    pub(crate) fn len(&self) -> usize {
        self.keys_to_migrate.len()
    }
}
