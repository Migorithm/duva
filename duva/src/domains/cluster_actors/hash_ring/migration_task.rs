use crate::ReplicationId;

#[derive(Debug, Clone)]
pub(crate) struct MigrationTask {
    pub(super) partition_range: (u64, u64), // (start_hash, end_hash)
    pub(super) from_node: ReplicationId,
    pub(super) to_node: ReplicationId,
    pub(super) keys_to_migrate: Vec<String>, // actual keys in this range
}
