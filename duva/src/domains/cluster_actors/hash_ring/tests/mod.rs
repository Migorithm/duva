use crate::{
    ReplicationId,
    domains::cluster_actors::hash_ring::{HashRing, MigrationTask},
    prelude::PeerIdentifier,
};
use std::{collections::HashSet, thread::sleep, time::Duration};
mod add_and_remove;
mod migration;

pub(crate) fn migration_task_create_helper(start_hash: u64, end_hash: u64) -> MigrationTask {
    MigrationTask {
        task_id: (start_hash, end_hash),
        to: ReplicationId::Key("127.0.0.1:6559".into()),
        keys_to_migrate: (start_hash..end_hash).map(|i| format!("key_{}", i)).collect(),
    }
}
