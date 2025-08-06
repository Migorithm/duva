use std::collections::{HashMap, VecDeque};

use crate::{ReplicationId, domains::cluster_actors::ConsensusRequest, types::Callback};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MigrationTask {
    pub(crate) task_id: (u64, u64),          // (start_hash, end_hash)
    pub(crate) keys_to_migrate: Vec<String>, // actual keys in this range
}

impl MigrationTask {
    pub(crate) fn key_len(&self) -> usize {
        self.keys_to_migrate.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MigrationBatch {
    pub(crate) id: String,
    pub(crate) target_repl: ReplicationId,
    pub(crate) tasks: Vec<MigrationTask>,
}

impl MigrationBatch {
    pub(crate) fn new(target_repl: ReplicationId, tasks: Vec<MigrationTask>) -> Self {
        Self { id: uuid::Uuid::now_v7().to_string(), target_repl, tasks }
    }
}

#[derive(Debug)]
pub(crate) struct PendingMigrationBatch {
    pub(crate) callback: Callback<anyhow::Result<()>>,
    pub(crate) keys: Vec<String>,
}

impl PendingMigrationBatch {
    pub(crate) fn new(
        callback: impl Into<Callback<anyhow::Result<()>>>,
        keys: Vec<String>,
    ) -> Self {
        Self { callback: callback.into(), keys }
    }
}

#[derive(Debug, Default)]
pub(crate) struct PendingMigration {
    requests: VecDeque<ConsensusRequest>,
    batches: HashMap<String, PendingMigrationBatch>,
}
impl PendingMigration {
    pub(crate) fn add_req(&mut self, req: ConsensusRequest) {
        self.requests.push_back(req);
    }
    pub(crate) fn add_batch(&mut self, id: String, batch: PendingMigrationBatch) {
        self.batches.insert(id, batch);
    }
    pub(crate) fn pop_batch(&mut self, id: &String) -> Option<PendingMigrationBatch> {
        self.batches.remove(id)
    }
    pub(crate) fn pending_requests(self) -> VecDeque<ConsensusRequest> {
        self.requests
    }

    #[cfg(test)]
    pub(crate) fn num_reqs(&self) -> usize {
        self.requests.len()
    }

    pub(crate) fn num_batches(&self) -> usize {
        self.batches.len()
    }
}
