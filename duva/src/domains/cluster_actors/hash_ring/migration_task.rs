use std::collections::{HashMap, VecDeque};

use crate::{ReplicationId, domains::cluster_actors::ConsensusRequest, types::Callback};

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
