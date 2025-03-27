use chrono::{DateTime, Utc};
use std::collections::HashMap;
use uuid::Uuid;

use crate::make_smart_pointer;

#[derive(Default)]
pub(crate) struct ClientSessions(HashMap<Uuid, Session>);

pub(crate) struct Session {
    last_accessed: DateTime<Utc>,
    latest_processed_idx: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct SessionRequest {
    pub(crate) request_id: u64,
    pub(crate) client_id: Uuid,
}
impl SessionRequest {
    pub(crate) fn new(request_id: u64, client_id: Uuid) -> Self {
        Self { request_id, client_id }
    }
}

make_smart_pointer!(ClientSessions,HashMap<Uuid, Session>);

impl ClientSessions {
    pub(crate) fn register_client(&mut self) -> Uuid {
        let id = Uuid::now_v7();
        self.insert(id, Session { last_accessed: Utc::now(), latest_processed_idx: None });
        id
    }

    pub(crate) fn is_processed(&self, client_req: SessionRequest) -> bool {
        if self.get(&client_req.client_id).is_none() {
            return false;
        }
        let session = self.get(&client_req.client_id).unwrap();
        let Some(res) = session.latest_processed_idx.as_ref() else {
            return false;
        };

        *res == client_req.request_id
    }
    pub(crate) fn set_response(&mut self, id: Uuid, last_processed_idx: u64) {
        let entry = self
            .entry(id)
            .or_insert(Session { last_accessed: Default::default(), latest_processed_idx: None });
        entry.last_accessed = Utc::now();
        entry.latest_processed_idx = Some(last_processed_idx);
    }
}
