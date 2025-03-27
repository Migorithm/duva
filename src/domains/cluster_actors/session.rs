use chrono::{DateTime, Utc};
use std::collections::HashMap;
use uuid::Uuid;

use crate::{domains::query_parsers::QueryIO, make_smart_pointer};

#[derive(Default)]
pub(crate) struct ClientSessions(HashMap<Uuid, Session>);

pub(crate) struct Session {
    last_accessed: DateTime<Utc>,
    response: Option<SessionResponse>,
}

#[derive(Debug, Clone)]
pub(crate) struct SessionRequest {
    request_id: u64,
    client_id: Uuid,
}
impl SessionRequest {
    pub(crate) fn new(request_id: u64, client_id: Uuid) -> Self {
        Self { request_id, client_id }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SessionResponse {
    request_id: u64,
    value: QueryIO,
}

make_smart_pointer!(ClientSessions,HashMap<Uuid, Session>);

impl ClientSessions {
    pub(crate) fn register_client(&mut self) -> Uuid {
        let id = Uuid::now_v7();
        self.insert(id, Session { last_accessed: Utc::now(), response: None });
        id
    }

    pub(crate) fn get_response(&mut self, client_req: SessionRequest) -> Option<QueryIO> {
        let session = self.get(&client_req.client_id)?;
        let Some(res) = session.response.as_ref() else {
            return None;
        };
        if res.request_id == client_req.request_id { Some(res.value.clone()) } else { None }
    }
    pub(crate) fn set_response(&mut self, id: Uuid, response: SessionResponse) {
        let entry =
            self.entry(id).or_insert(Session { last_accessed: Default::default(), response: None });
        entry.last_accessed = Utc::now();
        entry.response = Some(response);
    }
}
