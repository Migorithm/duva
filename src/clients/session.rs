use chrono::{DateTime, Utc};
use std::collections::HashMap;
use uuid::Uuid;

use crate::{domains::query_parsers::QueryIO, make_smart_pointer};

struct ClientSessions(HashMap<Uuid, Session>);

struct Session {
    last_accessed: DateTime<Utc>,
    response: Option<Response>,
}

#[derive(Debug, Clone)]
struct Response {
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

    pub fn get_response(&mut self, id: Uuid, req_id: u64) -> Option<Response> {
        let session = self.get(&id)?;
        let Some(res) = session.response.as_ref() else {
            return None;
        };
        if res.request_id == req_id { Some(res.clone()) } else { None }
    }
}
