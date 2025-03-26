use chrono::{DateTime, Utc};
use std::collections::HashMap;
use uuid::Uuid;

use crate::{domains::query_parsers::QueryIO, make_smart_pointer};

struct ClientSessions(HashMap<Uuid, Session>);

#[derive(Default)]
struct Session {
    last_accessed: DateTime<Utc>,
    response: Option<QueryIO>,
}
make_smart_pointer!(ClientSessions,HashMap<Uuid, Session>);

impl ClientSessions {
    pub(crate) fn register_client(&mut self) -> Uuid {
        let id = Uuid::now_v7();
        self.insert(id, Session::default());
        id
    }
}
