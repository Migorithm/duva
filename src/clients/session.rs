use chrono::{DateTime, Utc};
use std::collections::HashMap;
use uuid::Uuid;

use crate::domains::query_parsers::QueryIO;

struct ClientSessions(HashMap<Uuid, Session>);

struct Session {
    last_accessed: DateTime<Utc>,
    response: QueryIO,
}
