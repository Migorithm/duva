use chrono::{DateTime, Utc};
use std::collections::HashMap;
use uuid::Uuid;

use crate::make_smart_pointer;

use super::heartbeats::heartbeat::ClientSessionInfo;

#[derive(Default)]
pub(crate) struct ClientSessions(HashMap<Uuid, Session>);

pub(crate) struct Session {
    last_accessed: DateTime<Utc>,
    processed_req_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SessionRequest {
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
    pub(crate) fn is_processed(&self, client_req: &Option<SessionRequest>) -> bool {
        let Some(client_req) = client_req else {
            return false;
        };
        if self.get(&client_req.client_id).is_none() {
            return false;
        }
        let session = self.get(&client_req.client_id).unwrap();
        let Some(res) = session.processed_req_id.as_ref() else {
            return false;
        };

        *res == client_req.request_id
    }
    pub(crate) fn set_response(&mut self, session_req: Option<SessionRequest>) {
        let Some(session_req) = session_req else { return };

        let entry = self
            .entry(session_req.client_id)
            .or_insert(Session { last_accessed: Default::default(), processed_req_id: None });
        entry.last_accessed = Utc::now();
        entry.processed_req_id = Some(session_req.request_id);
    }
    pub(crate) fn extract_session_info(&self) -> HashMap<Uuid, ClientSessionInfo> {
        self.iter()
            .filter_map(|(client_id, session)| {
                session.processed_req_id.map(|processed_req_id| {
                    let timestamp = session.last_accessed.timestamp_millis() as u64;
                    (
                        *client_id,
                        ClientSessionInfo {
                            client_id: client_id.clone(),
                            processed_req_id,
                            timestamp,
                        },
                    )
                })
            })
            .collect()
    }
    pub(crate) fn merge_session_info(&mut self, remote_sessions: HashMap<Uuid, ClientSessionInfo>) {
        for (client_id, remote_info) in remote_sessions {
            let local_session = self
                .entry(client_id)
                .or_insert(Session { last_accessed: Default::default(), processed_req_id: None });

            let remote_time = DateTime::<Utc>::from_timestamp_millis(remote_info.timestamp as i64)
                .unwrap_or_default();
            if remote_time > local_session.last_accessed {
                local_session.last_accessed = remote_time;
                local_session.processed_req_id = Some(remote_info.processed_req_id);
            }
        }
    }
}
