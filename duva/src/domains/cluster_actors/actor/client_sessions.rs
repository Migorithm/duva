use crate::{make_smart_pointer, presentation::clients::request::ClientReq};
use chrono::{DateTime, Utc};
use std::collections::HashMap;

#[derive(Default, Debug)]
pub(crate) struct ClientSessions(HashMap<String, Session>);
make_smart_pointer!(ClientSessions,HashMap<String, Session>);

#[derive(Default, Debug)]
pub(crate) struct Session {
    last_accessed: DateTime<Utc>,
    processed_req_id: Option<u64>,
}

impl ClientSessions {
    pub(crate) fn is_processed(&self, req: &Option<ClientReq>) -> bool {
        if let Some(session_req) = req
            && let Some(session) = self.get(&session_req.client_id)
            && let Some(res) = session.processed_req_id.as_ref()
        {
            return *res == session_req.request_id;
        }

        false
    }
    pub(crate) fn set_response(&mut self, session_req: Option<ClientReq>) {
        let Some(session_req) = session_req else { return };
        let entry = self
            .entry(session_req.client_id)
            .or_insert(Session { last_accessed: Default::default(), processed_req_id: None });
        entry.last_accessed = Utc::now();
        entry.processed_req_id = Some(session_req.request_id);
    }
}
