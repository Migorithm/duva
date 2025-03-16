pub(crate) use super::heartbeats::heartbeat::BannedPeer;
pub(crate) use super::heartbeats::heartbeat::HeartBeatMessage;
use crate::domains::append_only_files::WriteOperation;
use crate::domains::peers::identifier::PeerIdentifier;

#[derive(Debug, Clone)]
pub struct ReplicationInfo {
    pub(crate) leader_repl_id: PeerIdentifier, // The replication ID of the master example: 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb

    pub(crate) hwm: u64, // high water mark (commit idx)
    role: String,

    pub(crate) self_host: String,
    pub(crate) self_port: u16,
    //If the instance is a replica, these additional fields are provided:
    pub(crate) leader_host: Option<String>,
    pub(crate) leader_port: Option<u16>,

    // * state is shared among peers
    pub(crate) term: u64,
    pub(crate) ban_list: Vec<BannedPeer>,
}

impl ReplicationInfo {
    pub fn new(replicaof: Option<(String, String)>, self_host: &str, self_port: u16) -> Self {
        let leader_repl_id = if let Some((leader_host, leader_port)) = replicaof.as_ref() {
            PeerIdentifier::new(
                leader_host,
                leader_port.parse().expect("Invalid port number given"),
            )
        } else {
            PeerIdentifier::new(self_host, self_port)
        };

        let replication = ReplicationInfo {
            leader_repl_id: leader_repl_id.clone(),
            hwm: 0,
            role: if replicaof.is_some() { "follower".to_string() } else { "leader".to_string() },
            leader_host: replicaof.as_ref().cloned().map(|(host, _)| host),
            leader_port: replicaof
                .map(|(_, port)| port.parse().expect("Invalid port number given")),
            term: 0,
            self_host: self_host.to_string(),
            self_port,
            ban_list: Default::default(),
        };

        replication
    }

    pub(crate) fn self_info(&self) -> String {
        let self_id = self.self_identifier();
        let leader_repl_id =
            if *self_id == *self.leader_repl_id { "-" } else { &self.leader_repl_id };

        //TODO last 0 denotes slots - subject to work
        format!("{} myself,{} {} 0", self_id, self.role(), leader_repl_id)
    }

    pub(crate) fn self_identifier(&self) -> PeerIdentifier {
        PeerIdentifier::new(&self.self_host, self.self_port)
    }
    pub(crate) fn role(&self) -> &str {
        &self.role
    }

    pub fn vectorize(self) -> Vec<String> {
        vec![
            format!("role:{}", self.role),
            format!("leader_repl_id:{}", self.leader_repl_id),
            format!("high_watermark:{}", self.hwm),
            format!("self_identifier:{}", self.self_identifier()),
        ]
    }

    pub(crate) fn leader_bind_addr(&self) -> Option<PeerIdentifier> {
        Some(format!("{}:{}", self.leader_host.as_ref()?, self.leader_port?).into())
    }

    pub(crate) fn in_ban_list(&self, peer_identifier: &PeerIdentifier) -> bool {
        if let Ok(current_time_in_sec) = time_in_secs() {
            self.ban_list.iter().any(|node| {
                &node.p_id == peer_identifier && current_time_in_sec - node.ban_time < 60
            })
        } else {
            false
        }
    }

    pub fn append_entry(&self, hop_count: u8, entries: Vec<WriteOperation>) -> HeartBeatMessage {
        let mut heartbeat = self.default_heartbeat(hop_count);
        heartbeat.append_entries.extend(entries);
        heartbeat
    }

    pub fn default_heartbeat(&self, hop_count: u8) -> HeartBeatMessage {
        HeartBeatMessage {
            heartbeat_from: self.self_identifier(),
            term: self.term,
            hwm: self.hwm,
            leader_replid: self.leader_repl_id.clone(),
            hop_count,
            ban_list: self.ban_list.clone(),
            append_entries: vec![],
            cluster_nodes: vec![],
        }
    }

    pub(crate) fn ban_peer(&mut self, p_id: &PeerIdentifier) -> anyhow::Result<()> {
        self.ban_list.push(BannedPeer { p_id: p_id.clone(), ban_time: time_in_secs()? });
        Ok(())
    }

    pub(crate) fn remove_from_ban_list(&mut self, peer_addr: &PeerIdentifier) {
        let idx = self.ban_list.iter().position(|node| &node.p_id == peer_addr);
        if let Some(idx) = idx {
            self.ban_list.swap_remove(idx);
        }
    }
    pub(crate) fn is_leader_mode(&self) -> bool {
        self.leader_host.is_none()
    }

    pub(crate) fn set_leader_state(&mut self) {
        self.role = "leader".to_string();
        self.leader_host = None;
        self.leader_port = None;
        self.leader_repl_id = self.self_identifier();
    }
}

pub(crate) fn time_in_secs() -> anyhow::Result<u64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs())
}
