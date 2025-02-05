use bytes::Bytes;

use crate::services::config::init::get_env;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;

use crate::services::cluster::peers::identifier::PeerIdentifier;
use crate::services::cluster::replications::replid_generator::generate_replid;

pub static IS_MASTER_MODE: AtomicBool = AtomicBool::new(true);

#[derive(Debug, Clone)]
pub struct Replication {
    pub(crate) connected_slaves: u16, // The number of connected replicas
    pub(crate) master_replid: String, // The replication ID of the master example: 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb
    pub(crate) master_repl_offset: u64, // The server's current replication offset. Example: 0
    second_repl_offset: i16,          // -1
    repl_backlog_active: usize,       // 0
    repl_backlog_size: usize,         // 1048576
    repl_backlog_first_byte_offset: usize, // 0
    role: String,
    //If the instance is a replica, these additional fields are provided:
    pub(crate) master_host: Option<String>,
    pub(crate) master_port: Option<u16>,

    // * state is shared among peers
    pub(crate) term: u64,
    pub(crate) self_identifier: PeerIdentifier,
    pub(crate) ban_list: Vec<BannedPeer>,
}

impl Default for Replication {
    fn default() -> Self {
        let env = get_env();
        let replication = Replication::new(env.replicaof.clone(), &env.host, env.port);
        IS_MASTER_MODE
            .store(replication.master_port.is_none(), std::sync::atomic::Ordering::Relaxed);
        replication
    }
}

impl Replication {
    pub fn new(replicaof: Option<(String, String)>, self_host: &str, self_port: u16) -> Self {
        let master_replid = if replicaof.is_none() { generate_replid() } else { "?".to_string() };

        Replication {
            connected_slaves: 0, // dynamically configurable
            master_replid: master_replid.clone(),
            master_repl_offset: 0,
            second_repl_offset: -1,
            repl_backlog_active: 0,
            repl_backlog_size: 1048576,
            repl_backlog_first_byte_offset: 0,
            role: if replicaof.is_some() { "slave".to_string() } else { "master".to_string() },
            master_host: replicaof.as_ref().cloned().map(|(host, _)| host),
            master_port: replicaof
                .map(|(_, port)| port.parse().expect("Invalid port number of given")),
            term: 0,
            self_identifier: PeerIdentifier::new(self_host, self_port),
            ban_list: Default::default(),
        }
    }
    pub fn vectorize(self) -> Vec<String> {
        vec![
            format!("role:{}", self.role),
            format!("connected_slaves:{}", self.connected_slaves),
            format!("master_replid:{}", self.master_replid),
            format!("master_repl_offset:{}", self.master_repl_offset),
            format!("second_repl_offset:{}", self.second_repl_offset),
            format!("repl_backlog_active:{}", self.repl_backlog_active),
            format!("repl_backlog_size:{}", self.repl_backlog_size),
            format!("repl_backlog_first_byte_offset:{}", self.repl_backlog_first_byte_offset),
            format!("self_identifier:{}", &*self.self_identifier),
        ]
    }

    pub fn master_bind_addr(&self) -> PeerIdentifier {
        format!("{}:{}", self.master_host.as_ref().unwrap(), self.master_port.unwrap()).into()
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
    pub fn current_state(&self, hop_count: u8) -> PeerState {
        PeerState {
            heartbeat_from: self.self_identifier.clone(),
            term: self.term,
            offset: self.master_repl_offset,
            master_replid: self.master_replid.clone(),
            hop_count,
            ban_list: self.ban_list.clone(),
        }
    }

    // ! how do we remove peer from ban list when it comes from outside?
    pub(crate) fn merge_ban_list(&mut self, ban_list: Vec<BannedPeer>) {
        if ban_list.is_empty() {
            return;
        }

        // merge, deduplicate and retain the latest
        self.ban_list.extend(ban_list);
        self.ban_list.sort_by_key(|node| (node.p_id.clone(), std::cmp::Reverse(node.ban_time)));
        self.ban_list.dedup_by_key(|node| node.p_id.clone());
    }

    pub(crate) fn ban_peer(&mut self, p_id: &PeerIdentifier) -> anyhow::Result<()> {
        self.ban_list.push(BannedPeer { p_id: p_id.clone(), ban_time: time_in_secs()? });
        Ok(())
    }
}

pub(crate) fn time_in_secs() -> anyhow::Result<u64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs())
}
#[derive(Debug, Clone, PartialEq)]
pub struct PeerState {
    pub(crate) heartbeat_from: PeerIdentifier,
    pub(crate) term: u64,
    pub(crate) offset: u64,
    pub(crate) master_replid: String,
    pub(crate) hop_count: u8, // Decremented on each hop - for gossip
    pub(crate) ban_list: Vec<BannedPeer>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BannedPeer {
    pub(crate) p_id: PeerIdentifier,
    pub(crate) ban_time: u64,
}

impl FromStr for BannedPeer {
    type Err = std::io::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split('-');
        let p_id = parts.next().unwrap().parse::<PeerIdentifier>()?;
        let ban_time = parts.next().unwrap().parse::<u64>().unwrap();
        Ok(BannedPeer { p_id, ban_time })
    }
}

impl From<BannedPeer> for Bytes {
    fn from(banned_peer: BannedPeer) -> Self {
        Bytes::from(format!("{}-{}", banned_peer.p_id, banned_peer.ban_time))
    }
}
