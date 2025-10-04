use crate::{
    domains::{
        peers::identifier::TPeerAddress,
        replications::{ReplicationId, ReplicationRole},
    },
    prelude::PeerIdentifier,
};

#[derive(Default, Clone, Debug, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub struct ReplicationState {
    pub node_id: PeerIdentifier,
    pub replid: ReplicationId,
    pub role: ReplicationRole,
    pub(crate) last_log_index: u64,
    pub(crate) term: u64,
}

impl ReplicationState {
    pub(crate) fn decide_peer_state(self, my_repl_id: &ReplicationId) -> Self {
        let replid = match (&self.replid, my_repl_id) {
            | (ReplicationId::Undecided, _) => my_repl_id.clone(),
            | (_, ReplicationId::Undecided) => self.replid.clone(),
            | _ => self.replid.clone(),
        };
        Self { replid, ..self }
    }

    pub(crate) fn id(&self) -> &PeerIdentifier {
        &self.node_id
    }

    pub(crate) fn parse_node_info(line: &str) -> Option<Self> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() != 5 {
            return None;
        }

        let [addr, id_part, term, log_index, role] = parts[..] else {
            return None;
        };

        let repl_id = Self::extract_replid(id_part)?;

        let log_index = log_index.parse().unwrap_or_default();
        let term = term.parse().unwrap_or_default();

        Some(Self {
            node_id: PeerIdentifier(addr.bind_addr().unwrap()),
            replid: repl_id.into(),
            last_log_index: log_index,
            role: role.to_string().into(),
            term,
        })
    }

    fn extract_replid(id_part: &str) -> Option<String> {
        if id_part.contains("myself,") {
            Some(id_part[7..].to_string())
        } else {
            Some(id_part.to_string())
        }
    }

    pub(crate) fn from_file(path: &str) -> Vec<Self> {
        let Some(contents) = Self::read_file_if_valid(path) else {
            return vec![];
        };

        let Some(my_repl_id) = Self::extract_my_repl_id(&contents) else {
            return vec![];
        };

        let mut nodes: Vec<Self> = contents
            .lines()
            .filter(|line| !line.trim().is_empty())
            .filter_map(Self::parse_node_info)
            .collect();

        nodes.sort_by_key(|n| n.replid.to_string() == my_repl_id);
        nodes
    }

    fn read_file_if_valid(path: &str) -> Option<String> {
        let metadata = std::fs::metadata(path).ok()?;
        let modified = metadata.modified().ok()?;

        if modified.elapsed().unwrap_or_default().as_secs() > 300 {
            return None;
        }

        std::fs::read_to_string(path).ok()
    }

    fn extract_my_repl_id(contents: &str) -> Option<String> {
        contents.lines().filter(|line| !line.trim().is_empty()).find_map(|line| {
            let parts: Vec<&str> = line.split_whitespace().collect();
            for part in parts {
                if part.contains("myself,") {
                    return Some(part[7..].to_string());
                }
            }
            None
        })
    }

    pub(crate) fn format(&self, peer_id: &PeerIdentifier) -> String {
        if self.node_id == *peer_id {
            return format!(
                "{} myself,{} 0 {} {}",
                self.node_id, self.replid, self.last_log_index, self.role
            );
        }
        format!("{} {} 0 {} {}", self.node_id, self.replid, self.last_log_index, self.role)
    }

    pub(crate) fn is_self(&self, bind_addr: &str) -> bool {
        self.node_id.bind_addr().unwrap() == bind_addr
    }

    pub(crate) fn vectorize(self) -> Vec<String> {
        vec![
            format!("role:{}", self.role),
            format!("leader_repl_id:{}", self.replid),
            format!("last_log_index:{}", self.last_log_index),
            format!("self_identifier:{}", self.node_id),
        ]
    }
}
