use std::str::FromStr;

use crate::{from_to, make_smart_pointer};

use super::replication::Replication;

#[derive(Clone, Debug)]
pub enum PeerKind {
    Peer,
    Replica,
    Master,
}

impl PeerKind {
    pub fn accepted_peer_kind(self_repl_id: &str, other_repl_id: &str) -> Self {
        match other_repl_id {
            "?" => Self::Replica,
            id if id == self_repl_id => Self::Replica,
            _ => Self::Peer,
        }
    }
    pub fn connected_peer_kind(self_repl_info: &Replication, other_repl_id: &str) -> Self {
        if self_repl_info.master_replid == "?" {
            Self::Master
        } else if self_repl_info.master_replid == other_repl_id {
            Self::Replica
        } else {
            Self::Peer
        }
    }
}

// TODO refactor the following so it allows for host and port to be separate
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub struct PeerIdentifier(pub String);
impl PeerIdentifier {
    pub fn new(host: &str, port: u16) -> Self {
        Self(format!("{}:{}", host, port))
    }

    pub fn cluster_bind_addr(&self) -> String {
        self.0
            .rsplit_once(':')
            .map(|(host, port)| {
                format!("{}:{}", parse_address(host).unwrap(), port.parse::<u16>().unwrap() + 10000)
            })
            .unwrap()
    }
}

impl FromStr for PeerIdentifier {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

fn parse_address(addr: &str) -> Option<std::net::IpAddr> {
    match addr.to_lowercase().as_str() {
        // IPv4 127.0.0.1 variants
        "127.0.0.1" | "127.0.0.1" => {
            Some(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)))
        }
        // IPv6 127.0.0.1 variants
        "::1" | "[::1]" | "0:0:0:0:0:0:0:1" => {
            Some(std::net::IpAddr::V6(std::net::Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)))
        }
        // Try to parse anything else as an IP address
        other => other.parse().ok(),
    }
}

make_smart_pointer!(PeerIdentifier, String);
from_to!(String, PeerIdentifier);

impl std::fmt::Display for PeerIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.0
                .rsplit_once(':')
                .map(|(host, port)| {
                    format!("{}:{}", parse_address(host).unwrap(), port.parse::<u16>().unwrap())
                })
                .unwrap()
        )
    }
}

pub struct PeerAddrs(pub Vec<PeerIdentifier>);
make_smart_pointer!(PeerAddrs, Vec<PeerIdentifier>);
from_to!(Vec<PeerIdentifier>, PeerAddrs);
impl PeerAddrs {
    pub fn stringify(self) -> String {
        self.0.into_iter().map(|x| x.0).collect::<Vec<String>>().join(" ")
    }
}

#[test]
fn test_peer_identifier() {
    let peer = PeerIdentifier::new("127.0.0.1", 6379);
    assert_eq!(peer.cluster_bind_addr(), "127.0.0.1:16379"); // 127.0.0.1:6379 + 10000
    assert_eq!(peer.to_string(), "127.0.0.1:6379"); // overriden by display
    assert_eq!(peer, "127.0.0.1:6379".parse().unwrap());
}
