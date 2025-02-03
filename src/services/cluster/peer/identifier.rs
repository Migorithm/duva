use crate::services::cluster::peer::address;
use crate::{from_to, make_smart_pointer};
use std::str::FromStr;

make_smart_pointer!(PeerIdentifier, String);
from_to!(String, PeerIdentifier);

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
                format!("{}:{}", address::parse_address(host).unwrap(), port.parse::<u16>().unwrap() + 10000)
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

impl std::fmt::Display for PeerIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.0
                .rsplit_once(':')
                .map(|(host, port)| {
                    format!("{}:{}", address::parse_address(host).unwrap(), port.parse::<u16>().unwrap())
                })
                .unwrap()
        )
    }
}

#[test]
fn test_peer_identifier() {
    let peer = PeerIdentifier::new("127.0.0.1", 6379);
    assert_eq!(peer.cluster_bind_addr(), "127.0.0.1:16379"); // 127.0.0.1:6379 + 10000
    assert_eq!(peer.to_string(), "127.0.0.1:6379"); // overriden by display
    assert_eq!(peer, "127.0.0.1:6379".parse().unwrap());
}
