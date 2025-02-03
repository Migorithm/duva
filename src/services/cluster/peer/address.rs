use crate::services::cluster::peer::identifier::PeerIdentifier;
use crate::{from_to, make_smart_pointer};

make_smart_pointer!(PeerAddrs, Vec<PeerIdentifier>);
from_to!(Vec<PeerIdentifier>, PeerAddrs);

pub struct PeerAddrs(pub Vec<PeerIdentifier>);

impl PeerAddrs {
    pub fn stringify(self) -> String {
        self.0.into_iter().map(|x| x.0).collect::<Vec<String>>().join(" ")
    }
}

pub fn parse_address(addr: &str) -> Option<std::net::IpAddr> {
    match addr.to_lowercase().as_str() {
        // IPv4 127.0.0.1 variants
        "127.0.0.1" => {
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
