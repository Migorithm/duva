use crate::{from_to, make_smart_pointer};

#[derive(
    Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash, Default, bincode::Encode, bincode::Decode,
)]
pub(crate) struct PeerIdentifier(pub String);
impl PeerIdentifier {
    pub(crate) fn new(host: &str, port: u16) -> Self {
        Self(format!("{}:{}", host, port))
    }

    pub(crate) fn cluster_bind_addr(&self) -> String {
        self.0
            .rsplit_once(':')
            .map(|(host, port)| {
                format!("{}:{}", parse_address(host).unwrap(), port.parse::<u16>().unwrap() + 10000)
            })
            .unwrap()
    }
}

fn parse_address(addr: &str) -> Option<std::net::IpAddr> {
    match addr.to_lowercase().as_str() {
        // IPv4 127.0.0.1 variants
        "127.0.0.1" | "localhost" => {
            Some(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)))
        },
        // IPv6 127.0.0.1 variants
        "::1" | "[::1]" | "0:0:0:0:0:0:0:1" => {
            Some(std::net::IpAddr::V6(std::net::Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)))
        },
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
