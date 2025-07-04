use crate::make_smart_pointer;

#[derive(
    Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash, Default, bincode::Encode, bincode::Decode,
)]
pub struct PeerIdentifier(pub String);
impl PeerIdentifier {
    pub(crate) fn new(host: &str, port: u16) -> Self {
        parse_address(host).map(|ip| Self(format!("{ip}:{port}"))).unwrap()
    }
}

pub trait TPeerAddress {
    fn bind_addr(&self) -> anyhow::Result<String>;
    fn cluster_bind_addr(&self) -> anyhow::Result<String>;
}

impl<T: AsRef<str>> TPeerAddress for T {
    fn bind_addr(&self) -> anyhow::Result<String> {
        let (host, port) = extract_host_and_port(self.as_ref())?;
        Ok(format!("{host}:{port}"))
    }
    fn cluster_bind_addr(&self) -> anyhow::Result<String> {
        let (host, port) = extract_host_and_port(self.as_ref())?;
        Ok(format!("{}:{}", host, port + 10000))
    }
}

fn extract_host_and_port(addr: &str) -> anyhow::Result<(std::net::IpAddr, u16)> {
    let (host, port_str) =
        addr.rsplit_once(':').ok_or_else(|| anyhow::anyhow!("Invalid address format"))?;
    let host = parse_address(host)?;
    let port = port_str.parse::<u16>()?;
    Ok((host, port))
}

fn parse_address(addr: &str) -> anyhow::Result<std::net::IpAddr> {
    match addr.to_lowercase().as_str() {
        // IPv4 127.0.0.1 variants
        | "127.0.0.1" | "localhost" => {
            Ok(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)))
        },
        // IPv6 127.0.0.1 variants
        | "::1" | "[::1]" | "0:0:0:0:0:0:0:1" => {
            Ok(std::net::IpAddr::V6(std::net::Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)))
        },
        // Try to parse anything else as an IP address
        | other => other.parse().map_err(|_| {
            anyhow::anyhow!(
                "Invalid address: {}. Expected a valid IP address or 'localhost'.",
                other
            )
        }),
    }
}

make_smart_pointer!(PeerIdentifier, String);

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

#[test]
fn test_peer_identifier() {
    let peer = PeerIdentifier::new("127.0.0.1", 1234);
    let peer2 = PeerIdentifier::new("127.0.0.3", 1234);

    assert!(peer < peer2);
}

#[test]
fn test_peer_identifier2() {
    let peer = PeerIdentifier::new("127.0.0.1", 1234);
    let peer2 = PeerIdentifier::new("127.0.0.1", 1235);

    assert!(peer < peer2);
}
