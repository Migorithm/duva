use crate::domains::peers::identifier::PeerIdentifier;

#[derive(Debug, Clone, PartialEq, Eq, Default, bincode::Decode, bincode::Encode)]
pub struct AuthRequest {
    pub client_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, bincode::Decode, bincode::Encode)]
pub struct AuthResponse {
    pub client_id: String,
    pub request_id: u64,
    pub cluster_nodes: Vec<PeerIdentifier>,
}
