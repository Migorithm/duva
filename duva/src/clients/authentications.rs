#[derive(Debug, Clone, PartialEq, Eq, bincode::Decode, bincode::Encode)]
pub enum AuthRequest {
    ConnectWithId(String),
    ConnectWithoutId,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, bincode::Decode, bincode::Encode)]
pub struct AuthResponse {
    pub client_id: String,
    pub request_id: u64,
}
