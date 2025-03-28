#[derive(Debug, Clone, PartialEq, Eq, bincode::Decode, bincode::Encode)]
pub enum AuthRequest {
    ConnectWithId(String),
    ConnectWithoutId,
}

#[derive(Debug, Clone, PartialEq, Eq, bincode::Decode, bincode::Encode)]
pub enum AuthResponse {
    ClientId(String),
}
