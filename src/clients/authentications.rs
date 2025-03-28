#[derive(Debug, Clone, PartialEq, Eq, bincode::Decode, bincode::Encode)]
pub enum AuthRequest {
    ClientIdExists,
    ClientIdNotExists,
}

#[derive(Debug, Clone, PartialEq, Eq, bincode::Decode, bincode::Encode)]
pub enum AuthResponse {
    ClientId(String),
}
