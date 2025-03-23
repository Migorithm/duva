#[derive(Debug)]
pub enum ConsensusClientResponse {
    LogIndex(Option<u64>),
    Err(String),
}

#[derive(Debug, Clone, PartialEq, bincode::Decode, bincode::Encode)]
pub(crate) struct ConsensusFollowerResponse {
    pub(crate) log_idx: u64,
    pub(crate) term: u64,
    pub(crate) is_granted: bool,
}
