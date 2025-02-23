use std::collections::HashMap;

use crate::domains::peers::identifier::PeerIdentifier;

#[derive(Default)]
pub struct CommitTracker(HashMap<PeerIdentifier, u64>); // commit index
