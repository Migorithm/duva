use crate::services::cluster::peers::identifier::PeerIdentifier;
use crate::{from_to, make_smart_pointer};

make_smart_pointer!(PeerAddrs, Vec<PeerIdentifier>);
from_to!(Vec<PeerIdentifier>, PeerAddrs);

pub struct PeerAddrs(pub Vec<PeerIdentifier>);

impl PeerAddrs {
    pub fn stringify(self) -> String {
        self.0.into_iter().map(|x| x.0).collect::<Vec<String>>().join(" ")
    }
}
