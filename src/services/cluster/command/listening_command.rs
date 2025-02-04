use crate::services::cluster::replications::replication::PeerState;
use crate::services::query_io::QueryIO;

#[derive(Debug)]
pub enum CommandFromMaster {
    HeartBeat(PeerState),
    Replicate { query: QueryIO },
    Sync(QueryIO),
}

pub enum CommandFromSlave {
    HeartBeat(PeerState),
}

impl TryFrom<QueryIO> for CommandFromMaster {
    type Error = anyhow::Error;
    fn try_from(query: QueryIO) -> anyhow::Result<Self> {
        match query {
            file @ QueryIO::File(_) => Ok(Self::Sync(file)),
            QueryIO::PeerState(peer_state) => Ok(Self::HeartBeat(peer_state)),
            _ => todo!(),
        }
    }
}

impl TryFrom<QueryIO> for CommandFromSlave {
    type Error = anyhow::Error;
    fn try_from(query: QueryIO) -> anyhow::Result<Self> {
        match query {
            QueryIO::PeerState(peer_state) => Ok(CommandFromSlave::HeartBeat(peer_state)),
            _ => todo!(),
        }
    }
}
