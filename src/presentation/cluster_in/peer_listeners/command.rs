use crate::services::cluster::replications::replication::HeartBeatMessage;
use crate::services::query_io::QueryIO;

#[derive(Debug)]
pub enum CommandFromMaster {
    HeartBeat(HeartBeatMessage),
    Sync(QueryIO),
}

pub enum CommandFromSlave {
    HeartBeat(HeartBeatMessage),
}

impl TryFrom<QueryIO> for CommandFromMaster {
    type Error = anyhow::Error;
    fn try_from(query: QueryIO) -> anyhow::Result<Self> {
        match query {
            file @ QueryIO::File(_) => Ok(Self::Sync(file)),
            QueryIO::HeartBeat(peer_state) => Ok(Self::HeartBeat(peer_state)),
            // TODO term info should be included?
            _ => todo!(),
        }
    }
}

impl TryFrom<QueryIO> for CommandFromSlave {
    type Error = anyhow::Error;
    fn try_from(query: QueryIO) -> anyhow::Result<Self> {
        match query {
            QueryIO::HeartBeat(peer_state) => Ok(CommandFromSlave::HeartBeat(peer_state)),
            _ => todo!(),
        }
    }
}
