use crate::services::cluster::replications::replication::HeartBeatMessage;
use crate::services::query_io::QueryIO;
use bytes::Bytes;

#[derive(Debug)]
pub enum RequestFromMaster {
    HeartBeat(HeartBeatMessage),
    FullSync(Bytes),
}

pub enum RequestFromSlave {
    HeartBeat(HeartBeatMessage),
    Acks(Vec<u64>),
}

impl TryFrom<QueryIO> for RequestFromMaster {
    type Error = anyhow::Error;
    fn try_from(query: QueryIO) -> anyhow::Result<Self> {
        match query {
            QueryIO::File(data) => Ok(Self::FullSync(data)),
            QueryIO::HeartBeat(peer_state) => Ok(Self::HeartBeat(peer_state)),
            // TODO term info should be included?
            _ => todo!(),
        }
    }
}

impl TryFrom<QueryIO> for RequestFromSlave {
    type Error = anyhow::Error;
    fn try_from(query: QueryIO) -> anyhow::Result<Self> {
        match query {
            QueryIO::HeartBeat(peer_state) => Ok(RequestFromSlave::HeartBeat(peer_state)),
            QueryIO::Acks(acks) => Ok(RequestFromSlave::Acks(acks)),
            _ => todo!(),
        }
    }
}
