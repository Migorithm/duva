use std::time::Duration;

use crate::adapters::io::tokio_stream::TokioConnectStreamFactory;
use crate::services::cluster::actor::{ClusterActor, PeerAddr};

use crate::services::cluster::command::ClusterCommand;
use crate::services::stream_manager::interface::{TConnectStreamFactory, TExtractQuery, TStream};
use crate::services::stream_manager::query_io::QueryIO;
use crate::services::stream_manager::request_controller::replica::arguments::PeerRequestArguments;
use crate::services::stream_manager::request_controller::replica::replication_request::{
    HandShakeRequest, ReplicationRequest,
};
use tokio::sync::mpsc::Sender;
use tokio::task::yield_now;
use tokio::time::interval;

#[derive(Clone)]
pub struct ClusterManager(Sender<ClusterCommand>);

impl ClusterManager {
    pub fn run(actor: ClusterActor) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tokio::spawn(actor.handle(rx));
        Self(tx)
    }

    pub(crate) async fn get_peers(&self) -> anyhow::Result<Vec<PeerAddr>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.0.send(ClusterCommand::GetPeers(tx)).await?;
        let peers = rx.await?;
        Ok(peers)
    }

    // Incoming connection is either replica or cluster peer stream.
    // 1) If the incoming connection is a replica peer stream, the system will do the following:
    // - establish a three-way handshake
    // - establish a primary-replica relationship
    // - let the requesting peer know the other peers so they can connect
    // - start the replication process
    //
    // 2) If the incoming connection is a cluster peer stream, the system will do the following:
    // - establish a three-way handshake
    // - Check if the process already has a connection with the requesting peer
    // - If not, establish a connection with the requesting peer
    // - Let the requesting peer know the other peers so they can connect
    pub(crate) async fn accept_peer(
        &self,
        mut peer_stream: impl TExtractQuery<HandShakeRequest, PeerRequestArguments>
            + TExtractQuery<ReplicationRequest, PeerRequestArguments>
            + TStream,
    ) {
        let peer_addr = self
            .establish_threeway_handshake(&mut peer_stream)
            .await
            .unwrap();
        // Send the peer address to the query manager to be used for replication.
        self.disseminate_peers(&mut peer_stream).await.unwrap();

        self.schedule_heartbeat(peer_addr).await.unwrap();

        // Following infinite loop may need to be changed once replica information is given
        self.handle_replication_request(&mut peer_stream)
            .await
            .unwrap();
    }

    // TODO subject to change! naming is not quite right
    async fn handle_replication_request(
        &self,
        stream: &mut (impl TExtractQuery<ReplicationRequest, PeerRequestArguments> + TStream),
    ) -> anyhow::Result<()> {
        loop {
            let Ok((ReplicationRequest, PeerRequestArguments(args))) = stream.extract_query().await
            else {
                eprintln!("invalid user request");
                continue;
            };

            // * Having error from the following will not a concern as liveness concern is on cluster manager
            // let _ = match self.controller.handle(request, query_args).await {
            //     Ok(response) => self.send(response).await,
            //     Err(e) => self.send(QueryIO::Err(e.to_string())).await,
            // };
        }
    }

    async fn establish_threeway_handshake(
        &self,
        stream: &mut (impl TExtractQuery<HandShakeRequest, PeerRequestArguments> + TStream),
    ) -> anyhow::Result<PeerAddr> {
        self.handle_ping(stream).await?;

        let port = self.handle_replconf_listening_port(stream).await?;

        // TODO find use of capa?
        let _capa_val_vec = self.handle_replconf_capa(stream).await?;

        // TODO find use of psync info?
        let (_repl_id, _offset) = self.handle_psync(stream).await?;

        // ! TODO: STRANGE BEHAVIOUR
        // if not for the following, message is sent with the previosly sent message
        // even with this, it shows flaking behaviour
        yield_now().await;

        Ok(PeerAddr(format!(
            "{}:{}",
            stream.get_peer_ip()?,
            port + 10000
        )))
    }
    async fn handle_ping(
        &self,
        stream: &mut (impl TExtractQuery<HandShakeRequest, PeerRequestArguments> + TStream),
    ) -> anyhow::Result<()> {
        let Ok((HandShakeRequest::Ping, _)) = stream.extract_query().await else {
            return Err(anyhow::anyhow!("Ping not given"));
        };
        stream
            .write(QueryIO::SimpleString("PONG".to_string()))
            .await?;
        Ok(())
    }

    async fn handle_replconf_listening_port(
        &self,
        stream: &mut (impl TExtractQuery<HandShakeRequest, PeerRequestArguments> + TStream),
    ) -> anyhow::Result<i16> {
        let (HandShakeRequest::ReplConf, query_args) = stream.extract_query().await? else {
            return Err(anyhow::anyhow!("ReplConf not given during handshake"));
        };
        let port = if query_args.first() == Some(&QueryIO::BulkString("listening-port".to_string()))
        {
            query_args.take_replica_port()?
        } else {
            return Err(anyhow::anyhow!("Invalid listening-port given"));
        };
        stream
            .write(QueryIO::SimpleString("OK".to_string()))
            .await?;

        Ok(port.parse::<i16>()?)
    }

    async fn handle_replconf_capa(
        &self,
        stream: &mut (impl TExtractQuery<HandShakeRequest, PeerRequestArguments> + TStream),
    ) -> anyhow::Result<Vec<(String, String)>> {
        let (HandShakeRequest::ReplConf, query_args) = stream.extract_query().await? else {
            return Err(anyhow::anyhow!("ReplConf not given during handshake"));
        };
        let capa_val_vec = query_args.take_capabilities()?;
        stream
            .write(QueryIO::SimpleString("OK".to_string()))
            .await?;

        Ok(capa_val_vec)
    }
    async fn handle_psync(
        &self,
        stream: &mut (impl TExtractQuery<HandShakeRequest, PeerRequestArguments> + TStream),
    ) -> anyhow::Result<(String, i64)> {
        let (HandShakeRequest::Psync, query_args) = stream.extract_query().await? else {
            return Err(anyhow::anyhow!("Psync not given during handshake"));
        };
        let (repl_id, offset) = query_args.take_psync()?;
        stream
            .write(QueryIO::SimpleString(
                "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0".to_string(),
            ))
            .await?;

        Ok((repl_id, offset))
    }

    pub async fn disseminate_peers(
        &self,
        stream: &mut (impl TExtractQuery<HandShakeRequest, PeerRequestArguments> + TStream),
    ) -> anyhow::Result<()> {
        let peers = self.get_peers().await?;
        if peers.is_empty() {
            return Ok(());
        }

        stream
            .write(QueryIO::SimpleString(format!(
                "PEERS {}",
                peers
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(" ")
            )))
            .await?;
        Ok(())
    }

    async fn schedule_heartbeat(&self, peer_addr: PeerAddr) -> anyhow::Result<()> {
        // 1000 mills just because that's default for Redis.
        const HEARTBEAT_INTERVAL: u64 = 1000;
        let mut stream = TokioConnectStreamFactory.connect(peer_addr).await?;
        let mut interval = interval(Duration::from_millis(HEARTBEAT_INTERVAL));
        tokio::spawn(async move {
            loop {
                interval.tick().await;
                if let Err(err) = stream
                    .write(QueryIO::SimpleString("PING".to_string()))
                    .await
                {
                    eprintln!("Error sending heartbeat: {:?}", err);
                    // TODO add more logic
                    break;
                }
            }
        });
        Ok(())
    }
}
