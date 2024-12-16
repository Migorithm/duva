pub mod client_request_controllers;
pub mod error;
pub mod interface;
pub mod query_io;
pub mod replication_request_controllers;
use std::time::Duration;

use crate::services::stream_manager::client_request_controllers::client_request::ClientRequest;
use crate::services::stream_manager::client_request_controllers::ClientRequestController;

use anyhow::Context;
use client_request_controllers::arguments::ClientRequestArguments;
use error::IoError;
use interface::{
    TCancellationNotifier, TCancellationTokenFactory, TConnectStreamFactory, TStream,
    TWriterFactory,
};

use query_io::QueryIO;
use replication_request_controllers::{
    arguments::PeerRequestArguments, replication_request::HandShakeRequest,
    ReplicationRequestController,
};
use tokio::{task::yield_now, time::interval};

/// Controller is a struct that will be used to read and write values to the client.
pub struct StreamManager<T, U>
where
    T: TStream,
{
    pub(crate) stream: T,
    pub(crate) controller: U,
}

impl<T, U> StreamManager<T, U>
where
    T: TStream,
{
    pub(crate) fn new(stream: T, controller: U) -> Self {
        StreamManager { stream, controller }
    }

    pub async fn send(&mut self, value: QueryIO) -> Result<(), IoError> {
        self.stream.write(value).await
    }
}

impl<T> StreamManager<T, &'static ClientRequestController>
where
    T: TStream,
{
    async fn extract_query(&mut self) -> anyhow::Result<(ClientRequest, ClientRequestArguments)> {
        let query_io = self.stream.read_value().await?;
        match query_io {
            QueryIO::Array(value_array) => Ok((
                value_array
                    .first()
                    .context("request not given")?
                    .clone()
                    .unpack_bulk_str()?
                    .try_into()?,
                ClientRequestArguments::new(value_array.into_iter().skip(1).collect()),
            )),
            _ => Err(anyhow::anyhow!("Unexpected command format")),
        }
    }

    pub async fn handle_single_client_stream<F: TWriterFactory>(
        mut self,
        cancellation_token_factory: impl TCancellationTokenFactory,
    ) {
        const TIMEOUT: u64 = 100;

        loop {
            let Ok((request, query_args)) = self.extract_query().await else {
                eprintln!("invalid user request");
                continue;
            };

            let (cancellation_notifier, cancellation_token) =
                cancellation_token_factory.create(TIMEOUT);

            // TODO subject to change - more to dynamic
            // Notify the cancellation notifier to cancel the query after 100 milliseconds.
            cancellation_notifier.notify();

            let res = match self
                .controller
                .handle::<F>(cancellation_token, request, query_args)
                .await
            {
                Ok(response) => self.send(response).await,
                Err(e) => self.send(QueryIO::Err(e.to_string())).await,
            };

            if let Err(e) = res {
                if e.should_break() {
                    break;
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct PeerAddr(pub String);
impl<T> StreamManager<T, &'static ReplicationRequestController>
where
    T: TStream,
{
    async fn extract_query<R>(&mut self) -> anyhow::Result<(R, PeerRequestArguments)>
    where
        R: TryFrom<String>,
        anyhow::Error: From<R::Error>,
    {
        let query_io = self.stream.read_value().await?;
        match query_io {
            // TODO refactor
            QueryIO::Array(value_array) => Ok((
                value_array
                    .first()
                    .context("request not given")?
                    .clone()
                    .unpack_bulk_str()?
                    .try_into()?,
                PeerRequestArguments::new(value_array.into_iter().skip(1).collect()),
            )),
            _ => Err(anyhow::anyhow!("Unexpected command format")),
        }
    }
    async fn send_simple_string(&mut self, value: &str) -> Result<(), IoError> {
        self.send(QueryIO::SimpleString(value.to_string())).await
    }

    // Temporal coupling: each handling functions inside the following method must be in order
    pub async fn establish_threeway_handshake(&mut self) -> anyhow::Result<PeerAddr> {
        self.handle_ping().await?;

        let port = self.handle_replconf_listening_port().await?;

        // TODO find use of capa?
        let _capa_val_vec = self.handle_replconf_capa().await?;

        // TODO find use of psync info?
        let (_repl_id, _offset) = self.handle_psync().await?;

        // ! TODO: STRANGE BEHAVIOUR
        // if not for the following, message is sent with the previosly sent message
        // even with this, it shows flaking behaviour
        yield_now().await;

        Ok(PeerAddr(format!(
            "{}:{}",
            self.stream.get_peer_ip()?,
            port + 10000
        )))
    }

    async fn handle_ping(&mut self) -> anyhow::Result<()> {
        let (HandShakeRequest::Ping, _) = self.extract_query().await? else {
            return Err(anyhow::anyhow!("Ping not given"));
        };
        self.send_simple_string("PONG").await?;
        Ok(())
    }
    async fn handle_replconf_listening_port(&mut self) -> anyhow::Result<i16> {
        let (HandShakeRequest::ReplConf, query_args) = self.extract_query().await? else {
            return Err(anyhow::anyhow!("ReplConf not given during handshake"));
        };
        let port = if query_args.first() == Some(&QueryIO::BulkString("listening-port".to_string()))
        {
            query_args.take_replica_port()?
        } else {
            return Err(anyhow::anyhow!("Invalid listening-port given"));
        };
        self.send_simple_string("OK").await?;

        Ok(port.parse::<i16>()?)
    }

    async fn handle_replconf_capa(&mut self) -> anyhow::Result<Vec<(String, String)>> {
        let (HandShakeRequest::ReplConf, query_args) = self.extract_query().await? else {
            return Err(anyhow::anyhow!("ReplConf not given during handshake"));
        };
        let capa_val_vec = query_args.take_capabilities()?;
        self.send_simple_string("OK").await?;

        Ok(capa_val_vec)
    }
    async fn handle_psync(&mut self) -> anyhow::Result<(String, i64)> {
        let (HandShakeRequest::Psync, query_args) = self.extract_query().await? else {
            return Err(anyhow::anyhow!("Psync not given during handshake"));
        };
        let (repl_id, offset) = query_args.take_psync()?;
        self.send_simple_string("FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0")
            .await?;

        Ok((repl_id, offset))
    }

    async fn get_peers(&self) -> anyhow::Result<Vec<String>> {
        let peers = self.controller.config_manager.get_peers().await?;
        Ok(peers)
    }
    pub async fn disseminate_peers(&mut self) -> anyhow::Result<()> {
        let peers = self.get_peers().await?;
        if peers.is_empty() {
            return Ok(());
        }

        self.send_simple_string(
            format!(
                "PEERS {}",
                peers
                    .iter()
                    .map(|peer| peer.to_string())
                    .collect::<Vec<String>>()
                    .join(" ")
            )
            .as_str(),
        )
        .await?;

        Ok(())
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
    pub(crate) async fn handle_peer_stream(
        mut self,
        connect_stream_factory: impl TConnectStreamFactory,
    ) -> anyhow::Result<()> {
        let peer_addr = self.establish_threeway_handshake().await.unwrap();

        // Send the peer address to the query manager to be used for replication.
        self.disseminate_peers().await.unwrap();

        self.schedule_heartbeat(connect_stream_factory, peer_addr)
            .await?;

        // Following infinite loop may need to be changed once replica information is given
        loop {
            let (request, query_args) = self.extract_query().await?;

            // * Having error from the following will not a concern as liveness concern is on cluster manager
            let _ = match self.controller.handle(request, query_args).await {
                Ok(response) => self.send(response).await,
                Err(e) => self.send(QueryIO::Err(e.to_string())).await,
            };
        }
    }

    async fn schedule_heartbeat(
        &mut self,
        connect_stream_factory: impl TConnectStreamFactory,
        peer_addr: PeerAddr,
    ) -> anyhow::Result<()> {
        // 1000 mills just because that's default for Redis.
        const HEARTBEAT_INTERVAL: u64 = 1000;
        let mut stream = connect_stream_factory.connect(peer_addr).await?;
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
