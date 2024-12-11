pub mod client_request_controllers;
pub mod error;
pub mod interface;
pub mod query_io;
pub mod replication_request_controllers;
use crate::services::query_manager::client_request_controllers::client_request::ClientRequest;
use crate::services::query_manager::client_request_controllers::ClientRequestController;

use anyhow::Context;
use bytes::BytesMut;
use client_request_controllers::arguments::ClientRequestArguments;
use error::IoError;
use interface::{
    TCancellationNotifier, TCancellationTokenFactory, TConnectStreamFactory, TStream,
    TWriterFactory,
};

use query_io::QueryIO;
use replication_request_controllers::{
    arguments::ReplicationRequestArguments, replication_request::ReplicationRequest,
    ReplicationRequestController,
};

/// Controller is a struct that will be used to read and write values to the client.
pub struct QueryManager<T, U>
where
    T: TStream,
{
    pub(crate) stream: T,
    pub(crate) controller: U,
}

impl<T, U> QueryManager<T, U>
where
    T: TStream,
{
    pub(crate) fn new(stream: T, controller: U) -> Self {
        QueryManager { stream, controller }
    }

    // crlf
    pub async fn read_value(&mut self) -> anyhow::Result<QueryIO> {
        let mut buffer = BytesMut::with_capacity(512);
        self.stream.read_bytes(&mut buffer).await?;

        let (query_io, _) = query_io::parse(buffer)?;
        Ok(query_io)
    }

    pub async fn write_value(&mut self, value: QueryIO) -> Result<(), IoError> {
        self.stream.write_all(value.serialize().as_bytes()).await?;
        Ok(())
    }
}

impl<T> QueryManager<T, &'static ClientRequestController>
where
    T: TStream,
{
    async fn extract_query(&mut self) -> anyhow::Result<(ClientRequest, ClientRequestArguments)> {
        let query_io = self.read_value().await?;
        match query_io {
            // TODO refactor
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
        cancellation_token_factory: impl TCancellationTokenFactory,
        stream: T,
        controller: &'static ClientRequestController,
    ) {
        const TIMEOUT: u64 = 100;
        let mut query_manager = QueryManager::new(stream, controller);

        loop {
            let Ok((request, query_args)) = query_manager.extract_query().await else {
                eprintln!("invalid user request");
                continue;
            };

            let (cancellation_notifier, cancellation_token) =
                cancellation_token_factory.create(TIMEOUT);

            // TODO subject to change - more to dynamic
            // Notify the cancellation notifier to cancel the query after 100 milliseconds.
            cancellation_notifier.notify();

            let res = match query_manager
                .controller
                .handle::<F>(cancellation_token, request, query_args)
                .await
            {
                Ok(response) => query_manager.write_value(response).await,
                Err(e) => query_manager.write_value(QueryIO::Err(e.to_string())).await,
            };

            if let Err(e) = res {
                if e.should_break() {
                    break;
                }
            }
        }
    }
}

pub struct PeerAddr(pub String);
impl<T> QueryManager<T, &'static ReplicationRequestController>
where
    T: TStream,
{
    async fn extract_query(
        &mut self,
    ) -> anyhow::Result<(ReplicationRequest, ReplicationRequestArguments)> {
        let query_io = self.read_value().await?;
        match query_io {
            // TODO refactor
            QueryIO::Array(value_array) => Ok((
                value_array
                    .first()
                    .context("request not given")?
                    .clone()
                    .unpack_bulk_str()?
                    .try_into()?,
                ReplicationRequestArguments::new(value_array.into_iter().skip(1).collect()),
            )),
            _ => Err(anyhow::anyhow!("Unexpected command format")),
        }
    }

    async fn establish_threeway_handshake(&self) -> anyhow::Result<PeerAddr> {
        //TODO replace the following
        let peer_port = todo!();
        let peer_addr = self.get_peer_addr(peer_port)?;
        Ok(peer_addr)
    }

    fn get_peer_addr(&self, port: i16) -> anyhow::Result<PeerAddr> {
        Ok(PeerAddr(format!("{}:{}", self.stream.get_peer_ip()?, port)))
    }

    pub(crate) async fn handle_peer_stream(
        connect_stream_factory: impl TConnectStreamFactory,
        in_stream: T,
        controller: &'static ReplicationRequestController,
    ) -> anyhow::Result<()> {
        let mut query_manager = QueryManager::new(in_stream, controller);

        // TODO Three way handshake
        let peer_addr = query_manager.establish_threeway_handshake().await?;

        // TODO Stream factory DI - p3
        let out_stream = connect_stream_factory.connect(peer_addr).await?;

        // Following infinite loop may need to be changed once replica information is given
        loop {
            let (request, query_args) = query_manager.extract_query().await?;

            // * Having error from the following will not a concern as liveness concern is on cluster manager
            let _ = match query_manager.controller.handle(request, query_args).await {
                Ok(response) => query_manager.write_value(response).await,
                Err(e) => query_manager.write_value(QueryIO::Err(e.to_string())).await,
            };
        }
    }
}
