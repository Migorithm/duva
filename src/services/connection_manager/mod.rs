pub mod error;
pub mod establishment;
pub mod interface;
pub mod query_io;
pub(crate) mod request_controller;
use anyhow::Context;
use error::IoError;
use interface::TCancellationNotifier;
use interface::TCancellationTokenFactory;

use interface::TStream;
use interface::TWriterFactory;
use query_io::QueryIO;
use request_controller::client::arguments::ClientRequestArguments;
use request_controller::client::client_request::ClientRequest;
use request_controller::client::ClientRequestController;
use tokio::net::TcpStream;

use crate::make_smart_pointer;

/// Controller is a struct that will be used to read and write values to the client.
pub struct ClientStreamManager<U> {
    pub(crate) stream: ClientStream,
    pub(crate) controller: U,
}

pub struct ClientStream(pub(crate) TcpStream);
make_smart_pointer!(ClientStream, TcpStream);

impl<U> ClientStreamManager<U> {
    pub(crate) fn new(stream: ClientStream, controller: U) -> Self {
        ClientStreamManager { stream, controller }
    }

    pub async fn send(&mut self, value: QueryIO) -> Result<(), IoError> {
        self.stream.write(value).await
    }
}

impl ClientStreamManager<&'static ClientRequestController> {
    pub(crate) async fn handle_single_client_stream<F: TWriterFactory>(
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
}
