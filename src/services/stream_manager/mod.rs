pub mod error;
pub mod interface;
pub mod query_io;
pub(crate) mod request_controller;
use super::cluster::actor::PeerAddr;
use error::IoError;
use interface::TCancellationNotifier;
use interface::TCancellationTokenFactory;
use interface::TExtractQuery;
use interface::TStream;
use interface::TWriterFactory;
use query_io::QueryIO;
use request_controller::client::arguments::ClientRequestArguments;
use request_controller::client::client_request::ClientRequest;
use request_controller::client::ClientRequestController;

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
    T: TStream + TExtractQuery<ClientRequest, ClientRequestArguments>,
{
    pub(crate) async fn handle_single_client_stream<F: TWriterFactory>(
        mut self,
        cancellation_token_factory: impl TCancellationTokenFactory,
    ) {
        const TIMEOUT: u64 = 100;

        loop {
            let Ok((request, query_args)) = self.stream.extract_query().await else {
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
