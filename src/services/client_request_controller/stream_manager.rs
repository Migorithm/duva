use crate::{
    make_smart_pointer,
    services::{
        interface::{TCancellationNotifier, TCancellationTokenFactory, TStream},
        query_io::QueryIO,
    },
};
use anyhow::Context;

use tokio::net::TcpStream;

use super::{arguments::ClientRequestArguments, request::ClientRequest, ClientRequestController};

pub struct ClientStream(pub(crate) TcpStream);
make_smart_pointer!(ClientStream, TcpStream);

impl ClientStream {
    async fn extract_query(&mut self) -> anyhow::Result<(ClientRequest, ClientRequestArguments)> {
        let query_io = self.read_value().await?;
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

    pub(crate) async fn handle_single_client_stream(
        mut self,
        cancellation_token_factory: impl TCancellationTokenFactory,
        controller: &'static ClientRequestController,
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

            let res = match controller
                .handle(cancellation_token, request, query_args)
                .await
            {
                Ok(response) => self.write(response).await,
                Err(e) => self.write(QueryIO::Err(e.to_string())).await,
            };

            if let Err(e) = res {
                if e.should_break() {
                    break;
                }
            }
        }
    }
}
