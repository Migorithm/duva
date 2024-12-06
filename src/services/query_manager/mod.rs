pub mod client_request_controllers;
pub mod interface;
mod query_arguments;
pub mod query_io;

use crate::services::query_manager::client_request_controllers::client_request::ClientRequest;
use crate::services::query_manager::client_request_controllers::ClientRequestController;
use crate::services::query_manager::interface::TCancellationWatcher;
use crate::services::statefuls::persist::endec::TEnDecoder;
use anyhow::{Context, Result};
use bytes::BytesMut;
use interface::{TRead, TWrite};
use query_arguments::QueryArguments;
use query_io::QueryIO;

/// Controller is a struct that will be used to read and write values to the client.
pub struct QueryManager<T, U>
where
    T: TWrite + TRead,
{
    pub(crate) stream: T,
    pub(crate) controller: U,
}

impl<T, U> QueryManager<T, U>
where
    T: TWrite + TRead,
{
    pub(crate) fn new(stream: T, controller: U) -> Self {
        QueryManager { stream, controller }
    }

    // crlf
    pub async fn read_value(&mut self) -> Result<QueryIO> {
        let mut buffer = BytesMut::with_capacity(512);
        self.stream.read_bytes(&mut buffer).await?;

        let (query_io, _) = query_io::parse(buffer)?;
        Ok(query_io)
    }

    pub async fn write_value(&mut self, value: QueryIO) -> Result<(), std::io::Error> {
        self.stream.write_all(value.serialize().as_bytes()).await?;
        Ok(())
    }
}

impl<T, U> QueryManager<T, &'static ClientRequestController<U>>
where
    T: TWrite + TRead,
    U: TEnDecoder,
{
    pub(crate) async fn extract_query(&mut self) -> Result<(ClientRequest, QueryArguments)> {
        let query_io = self.read_value().await?;
        match query_io {
            QueryIO::Array(value_array) => Ok((
                value_array
                    .first()
                    .context("request not given")?
                    .clone()
                    .unpack_bulk_str()?
                    .try_into()?,
                QueryArguments::new(value_array.into_iter().skip(1).collect()),
            )),
            _ => Err(anyhow::anyhow!("Unexpected command format")),
        }
    }

    pub(crate) async fn handle(
        &mut self,
        cancellation_token: impl TCancellationWatcher,
        cmd: ClientRequest,
        args: QueryArguments,
    ) -> Result<(), std::io::Error> {
        let result = self.controller.handle(cancellation_token, cmd, args).await;
        match result {
            Ok(response) => self.write_value(response).await?,
            Err(e) => self.write_value(QueryIO::Err(e.to_string())).await?,
        }
        Ok(())
    }
}
