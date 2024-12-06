pub mod interface;
pub mod query_io;
pub mod user_request;
mod query_arguments;
pub(crate) mod user_request_handler;
mod cluster_request_handler;
mod cluster_request;

use anyhow::Result;
use bytes::BytesMut;
use interface::{TRead, TWrite};
use query_arguments::QueryArguments;
use query_io::QueryIO;

/// Controller is a struct that will be used to read and write values to the client.
pub struct SocketController<T>
where
    T: TWrite + TRead,
{
    pub(crate) stream: T,
}

impl<T> SocketController<T>
where
    T: TWrite + TRead,
{
    pub(crate) fn new(
        stream: T,
    ) -> Self {
        SocketController {
            stream,
        }
    }

    // crlf
    pub async fn read_value(&mut self) -> Result<Option<(String, QueryArguments)>> {
        let mut buffer = BytesMut::with_capacity(512);
        self.stream.read_bytes(&mut buffer).await?;

        let (user_request, _) = query_io::parse(buffer)?;
        Ok(Some(Self::extract_query(user_request)?))
    }

    pub async fn write_value(&mut self, value: QueryIO) -> Result<()> {
        self.stream.write_all(value.serialize().as_bytes()).await?;
        Ok(())
    }

    fn extract_query(value: QueryIO) -> Result<(String, QueryArguments)> {
        match value {
            QueryIO::Array(value_array) => Ok((
                value_array.first().unwrap().clone().unpack_bulk_str()?,
                QueryArguments::new(value_array.into_iter().skip(1).collect()),
            )),
            _ => Err(anyhow::anyhow!("Unexpected command format")),
        }
    }
}

