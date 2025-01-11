use crate::{
    make_smart_pointer,
    services::{interface::TStream, query_io::QueryIO},
};
use anyhow::Context;

use tokio::net::TcpStream;

use super::{arguments::ClientRequestArguments, request::ClientRequest};

pub struct ClientStream(pub(crate) TcpStream);
make_smart_pointer!(ClientStream, TcpStream);

impl ClientStream {
    pub(crate) async fn extract_query(
        &mut self,
    ) -> anyhow::Result<(ClientRequest, ClientRequestArguments)> {
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
}
