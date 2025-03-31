use super::{parser::parse_query, request::ClientRequest};
use crate::{
    TSerdeReadWrite,
    clients::authentications::{AuthRequest, AuthResponse},
    domains::{IoError, cluster_actors::session::SessionRequest, query_parsers::QueryIO},
    make_smart_pointer,
    services::interface::TRead,
};

use anyhow::Context;
use tokio::net::TcpStream;
use uuid::Uuid;

pub struct ClientStream {
    pub(crate) stream: TcpStream,
    pub(crate) client_id: Uuid,
}

make_smart_pointer!(ClientStream, TcpStream=>stream);

impl ClientStream {
    pub(crate) async fn authenticate(mut stream: TcpStream) -> Result<Self, IoError> {
        let auth_req: AuthRequest = stream.deserialized_read().await?;

        let client_id = match auth_req.client_id {
            Some(client_id) => {
                // TODO check if the given client_id has been tracked
                Uuid::parse_str(&client_id).map_err(|e| IoError::Custom(e.to_string()))?
            },
            None => Uuid::now_v7(),
        };

        stream
            .serialized_write(AuthResponse { client_id: client_id.to_string(), request_id: 0 })
            .await?;

        Ok(Self { stream, client_id })
    }

    pub(crate) async fn extract_query(&mut self) -> Result<Vec<ClientRequest>, IoError> {
        let query_ios = self.read_values().await?;

        query_ios
            .into_iter()
            .map(|query_io| match query_io {
                QueryIO::Array(value) => {
                    let (command, args) = Self::extract_command_args(value)?;
                    parse_query(None, command.to_lowercase(), args)
                        .map_err(|e| IoError::Custom(e.to_string()))
                },
                QueryIO::SessionRequest { request_id, value } => {
                    let (command, args) = Self::extract_command_args(value)?;
                    parse_query(
                        Some(SessionRequest::new(request_id, self.client_id())),
                        command.to_lowercase(),
                        args,
                    )
                    .map_err(|e| IoError::Custom(e.to_string()))
                },
                _ => Err(IoError::Custom("Unexpected command format".to_string())),
            })
            .collect()
    }

    fn extract_command_args(values: Vec<QueryIO>) -> Result<(String, Vec<String>), IoError> {
        let mut values = values.into_iter().flat_map(|v| v.unpack_single_entry::<String>());
        let command =
            values.next().ok_or(IoError::Custom("Unexpected command format".to_string()))?;
        Ok((command, values.collect()))
    }

    fn client_id(&self) -> uuid::Uuid {
        //TODO client_id should be generated on connection
        Uuid::now_v7()
    }
}
