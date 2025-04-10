use super::{parser::parse_query, request::ClientRequest};
use crate::{
    TSerdeReadWrite,
    clients::authentications::{AuthRequest, AuthResponse},
    domains::{
        IoError, cluster_actors::session::SessionRequest, peers::identifier::PeerIdentifier,
        query_parsers::QueryIO,
    },
    services::interface::{TRead, TWrite},
};

use tokio::net::{
    TcpStream,
    tcp::{OwnedReadHalf, OwnedWriteHalf},
};
use uuid::Uuid;

pub struct ClientStream {
    pub(crate) r: ClientReader,
    pub(crate) w: ClientWriter,
}

pub struct ClientReader {
    pub(crate) r: OwnedReadHalf,
    pub(crate) client_id: Uuid,
}
pub struct ClientWriter(pub(crate) OwnedWriteHalf);

impl ClientStream {
    pub(crate) async fn authenticate(
        mut stream: TcpStream,
        peers: Vec<PeerIdentifier>,
        is_leader: bool,
    ) -> Result<Self, IoError> {
        let auth_req: AuthRequest = stream.deserialized_read().await?;

        let client_id = match auth_req.client_id {
            Some(client_id) => {
                // TODO check if the given client_id has been tracked
                Uuid::parse_str(&client_id).map_err(|e| IoError::Custom(e.to_string()))?
            },
            None => Uuid::now_v7(),
        };

        stream
            .serialized_write(AuthResponse {
                client_id: client_id.to_string(),
                request_id: auth_req.request_id,
                cluster_nodes: peers,
                connected_to_leader: is_leader,
            })
            .await?;

        let (r, w) = stream.into_split();
        Ok(Self { r: ClientReader { r, client_id }, w: ClientWriter(w) })
    }
}

impl ClientReader {
    pub(crate) async fn extract_query(&mut self) -> Result<Vec<ClientRequest>, IoError> {
        let query_ios = self.r.read_values().await?;

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
                        Some(SessionRequest::new(request_id, self.client_id)),
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
}

impl ClientWriter {
    pub(crate) async fn write(&mut self, query_io: QueryIO) -> Result<(), IoError> {
        self.0.write(query_io).await
    }
}
