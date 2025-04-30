use crate::{
    domains::IoError,
    prelude::PeerIdentifier,
    presentation::clients::stream::{ClientStreamReader, ClientStreamWriter},
    services::interface::TSerdeReadWrite,
};
use tokio::net::TcpStream;
use uuid::Uuid;

pub(crate) async fn authenticate(
    mut stream: TcpStream,
    peers: Vec<PeerIdentifier>,
    is_leader: bool,
) -> Result<(ClientStreamReader, ClientStreamWriter), IoError> {
    let auth_req: AuthRequest = stream.deserialized_read().await?;

    let client_id = match auth_req.client_id {
        Some(client_id) => {
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
    let reader = ClientStreamReader { r, client_id };
    let sender = ClientStreamWriter(w);

    Ok((reader, sender))
}

#[derive(Debug, Clone, PartialEq, Eq, Default, bincode::Decode, bincode::Encode)]
pub struct AuthRequest {
    pub client_id: Option<String>,
    pub request_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, bincode::Decode, bincode::Encode)]
pub struct AuthResponse {
    pub client_id: String,
    pub request_id: u64,
    pub cluster_nodes: Vec<PeerIdentifier>,
    pub connected_to_leader: bool,
}
