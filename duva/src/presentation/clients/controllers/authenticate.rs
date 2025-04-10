use tokio::net::TcpStream;
use uuid::Uuid;

use crate::{
    clients::authentications::{AuthRequest, AuthResponse},
    domains::IoError,
    prelude::PeerIdentifier,
    presentation::clients::stream::{ClientStreamReader, ClientStreamWriter},
    services::interface::TSerdeReadWrite,
};

pub(crate) async fn authenticate(
    mut stream: TcpStream,
    peers: Vec<PeerIdentifier>,
    is_leader: bool,
) -> Result<ClientStreamReader, IoError> {
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
    let sender = ClientStreamWriter(w).run();
    let reader = ClientStreamReader { r, client_id, sender };

    Ok(reader)
}
