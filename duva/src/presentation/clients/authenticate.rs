use crate::domains::cluster_actors::replication::ReplicationId;
use crate::{
    domains::{IoError, TSerdeReadWrite, cluster_actors::topology::Topology},
    presentation::clients::stream::{ClientStreamReader, ClientStreamWriter},
};
use tokio::net::TcpStream;
use uuid::Uuid;

pub(crate) async fn authenticate(
    mut stream: TcpStream,
    topology: Topology,
    is_leader: bool,
    replication_id: ReplicationId,
) -> Result<(ClientStreamReader, ClientStreamWriter), IoError> {
    let auth_req: AuthRequest = stream.deserialized_read().await?;

    let client_id = match auth_req.client_id {
        | Some(client_id) => {
            Uuid::parse_str(&client_id).map_err(|e| IoError::Custom(e.to_string()))?
        },
        | None => Uuid::now_v7(),
    };

    stream
        .serialized_write(AuthResponse {
            client_id: client_id.to_string(),
            request_id: auth_req.request_id,
            topology,
            connected_to_leader: is_leader,
            replication_id,
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

#[derive(Debug, Clone, Default, bincode::Decode, bincode::Encode)]
pub struct AuthResponse {
    pub client_id: String,
    pub request_id: u64,
    pub topology: Topology,
    pub connected_to_leader: bool,
    pub replication_id: ReplicationId,
}
