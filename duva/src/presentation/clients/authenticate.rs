use crate::domains::cluster_actors::replication::{ReplicationId, ReplicationRole};
use crate::presentation::clusters::communication_manager::ClusterCommunicationManager;
use crate::{
    domains::{IoError, TSerdeReadWrite, cluster_actors::topology::Topology},
    presentation::clients::stream::{ClientStreamReader, ClientStreamWriter},
};
use tokio::net::TcpStream;
use uuid::Uuid;

pub(crate) async fn authenticate(
    mut stream: TcpStream,
    cluster_manager: &ClusterCommunicationManager,
    auth_req: ConnectionRequest,
) -> anyhow::Result<(ClientStreamReader, ClientStreamWriter)> {
    let replication_state = cluster_manager.route_get_replication_state().await?;
    let role = replication_state.role;
    match (auth_req, role.clone()) {
        | (ConnectionRequest::New, _) => {
            let client_id = Uuid::now_v7().to_string();
            stream
                .serialized_write(ConnectionResponses::Connectable(ConnectionResponse {
                    client_id: client_id.clone(),
                    request_id: Default::default(),
                    topology: cluster_manager.route_get_topology().await?,
                    is_leader_node: role == ReplicationRole::Leader,
                    replication_id: replication_state.replid.clone(),
                }))
                .await?;

            let (r, w) = stream.into_split();
            let reader = ClientStreamReader { r, client_id };
            let sender = ClientStreamWriter(w);
            return Ok((reader, sender));
        },
        | (ConnectionRequest::ConnectToLeader(_), ReplicationRole::Follower) => {
            stream.serialized_write(ConnectionResponses::NotConnectable).await?;
            return Err(anyhow::anyhow!("Connect to follower node"));
        },
        | (ConnectionRequest::ConnectToLeader(connection_request), ReplicationRole::Leader)
        | (ConnectionRequest::ConnectToFollower(connection_request), _) => {
            let (client_id, request_id) = connection_request.deconstruct()?;

            stream
                .serialized_write(ConnectionResponses::Connectable(ConnectionResponse {
                    client_id: client_id.to_string(),
                    request_id,
                    topology: cluster_manager.route_get_topology().await?,
                    is_leader_node: role == ReplicationRole::Leader,
                    replication_id: replication_state.replid.clone(),
                }))
                .await?;

            let (r, w) = stream.into_split();
            let reader = ClientStreamReader { r, client_id };
            let sender = ClientStreamWriter(w);
            return Ok((reader, sender));
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq, bincode::Decode, bincode::Encode)]
pub enum ConnectionRequest {
    New,
    ConnectToLeader(RequestBody),
    ConnectToFollower(RequestBody),
}

#[derive(Debug, Clone, PartialEq, Eq, Default, bincode::Decode, bincode::Encode)]
pub struct RequestBody {
    pub client_id: String,
    pub request_id: u64,
}

impl RequestBody {
    pub(crate) fn deconstruct(self) -> anyhow::Result<(String, u64)> {
        let client_id =
            Uuid::parse_str(&self.client_id).map_err(|e| IoError::Custom(e.to_string()))?;
        Ok((client_id.to_string(), self.request_id))
    }
}
#[derive(Debug, Clone, bincode::Decode, bincode::Encode)]
pub enum ConnectionResponses {
    Connectable(ConnectionResponse),
    NotConnectable,
}

#[derive(Debug, Clone, Default, bincode::Decode, bincode::Encode)]
pub struct ConnectionResponse {
    pub client_id: String,
    pub request_id: u64,
    pub topology: Topology,
    pub is_leader_node: bool,
    pub replication_id: ReplicationId,
}
