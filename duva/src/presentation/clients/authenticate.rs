use crate::domains::cluster_actors::queue::ClusterActorSender;
use crate::domains::cluster_actors::replication::{ReplicationId, ReplicationRole};

use crate::{
    domains::{IoError, TSerdeReadWrite, cluster_actors::topology::Topology},
    presentation::clients::stream::{ClientStreamReader, ClientStreamWriter},
};
use tokio::net::TcpStream;
use uuid::Uuid;

pub(crate) async fn authenticate(
    mut stream: TcpStream,
    cluster_manager: &ClusterActorSender,
    auth_req: ConnectionRequest,
) -> anyhow::Result<(ClientStreamReader, ClientStreamWriter)> {
    let replication_state = cluster_manager.route_get_replication_state().await?;

    // if the request is not new authentication but the client is already authenticated
    if auth_req.client_id.is_some() && replication_state.role == ReplicationRole::Follower {
        stream
            .serialized_write(ConnectionResponse {
                is_leader_node: false,
                client_id: Default::default(),
                request_id: Default::default(),
                topology: Default::default(),
                replication_id: Default::default(),
            })
            .await?;
        // ! The following will be removed once we allow for follower read.
        return Err(anyhow::anyhow!("Follower node cannot authenticate"));
    }

    let (client_id, request_id) = auth_req.deconstruct()?;

    stream
        .serialized_write(ConnectionResponse {
            client_id: client_id.to_string(),
            request_id,
            topology: cluster_manager.route_get_topology().await?,
            is_leader_node: replication_state.role == ReplicationRole::Leader,
            replication_id: replication_state.replid.clone(),
        })
        .await?;

    let (r, w) = stream.into_split();
    let reader = ClientStreamReader { r, client_id };
    let sender = ClientStreamWriter(w);

    Ok((reader, sender))
}

// TODO make the following enum and make it explicit about why it wants to connect
#[derive(Debug, Clone, PartialEq, Eq, Default, bincode::Decode, bincode::Encode)]
pub struct ConnectionRequest {
    pub client_id: Option<String>,
    pub request_id: u64,
}

impl ConnectionRequest {
    pub(crate) fn deconstruct(self) -> anyhow::Result<(String, u64)> {
        let client_id = self.client_id.map_or_else(
            || Ok(Uuid::now_v7()),
            |id| Uuid::parse_str(&id).map_err(|e| IoError::Custom(e.to_string())),
        )?;
        Ok((client_id.to_string(), self.request_id))
    }
}

#[derive(Debug, Clone, Default, bincode::Decode, bincode::Encode)]
pub struct ConnectionResponse {
    pub client_id: String,
    pub request_id: u64,
    pub topology: Topology,
    pub is_leader_node: bool,
    pub replication_id: ReplicationId,
}
