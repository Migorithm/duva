use super::{ClientController, request::ClientRequest};
use crate::domains::cluster_actors::queue::ClusterActorSender;
use crate::domains::cluster_actors::topology::Topology;
use crate::domains::interface::TSerdeWrite;
use crate::domains::replications::ReplicationRole;
use crate::domains::{
    QueryIO,
    cluster_actors::SessionRequest,
    interface::{TRead, TWrite},
};
use crate::make_smart_pointer;
use crate::prelude::ConnectionRequest;
use crate::prelude::ConnectionResponse;
use crate::prelude::ConnectionResponses;
use crate::presentation::clients::request::ClientAction;
use tokio::{
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::mpsc::Sender,
};
use tracing::{error, info, instrument};

pub struct ClientStreamReader {
    pub(crate) r: OwnedReadHalf,
    pub(crate) client_id: String,
}

impl ClientStreamReader {
    #[instrument(level = tracing::Level::DEBUG, skip(self, handler, stream_writer_sender),fields(client_id= %self.client_id))]
    pub(crate) async fn handle_client_stream(
        mut self,
        handler: ClientController,
        stream_writer_sender: Sender<QueryIO>,
    ) {
        loop {
            // * extract queries
            let query_ios = self.r.read_values().await;
            if let Err(err) = query_ios {
                info!("{}", err);
                if err.should_break() {
                    return;
                }
                let _ = stream_writer_sender.send(QueryIO::Err(err.to_string().into())).await;
                continue;
            }

            // * map client request
            let requests = query_ios.unwrap().into_iter().map(|query_io| {
                let QueryIO::SessionRequest { request_id, action } = query_io else {
                    return Err("Unexpected command format".to_string());
                };
                Ok(ClientRequest {
                    action,
                    session_req: SessionRequest::new(request_id, self.client_id.clone()),
                })
            });

            for req in requests {
                match req {
                    Err(err) => {
                        let _ = stream_writer_sender.send(QueryIO::Err(err.into())).await;
                        break;
                    },
                    Ok(ClientRequest { action, session_req }) => {
                        info!(?action, "Processing request");

                        // * processing part
                        let result = match action {
                            ClientAction::NonMutating(non_mutating_action) => {
                                handler.handle_non_mutating(non_mutating_action).await
                            },
                            ClientAction::Mutating(log_entry) => {
                                handler.handle_mutating(session_req, log_entry).await
                            },
                        };

                        let response = result.unwrap_or_else(|e| {
                            error!("failure on state change / query {e}");
                            QueryIO::Err(e.to_string().into())
                        });
                        if stream_writer_sender.send(response).await.is_err() {
                            return;
                        }
                    },
                }
            }
        }
    }
}

pub struct ClientStreamWriter(pub(crate) OwnedWriteHalf);
make_smart_pointer!(ClientStreamWriter, OwnedWriteHalf);
impl ClientStreamWriter {
    pub(crate) fn run(
        mut self,
        mut topology_observer: tokio::sync::broadcast::Receiver<Topology>,
    ) -> Sender<QueryIO> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(2000);
        tokio::spawn(async move {
            while let Some(data) = rx.recv().await {
                if let Err(e) = self.write(data).await
                    && e.should_break()
                {
                    break;
                }
            }
        });

        tokio::spawn({
            let tx = tx.clone();
            async move {
                while let Ok(topology) = topology_observer.recv().await {
                    let _ = tx.send(QueryIO::TopologyChange(topology)).await;
                }
            }
        });
        tx
    }

    pub(crate) async fn send_conn_res(
        &mut self,
        cluster_manager: &ClusterActorSender,
        auth_req: ConnectionRequest,
    ) -> anyhow::Result<String> {
        let replication_state = cluster_manager.route_get_node_state().await?;

        // if the request is not new authentication but the client is already authenticated
        if auth_req.client_id.is_some() && replication_state.role == ReplicationRole::Follower {
            self.serialized_write(
                ConnectionResponses::Authenticated(ConnectionResponse::default()),
            )
            .await?;
            // ! The following will be removed once we allow for follower read.
            return Err(anyhow::anyhow!("Follower node cannot authenticate"));
        }

        let (client_id, request_id) = auth_req.deconstruct()?;

        let connection_response = ConnectionResponse {
            client_id: client_id.clone(),
            request_id,
            topology: cluster_manager.route_get_topology().await?,
            is_leader_node: replication_state.role == ReplicationRole::Leader,
            replication_id: replication_state.replid,
        };
        self.serialized_write(ConnectionResponses::Authenticated(connection_response)).await?;

        Ok(client_id)
    }
}
