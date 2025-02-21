use super::*;

impl ClientController<Acceptor> {
    pub(crate) fn new(actor_registry: ActorRegistry) -> Self {
        Self {
            cluster_communication_manager: actor_registry.cluster_communication_manager(),
            ttl_manager: actor_registry.ttl_manager,
            cache_manager: actor_registry.cache_manager,
            config_manager: actor_registry.config_manager,
            acceptor: PhantomData,
        }
    }

    pub(super) fn to_controller(self) -> ClientController<Handler> {
        ClientController {
            cluster_communication_manager: self.cluster_communication_manager,
            ttl_manager: self.ttl_manager,
            cache_manager: self.cache_manager,
            config_manager: self.config_manager,
            acceptor: PhantomData,
        }
    }

    /// Run while loop accepting stream and if the sentinel is received, abort the tasks
    pub async fn accept_client_connections(
        self,
        stop_sentinel_recv: tokio::sync::oneshot::Receiver<()>,
        client_stream_listener: TcpListener,
    ) {
        let mut conn_handlers: Vec<tokio::task::JoinHandle<()>> = Vec::with_capacity(100);

        select! {
            // The following closure doesn't take the ownership of the `conn_handlers` which enables us to abort the tasks
            // when the sentinel is received.
            _ = async {
                    while let Ok((stream, _)) = client_stream_listener.accept().await {
                        conn_handlers.push(tokio::spawn(
                            self.clone().handle_client_stream(stream),
                        ));
                    }
                } =>{

                }
            _ = stop_sentinel_recv => {
                // Reconnection logic should be implemented by client?
                    conn_handlers.iter().for_each(|handle| handle.abort());
                },

        };
    }

    async fn handle_client_stream(self, stream: TcpStream) {
        let mut stream = ClientStream(stream);

        let controller = self.to_controller();

        loop {
            let Ok(requests) = stream.extract_query().await else {
                eprintln!("invalid user request");
                continue;
            };

            let consensus =
                try_join_all(requests.iter().map(|r| controller.try_consensus(&r))).await;
            let Ok(res) = consensus else {
                eprintln!("Consensus failed");
                let _ = stream.write(QueryIO::Err("Consensus failed".into())).await;
                continue;
            };

            for (request, log_index_num) in requests.into_iter().zip(res.into_iter()) {
                // ! if request requires consensus, send it to cluster manager so tranasction inputs can be logged and consensus can be made
                // apply state change
                let res = match controller.handle(request).await {
                    Ok(response) => response,
                    Err(e) => QueryIO::Err(e.to_string().into()),
                };

                // ! run stream.write(res) and state change operation to replicas at the same time
                if let Some(offset) = log_index_num {
                    let _ = controller
                        .cluster_communication_manager
                        .send(ClusterCommand::SendCommitHeartBeat { offset })
                        .await;
                }
                if let Err(e) = stream.write(res).await {
                    if e.should_break() {
                        break;
                    }
                }
            }
        }
    }
}
