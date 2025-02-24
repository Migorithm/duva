use super::*;

impl ClientController<Acceptor> {
    pub(crate) fn new(actor_registry: ActorRegistry) -> Self {
        Self {
            cluster_communication_manager: actor_registry.cluster_communication_manager(),

            cache_manager: actor_registry.cache_manager,
            config_manager: actor_registry.config_manager,
            acceptor: PhantomData,
        }
    }

    pub(super) fn to_handler(self) -> ClientController<Handler> {
        ClientController {
            cluster_communication_manager: self.cluster_communication_manager,
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

        let handler = self.to_handler();

        // name the loop
        loop {
            let Ok(requests) = stream.extract_query().await else {
                eprintln!("invalid user request");
                continue;
            };

            let results = match handler.maybe_consensus_then_execute(requests).await {
                Ok(results) => results,

                // ! One of the following errors can be returned:
                // ! consensus or handler or commit
                Err(e) => {
                    eprintln!("[ERROR] {:?}", e);
                    let _ = stream.write(QueryIO::Err(e.to_string().into())).await;
                    continue;
                }
            };

            for res in results {
                if let Err(e) = stream.write(res).await {
                    if e.should_break() {
                        break;
                    }
                }
            }
        }
    }
}
