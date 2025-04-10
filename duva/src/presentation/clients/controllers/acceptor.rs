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

    pub(super) fn into_handler(self) -> ClientController<Handler> {
        ClientController {
            cluster_communication_manager: self.cluster_communication_manager,
            cache_manager: self.cache_manager,
            config_manager: self.config_manager,
            acceptor: PhantomData,
        }
    }

    pub(crate) async fn handle_client_stream(self, mut stream: ClientStream) {
        let handler = self.into_handler();

        loop {
            //TODO check on current mode of the node for every query? or get notified when change is made?

            match stream.r.extract_query().await {
                Ok(requests) => {
                    let results = match handler.maybe_consensus_then_execute(requests).await {
                        Ok(results) => results,

                        // ! One of the following errors can be returned:
                        // ! consensus or handler or commit
                        Err(e) => {
                            eprintln!("[ERROR] {:?}", e);
                            let _ = stream.w.write(QueryIO::Err(e.to_string())).await;
                            continue;
                        },
                    };

                    for res in results {
                        if let Err(e) = stream.w.write(res).await {
                            if e.should_break() {
                                break;
                            }
                        }
                    }
                },

                Err(err) => {
                    if err.should_break() {
                        eprintln!("[INFO] {}", err);
                        return;
                    }
                },
            }
        }
    }
}
