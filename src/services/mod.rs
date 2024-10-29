pub mod config_handler;
pub mod interface;
pub mod persistence;

pub mod query_manager;

use anyhow::Result;
use config_handler::ConfigHandler;
use interface::{TRead, TWriteBuf};
use persistence::{
    command::PersistCommand, router::PersistenceRouter, ttl_handlers::set::TtlSetter,
};

use query_manager::{query::Args, value::Value, MessageParser};

// Facade for the service layer
// This struct will be used to handle the incoming requests and send the response back to the client.
pub(crate) struct ServiceFacade {
    pub(crate) config_handler: config_handler::ConfigHandler,
    pub(crate) ttl_sender: TtlSetter,
}
impl ServiceFacade {
    pub fn new(config_handler: ConfigHandler, ttl_sender: TtlSetter) -> Self {
        ServiceFacade {
            config_handler: config_handler,
            ttl_sender: ttl_sender,
        }
    }

    pub async fn handle<U: TWriteBuf + TRead>(
        &mut self,
        resp_handler: &mut MessageParser<U>,
        persistence_router: &PersistenceRouter,
    ) -> Result<()> {
        let Some(v) = resp_handler.read_value().await? else {
            return Err(anyhow::anyhow!("Connection closed"));
        };

        let (cmd_str, args) = Args::extract_query(v)?;

        // TODO if it is persistence operation, get the key and hash, take the appropriate sender, send it;
        let response = match cmd_str.as_str() {
            "ping" => Value::SimpleString("PONG".to_string()),
            "echo" => args.first()?,
            "set" => {
                persistence_router
                    .route_set(&args, self.ttl_sender.clone())
                    .await?;
                Value::SimpleString("OK".to_string())
            }
            "get" => persistence_router.route_get(&args).await?,
            // modify we have to add a new command
            "config" => self.config_handler.handle_config(&args)?,
            c => panic!("Cannot handle command {}", c),
        };

        resp_handler.write_value(response).await?;
        Ok(())
    }
}
