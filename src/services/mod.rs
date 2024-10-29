pub mod config_handler;
pub mod interface;
pub mod persistence;
pub mod value;

use anyhow::Result;
use config_handler::ConfigHandler;
use interface::{TRead, TWriteBuf};
use persistence::{router::PersistenceRouter, ttl_handlers::set::TtlSetter};
use value::{Value, Values};

use crate::controller::IOController;

impl<U: TWriteBuf + TRead> IOController<U> {
    pub(crate) async fn handle(
        &mut self,
        persistence_router: &PersistenceRouter,
        ttl_sender: TtlSetter,
        mut config_handler: ConfigHandler,
    ) -> Result<()> {
        let Some(v) = self.read_value().await? else {
            return Err(anyhow::anyhow!("Connection closed"));
        };

        let (cmd_str, args) = Values::extract_query(v)?;

        // TODO if it is persistence operation, get the key and hash, take the appropriate sender, send it;
        let response = match cmd_str.as_str() {
            "ping" => Value::SimpleString("PONG".to_string()),
            "echo" => args.first()?,
            "set" => {
                persistence_router
                    .route_set(&args, ttl_sender.clone())
                    .await?;
                Value::SimpleString("OK".to_string())
            }
            "get" => persistence_router.route_get(&args).await?,
            // modify we have to add a new command
            "config" => config_handler.handle_config(&args)?,
            c => panic!("Cannot handle command {}", c),
        };

        self.write_value(response).await?;
        Ok(())
    }
}
