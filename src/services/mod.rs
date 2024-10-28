pub mod config_handler;
pub mod interface;
pub mod persistence;

pub mod query_manager;
pub mod ttl_handlers;

use anyhow::Result;
use config_handler::ConfigHandler;
use interface::{TRead, TWriteBuf};
use persistence::PersistEnum;

use query_manager::{
    command::Args,
    value::{TtlCommand, Value},
    MessageParser,
};
use tokio::sync::mpsc::Sender;

// Facade for the service layer
// This struct will be used to handle the incoming requests and send the response back to the client.
pub(crate) struct ServiceFacade {
    pub(crate) config_handler: config_handler::ConfigHandler,
    pub(crate) ttl_sender: Sender<TtlCommand>,
}
impl ServiceFacade {
    pub fn new(config_handler: ConfigHandler, ttl_sender: Sender<TtlCommand>) -> Self {
        ServiceFacade {
            config_handler: config_handler,
            ttl_sender: ttl_sender,
        }
    }

    pub async fn handle<U: TWriteBuf + TRead>(
        &mut self,
        resp_handler: &mut MessageParser<U>,
        persistence_handlers: &[Sender<PersistEnum>],
    ) -> Result<()> {
        let Some(v) = resp_handler.read_value().await? else {
            return Err(anyhow::anyhow!("Connection closed"));
        };

        let (command, args) = Args::extract_command(v)?;

        // TODO if it is persistence operation, get the key and hash, take the appropriate sender, send it;
        let response = match command.as_str() {
            "ping" => Value::SimpleString("PONG".to_string()),
            "echo" => args.first()?,
            "set" => {
                let shard_key = args.take_shard_key(persistence_handlers.len())?;
                persistence_handlers[shard_key as usize]
                    .send(PersistEnum::Set(args.clone(), self.ttl_sender.clone()))
                    .await?;

                Value::SimpleString("OK".to_string())
            }
            "get" => {
                let shard_key = args.take_shard_key(persistence_handlers.len())?;
                let (tx, rx) = tokio::sync::oneshot::channel();

                persistence_handlers[shard_key as usize]
                    .send(PersistEnum::Get(args.clone(), tx))
                    .await?;

                rx.await?
            }
            // modify we have to add a new command
            "config" => self.config_handler.handle_config(&args)?,
            c => panic!("Cannot handle command {}", c),
        };

        resp_handler.write_value(response).await?;
        Ok(())
    }
}
