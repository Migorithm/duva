pub mod config_handler;
pub mod interface;
pub mod persistence;
pub mod persistence_handler;
pub mod query_manager;
pub mod ttl_handlers;
use anyhow::Result;
use config_handler::ConfigHandler;
use interface::{Database, TRead, TWriteBuf};
use persistence::PersistEnum;
use persistence_handler::PersistenceHandler;
use query_manager::{
    command::Args,
    value::{TtlCommand, Value},
    MessageParser,
};
use tokio::sync::mpsc::Sender;

// Facade for the service layer
// This struct will be used to handle the incoming requests and send the response back to the client.
pub(crate) struct ServiceFacade<DB: Database> {
    pub(crate) config_handler: config_handler::ConfigHandler,
    pub(crate) persistence_handler: PersistenceHandler<DB>,
}
impl<DB: Database> ServiceFacade<DB> {
    pub fn new(config_handler: ConfigHandler, persistence_handler: PersistenceHandler<DB>) -> Self {
        ServiceFacade {
            config_handler: config_handler,
            persistence_handler,
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
            "set" => self.persistence_handler.handle_set(&args).await?,
            "get" => self.persistence_handler.handle_get(&args).await?,
            // modify we have to add a new command
            "config" => self.config_handler.handle_config(&args)?,
            c => panic!("Cannot handle command {}", c),
        };

        resp_handler.write_value(response).await?;
        Ok(())
    }
}
