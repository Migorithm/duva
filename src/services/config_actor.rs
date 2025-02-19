use crate::domains::config_actors::{
    actor::ConfigActor,
    command::{ConfigCommand, ConfigMessage, ConfigResource, ConfigResponse},
};
use tokio::sync::mpsc::Receiver;

impl ConfigActor {
    pub fn handle(mut self, mut inbox: Receiver<ConfigMessage>) {
        tokio::spawn(async move {
            while let Some(msg) = inbox.recv().await {
                match msg {
                    ConfigMessage::Query(query) => match query.resource {
                        ConfigResource::Dir => {
                            query.respond_with(ConfigResponse::Dir(self.dir.into()));
                        }
                        ConfigResource::DbFileName => {
                            query.respond_with(ConfigResponse::DbFileName(self.dbfilename.into()));
                        }
                        ConfigResource::FilePath => {
                            query.respond_with(ConfigResponse::FilePath(self.get_filepath()));
                        }
                    },
                    ConfigMessage::Command(config_command) => match config_command {
                        ConfigCommand::SetDbFileName(new_file_name) => {
                            self.set_dbfilename(&new_file_name);
                        }
                    },
                }
            }
        });
    }
}
