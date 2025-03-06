use crate::domains::config_actors::actor::ConfigActor;
use crate::domains::config_actors::command::ConfigCommand;
use crate::domains::config_actors::command::ConfigMessage;
use crate::domains::config_actors::command::ConfigQuery;
use crate::domains::config_actors::command::ConfigResource;
use crate::domains::config_actors::command::ConfigResponse;

use chrono::{DateTime, Utc};
use tokio::fs::try_exists;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

#[derive(Clone)]
pub struct ConfigManager {
    config: Sender<ConfigMessage>,
    pub(crate) startup_time: DateTime<Utc>,
    pub port: u16,
    pub(crate) host: String,
}

impl std::ops::Deref for ConfigManager {
    type Target = Sender<ConfigMessage>;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl ConfigManager {
    pub fn new(config: ConfigActor, host: String, port: u16) -> Self {
        let (tx, inbox) = tokio::sync::mpsc::channel(20);

        config.handle(inbox);

        Self { config: tx, startup_time: Utc::now(), port, host }
    }

    // The following is used on startup and check if the file exists
    pub async fn try_filepath(&self) -> anyhow::Result<Option<String>> {
        let res = self.route_query(ConfigResource::FilePath).await?;

        let ConfigResponse::FilePath(file_path) = res else {
            return Ok(None);
        };
        match try_exists(&file_path).await {
            Ok(true) => Ok(Some(file_path)),
            Ok(false) => {
                println!("File does not exist");
                Ok(None)
            }
            Err(_) => {
                println!("Error in try_filepath");
                Ok(None)
            } // Not given a dbfilename
        }
    }

    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn peer_bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port + 10000)
    }

    pub async fn route_get(&self, cmd: (String, String)) -> anyhow::Result<ConfigResponse> {
        let resource = match (cmd.0.to_lowercase().as_str(), cmd.1.to_lowercase().as_str()) {
            ("get", "dir") => ConfigResource::Dir,
            ("get", "dbfilename") => ConfigResource::DbFileName,
            _ => Err(anyhow::anyhow!("Invalid command"))?,
        };
        let res = self.route_query(resource).await?;
        Ok(res)
    }
    pub async fn get_filepath(&self) -> anyhow::Result<String> {
        let res = self.route_query(ConfigResource::FilePath).await?;

        let ConfigResponse::FilePath(file_path) = res else {
            return Err(anyhow::anyhow!("Failed to get file path"));
        };
        Ok(file_path)
    }

    pub async fn route_query(&self, resource: ConfigResource) -> anyhow::Result<ConfigResponse> {
        let (callback, rx) = oneshot::channel();
        self.send(ConfigMessage::Query(ConfigQuery::new(callback, resource))).await?;
        Ok(rx.await?)
    }
    pub async fn route_command(&self, command: ConfigCommand) -> anyhow::Result<()> {
        self.send(ConfigMessage::Command(command)).await?;
        Ok(())
    }
}
