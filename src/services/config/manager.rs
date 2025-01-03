use std::sync::atomic::AtomicBool;
use std::time::SystemTime;

use crate::env_var;

use super::actor::ConfigActor;
use super::command::ConfigCommand;
use super::command::ConfigMessage;
use super::command::ConfigQuery;
use super::replication::Replication;
use super::ConfigResource;
use super::ConfigResponse;

use tokio::fs::try_exists;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
pub static IS_MASTER_MODE: AtomicBool = AtomicBool::new(true);

#[derive(Clone)]
pub struct ConfigManager {
    config: Sender<ConfigMessage>,
    pub(crate) startup_time: SystemTime,
    pub port: u16,
    pub(crate) host: &'static str,
}

impl std::ops::Deref for ConfigManager {
    type Target = Sender<ConfigMessage>;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl ConfigManager {
    pub fn new(config: ConfigActor) -> Self {
        let (tx, inbox) = tokio::sync::mpsc::channel(20);

        // Set the initial state of the master mode
        IS_MASTER_MODE.store(
            config.replication.master_port.is_none(),
            std::sync::atomic::Ordering::Relaxed,
        );

        tokio::spawn(config.handle(inbox));

        env_var!({}{
            port = 6379,
            host = "localhost".to_string()
        });

        Self {
            config: tx,
            startup_time: SystemTime::now(),
            port,
            host: Box::leak(host.into_boxed_str()),
        }
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
    //TODO this is immutable! Should be moved to manager
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
    //TODO this is immutable! Should be moved to manager
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

    pub async fn replication_info(&self) -> anyhow::Result<Replication> {
        let res = self.route_query(ConfigResource::ReplicationInfo).await?;

        let ConfigResponse::ReplicationInfo(info) = res else {
            return Err(anyhow::anyhow!("Failed to get replication info"));
        };
        Ok(info)
    }

    pub async fn route_query(&self, resource: ConfigResource) -> anyhow::Result<ConfigResponse> {
        let (callback, rx) = oneshot::channel();
        self.send(ConfigMessage::Query(ConfigQuery::new(callback, resource)))
            .await?;
        Ok(rx.await?)
    }
    pub async fn route_command(&self, command: ConfigCommand) -> anyhow::Result<()> {
        self.send(ConfigMessage::Command(command)).await?;
        Ok(())
    }
}
