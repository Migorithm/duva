use super::command::ConfigQuery;
use super::config_actor::Config;
use super::ConfigResource;
use super::ConfigResponse;
use crate::make_smart_pointer;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;

#[derive(Clone)]
pub struct ConfigManager(Sender<ConfigQuery>);

make_smart_pointer!(ConfigManager, Sender<ConfigQuery>);

impl ConfigManager {
    pub fn run_with_default() -> Self {
        let config = Config::default();
        let (tx, inbox) = tokio::sync::mpsc::channel(20);
        tokio::spawn(config.handle(inbox));
        Self(tx)
    }
    pub fn run_with_config(config: Config) -> Self {
        let (tx, inbox) = tokio::sync::mpsc::channel(20);
        tokio::spawn(config.handle(inbox));
        Self(tx)
    }
    pub async fn route_get(
        &self,
        cmd: (String, String),
    ) -> anyhow::Result<Receiver<ConfigResponse>> {
        let resource = match (cmd.0.to_lowercase().as_str(), cmd.1.to_lowercase().as_str()) {
            ("get", "dir") => ConfigResource::Dir,
            ("get", "dbfilename") => ConfigResource::DbFileName,
            _ => Err(anyhow::anyhow!("Invalid command"))?,
        };
        let rx = self.route(resource).await?;
        Ok(rx)
    }
    pub async fn get_filepath(&self) -> anyhow::Result<String> {
        let rx = self.route(ConfigResource::FilePath).await?;

        let ConfigResponse::FilePath(file_path) = rx.await? else {
            return Err(anyhow::anyhow!("Failed to get file path"));
        };
        Ok(file_path.unwrap_or("dump.rdb".into()))
    }

    pub async fn replication_info(&self) -> anyhow::Result<Vec<String>> {
        let rx = self.route(ConfigResource::ReplicationInfo).await?;

        let ConfigResponse::ReplicationInfo(info) = rx.await? else {
            return Err(anyhow::anyhow!("Failed to get replication info"));
        };
        Ok(info)
    }

    async fn route(
        &self,
        resource: ConfigResource,
    ) -> anyhow::Result<oneshot::Receiver<ConfigResponse>> {
        let (callback, rx) = oneshot::channel();
        self.send(ConfigQuery::new(callback, resource)).await?;
        Ok(rx)
    }
}
