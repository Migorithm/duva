use std::collections::HashMap;
use tokio::sync::oneshot;

/// ConfigMessage is a message that can be sent to the ConfigManager.
/// It can be either a query or a command.
/// If it is a query, it will have a callback to send the response back to the caller.
/// If it is a command, it will not have a callback.
pub enum ConfigMessage {
    Query(ConfigQuery),
    Command(ConfigCommand),
}

pub struct ConfigQuery {
    pub callback: oneshot::Sender<ConfigResponse>,
    pub resource: ConfigResource,
}
impl ConfigQuery {
    pub(crate) fn new(callback: oneshot::Sender<ConfigResponse>, resource: ConfigResource) -> Self {
        Self { callback, resource }
    }
}
pub enum ConfigResource {
    Dir,
    DbFileName,
    FilePath,
    ReplicationInfo,
    SingleReplicaInfo(String),
}

pub enum ConfigResponse {
    Dir(String),
    DbFileName(String),
    FilePath(String),
    ReplicationInfo(Vec<String>),
    SingleReplicaInfo(HashMap<String, String>),
}

pub enum ConfigCommand {
    ReplicaPing,
    ReplicaConf(String, String, String),
}