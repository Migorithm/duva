use tokio::sync::oneshot;

use super::replication::Replication;

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

    pub(crate) fn respond_with(self, res: ConfigResponse) {
        let _ = self.callback.send(res);
    }
}
pub enum ConfigResource {
    Dir,
    DbFileName,
    FilePath,
    ReplicationInfo,
}

pub enum ConfigResponse {
    Dir(String),
    DbFileName(String),
    FilePath(String),
    ReplicationInfo(Replication),
}

pub enum ConfigCommand {
    ReplicaPing,
    SetDbFileName(String),
}
