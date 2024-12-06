use tokio::sync::oneshot;

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
}

pub enum ConfigResponse {
    Dir(String),
    DbFileName(String),
    FilePath(String),
    ReplicationInfo(Vec<String>),
}
