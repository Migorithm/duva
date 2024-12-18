pub mod actor;
use actor::{ClusterActor, ClusterCommand};
use tokio::sync::mpsc::Sender;

use super::stream_manager::interface::TStream;

pub struct ClusterManager<T: TStream>(Sender<ClusterCommand<T>>);

impl<T: TStream> ClusterManager<T> {
    pub fn run() -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let cluster_manager = ClusterActor::new();
        tokio::spawn(cluster_manager.handle(rx));
        Self(tx)
    }
}
