use tokio::sync::mpsc::Sender;
use crate::services::cluster::actor::{ClusterActor, PeerAddr};
use crate::services::cluster::command::ClusterCommand;

#[derive(Clone)]
pub struct ClusterManager(Sender<ClusterCommand>);

impl ClusterManager {
    pub fn run(actor: ClusterActor) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tokio::spawn(actor.handle(rx));
        Self(tx)
    }

    pub(crate) async fn get_peers(&self) -> anyhow::Result<Vec<PeerAddr>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.0.send(ClusterCommand::GetPeers(tx)).await?;
        let peers = rx.await?;
        Ok(peers)
    }
}