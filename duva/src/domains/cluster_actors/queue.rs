use crate::{
    domains::{
        cluster_actors::{ClientMessage, ClusterCommand, ConnectionMessage, LazyOption},
        replications::state::ReplicationState,
    },
    prelude::{PeerIdentifier, Topology},
    signals::TActorKillSwitch,
    types::Callback,
};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

pub(crate) struct ClusterActorQueue;

impl ClusterActorQueue {
    pub(crate) fn create(buffer: usize) -> (ClusterActorSender, ClusterActorReceiver) {
        let (normal_send, normal_recv) = tokio::sync::mpsc::channel(buffer);
        let (priority_send, priority_recv) = tokio::sync::mpsc::channel(100);
        (
            ClusterActorSender { normal_send, priority_send },
            ClusterActorReceiver { normal_recv, priority_recv },
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ClusterActorSender {
    normal_send: tokio::sync::mpsc::Sender<ClusterCommand>,
    priority_send: tokio::sync::mpsc::Sender<ClusterCommand>,
}

impl ClusterActorSender {
    pub(crate) async fn send(
        &self,
        cmd: impl Into<ClusterCommand>,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<ClusterCommand>> {
        let command = cmd.into();

        match command {
            ClusterCommand::Scheduler(_)
            | ClusterCommand::Peer(_)
            | ClusterCommand::ShutdownGracefully(_) => self.priority_send.send(command).await,
            ClusterCommand::ConnectionReq(_) | ClusterCommand::Client(_) => {
                self.normal_send.send(command).await
            },
        }
    }

    pub(crate) async fn wait_for_acceptance(&self) {
        let (tx, rx) = Callback::create();
        let _ = self.send(ClientMessage::CanEnter(tx)).await;
        rx.wait().await;
    }
    pub(crate) async fn route_get_peers(&self) -> anyhow::Result<Vec<PeerIdentifier>> {
        let (tx, rx) = Callback::create();
        self.send(ClientMessage::GetPeers(tx)).await?;
        let peers = rx.recv().await;
        Ok(peers)
    }

    pub(crate) async fn route_get_topology(&self) -> anyhow::Result<Topology> {
        let (tx, rx) = Callback::create();
        self.send(ClientMessage::GetTopology(tx)).await?;
        let peers = rx.recv().await;
        Ok(peers)
    }

    pub(crate) async fn route_connect_to_server(
        &self,
        connect_to: PeerIdentifier,
    ) -> anyhow::Result<()> {
        let (tx, rx) = Callback::create();
        self.send(ConnectionMessage::ConnectToServer { connect_to, callback: tx }).await?;
        rx.recv().await?;
        Ok(())
    }

    pub(crate) async fn route_get_node_state(&self) -> anyhow::Result<ReplicationState> {
        let (tx, rx) = Callback::create();
        self.send(ClientMessage::ReplicationState(tx)).await?;
        Ok(rx.recv().await)
    }

    pub(crate) async fn route_get_leader_id(&self) -> anyhow::Result<Option<PeerIdentifier>> {
        let (tx, rx) = Callback::create();
        self.send(ClientMessage::GetLeaderId(tx)).await?;
        Ok(rx.recv().await)
    }

    pub(crate) async fn route_get_cluster_info(&self) -> anyhow::Result<String> {
        //cluster_state:ok
        //cluster_slots_assigned:16384
        //cluster_slots_ok:16384
        //cluster_slots_pfail:0
        //cluster_slots_fail:0
        //cluster_known_nodes:6
        //cluster_size:3
        //cluster_current_epoch:6
        //cluster_my_epoch:2
        //cluster_stats_messages_sent:1483972
        //cluster_stats_messages_received:1483968
        //total_cluster_links_buffer_limit_exceeded:0
        let known_node_len = self.route_get_peers().await?.len();
        Ok(format!("cluster_known_nodes:{known_node_len}"))
    }

    pub(crate) async fn route_forget_peer(
        &self,
        peer_identifier: PeerIdentifier,
    ) -> anyhow::Result<bool> {
        let (tx, rx) = Callback::create();
        self.send(ClientMessage::Forget(peer_identifier, tx)).await?;
        let Some(_) = rx.recv().await else { return Ok(false) };
        Ok(true)
    }

    pub(crate) async fn route_replicaof(
        &self,
        peer_identifier: PeerIdentifier,
    ) -> anyhow::Result<()> {
        let (tx, rx) = Callback::create();
        self.send(ClientMessage::ReplicaOf(peer_identifier, tx)).await?;
        rx.recv().await
    }

    pub(crate) async fn route_cluster_meet(
        &self,
        peer_identifier: PeerIdentifier,
        lazy_option: LazyOption,
    ) -> anyhow::Result<()> {
        let (tx, rx) = Callback::create();
        self.send(ClientMessage::ClusterMeet(peer_identifier, lazy_option, tx)).await?;
        rx.recv().await
    }
    pub(crate) async fn route_cluster_reshard(&self) -> anyhow::Result<()> {
        let (tx, rx) = Callback::create();
        self.send(ClientMessage::ClusterReshard(tx)).await?;
        rx.recv().await
    }

    pub(crate) async fn route_cluster_nodes(&self) -> anyhow::Result<Vec<ReplicationState>> {
        let (tx, rx) = Callback::create();
        self.send(ClientMessage::ClusterNodes(tx)).await?;
        Ok(rx.recv().await)
    }

    pub(crate) async fn route_get_roles(&self) -> anyhow::Result<Vec<String>> {
        let (tx, rx) = Callback::create();
        self.send(ClientMessage::GetRoles(tx)).await?;
        Ok(rx.recv().await.into_iter().map(|(id, role)| format!("{}:{}", id.0, role)).collect())
    }

    pub(crate) async fn route_subscribe_topology_change(
        &self,
    ) -> tokio::sync::broadcast::Receiver<Topology> {
        let (tx, rx) = Callback::create();
        self.send(ClientMessage::SubscribeToTopologyChange(tx)).await;
        rx.recv().await
    }
}

#[async_trait]
impl TActorKillSwitch for ClusterActorSender {
    async fn shutdown_gracefully(&self) {
        let (tx, rx) = Callback::create();
        let _ = self.send(ClusterCommand::ShutdownGracefully(tx)).await;
        rx.recv().await;
    }
}

#[derive(Debug)]
pub struct ClusterActorReceiver {
    normal_recv: tokio::sync::mpsc::Receiver<ClusterCommand>,
    priority_recv: tokio::sync::mpsc::Receiver<ClusterCommand>,
}

impl ClusterActorReceiver {
    pub(crate) async fn recv(&mut self) -> Option<ClusterCommand> {
        self.next().await
    }
}

impl Stream for ClusterActorReceiver {
    type Item = ClusterCommand;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Try high-priority first
        if let Poll::Ready(msg) = Pin::new(&mut self.priority_recv).poll_recv(cx) {
            return Poll::Ready(msg);
        }

        // Then try low-priority
        if let Poll::Ready(msg) = Pin::new(&mut self.normal_recv).poll_recv(cx) {
            return Poll::Ready(msg);
        }

        Poll::Pending
    }
}
