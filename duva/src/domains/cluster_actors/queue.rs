use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Stream, StreamExt};

use crate::domains::cluster_actors::ClusterCommand;

pub(crate) struct ClusterActorQueue;

impl ClusterActorQueue {
    pub(crate) fn new(buffer: usize) -> (ClusterActorSender, ClusterActorReceiver) {
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
            | ClusterCommand::Scheduler(_) | ClusterCommand::Peer(_) => {
                self.priority_send.send(command).await
            },
            | ClusterCommand::ConnectionReq(_) | ClusterCommand::Client(_) => {
                self.normal_send.send(command).await
            },
        }
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
