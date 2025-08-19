use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Stream, StreamExt};

use crate::domains::cluster_actors::ClusterCommand;

pub struct ClusterActorQueue;

impl ClusterActorQueue {
    pub(crate) fn new(buffer: usize) -> (ClusterActorSender, ClusterActorReceiver) {
        let (low_send, low_recv) = tokio::sync::mpsc::channel(buffer);
        let (high_send, high_recv) = tokio::sync::mpsc::channel(100);

        (ClusterActorSender { low_send, high_send }, ClusterActorReceiver { low_recv, high_recv })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ClusterActorSender {
    low_send: tokio::sync::mpsc::Sender<ClusterCommand>,
    high_send: tokio::sync::mpsc::Sender<ClusterCommand>,
}

impl ClusterActorSender {
    pub(crate) async fn send(
        &self,
        cmd: impl Into<ClusterCommand>,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<ClusterCommand>> {
        let command = cmd.into();

        match command {
            | ClusterCommand::Scheduler(_) | ClusterCommand::Peer(_) => {
                self.high_send.send(command).await
            },
            | ClusterCommand::ConnectionReq(_) | ClusterCommand::Client(_) => {
                self.low_send.send(command).await
            },
        }
    }
}

#[derive(Debug)]
pub struct ClusterActorReceiver {
    low_recv: tokio::sync::mpsc::Receiver<ClusterCommand>,
    high_recv: tokio::sync::mpsc::Receiver<ClusterCommand>,
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
        if let Poll::Ready(msg) = Pin::new(&mut self.high_recv).poll_recv(cx) {
            return Poll::Ready(msg);
        }

        // Then try low-priority
        if let Poll::Ready(msg) = Pin::new(&mut self.low_recv).poll_recv(cx) {
            return Poll::Ready(msg);
        }

        Poll::Pending
    }
}
