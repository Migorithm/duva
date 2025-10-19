use async_trait::async_trait;
use futures::{StreamExt, future::join_all};
use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook_tokio::Signals;
use tracing::warn;

use crate::{
    domains::{
        caches::{actor::CacheCommandSender, command::CacheCommand},
        cluster_actors::{ClusterCommand, queue::ClusterActorSender},
    },
    types::{Callback, CallbackAwaiter},
};

pub struct SignalHandler {
    switches: Vec<Box<dyn TActorKillSwitch>>,
    sig_awaiter: CallbackAwaiter<i32>,
}

#[async_trait]
pub trait TActorKillSwitch {
    async fn shutdown_gracefully(&self);
}

impl SignalHandler {
    fn new() -> anyhow::Result<Self> {
        let mut signals = Signals::new(&[SIGTERM, SIGINT, SIGQUIT])?;
        let (tx, rx) = Callback::create();
        // Spawn task to handle signals
        tokio::spawn(async move {
            while let Some(signal) = signals.next().await {
                match signal {
                    SIGTERM | SIGINT | SIGQUIT => {
                        warn!("\nReceived signal: {:?}", signal);
                        tx.send(signal);
                        break;
                    },
                    _ => unreachable!(),
                }
            }
        });

        Ok(SignalHandler { switches: vec![], sig_awaiter: rx })
    }

    async fn spawn(self) {
        self.sig_awaiter.wait().await;
        join_all(
            self.switches
                .iter() // Use .iter() instead of .into_iter()
                .map(|switch| switch.shutdown_gracefully()),
        )
        .await;

        std::process::exit(0)
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

#[async_trait]
impl TActorKillSwitch for CacheCommandSender {
    async fn shutdown_gracefully(&self) {
        let (tx, rx) = Callback::create();
        let _ = self.send(CacheCommand::ShutdownGracefully(tx)).await;
        rx.recv().await;
    }
}
