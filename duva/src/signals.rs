use async_trait::async_trait;
use futures::StreamExt;
use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook_tokio::Signals;
use tracing::warn;

use crate::{
    domains::cluster_actors::{ClusterCommand, queue::ClusterActorSender},
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
        for switch in self.switches {
            switch.shutdown_gracefully().await;
        }
    }
}

#[async_trait]
impl TActorKillSwitch for ClusterActorSender {
    async fn shutdown_gracefully(&self) {
        let _ = self.send(ClusterCommand::ShutdownGracefully).await;
    }
}
