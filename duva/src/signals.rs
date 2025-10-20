use async_trait::async_trait;
use futures::{StreamExt, future::join_all};
use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook_tokio::Signals;
use tracing::warn;

use crate::types::{Callback, CallbackAwaiter};

#[async_trait]
pub trait TActorKillSwitch: std::marker::Send {
    async fn shutdown_gracefully(&self);
}

pub struct SignalHandler {
    switches: Vec<Box<dyn TActorKillSwitch>>,
    sig_awaiter: CallbackAwaiter<i32>,
}

impl SignalHandler {
    pub(crate) fn new() -> anyhow::Result<Self> {
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

    pub(crate) async fn wait_signals(self) {
        self.sig_awaiter.wait().await;
        join_all(self.switches.iter().map(|switch| switch.shutdown_gracefully())).await;

        println!("Shutdown...");
        std::process::exit(0)
    }

    pub(crate) fn register(&mut self, switches: Vec<Box<dyn TActorKillSwitch>>) {
        self.switches.extend(switches);
    }
}
