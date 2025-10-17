use std::ffi::c_int;

use futures::StreamExt;
use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook_tokio::Signals;
use tracing::warn;

use crate::types::{Callback, CallbackAwaiter};

pub struct SignalHandler {
    observers: Vec<Box<dyn TObserver>>,
    listener: CallbackAwaiter<c_int>,
}

trait TObserver {}

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

        Ok(SignalHandler { observers: vec![], listener: rx })
    }
}
