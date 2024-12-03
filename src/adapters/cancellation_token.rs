use crate::services::query_manager::interface::{
    TCancellationNotifier, TCancellationTokenFactory, TCancellationWatcher,
};

pub struct CancellationToken {
    sender: CancellationNotifier,
    receiver: tokio::sync::oneshot::Receiver<()>,
}
impl TCancellationTokenFactory for CancellationToken {
    fn create(timeout: u64) -> Self {
        let (tx, rx) = tokio::sync::oneshot::channel();
        Self {
            sender: CancellationNotifier {
                sender: tx,
                timeout,
            },
            receiver: rx,
        }
    }
    fn split(self) -> (impl TCancellationNotifier, impl TCancellationWatcher) {
        (self.sender, self.receiver)
    }
}

impl TCancellationWatcher for tokio::sync::oneshot::Receiver<()> {
    fn watch(&mut self) -> bool {
        self.try_recv().is_ok()
    }
}
impl TCancellationNotifier for CancellationNotifier {
    fn notify(self) {
        let local_set = tokio::task::LocalSet::new();
        local_set.spawn_local(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(self.timeout)).await;
            let _ = self.sender.send(());
        });
    }
}

struct CancellationNotifier {
    sender: tokio::sync::oneshot::Sender<()>,
    timeout: u64,
}
