use crate::services::query_manager::interface::{
    TCancellationNotifier, TCancellationTokenFactory, TCancellationWatcher,
};

#[derive(Copy, Clone)]
pub struct CancellationTokenFactory;

impl TCancellationTokenFactory for CancellationTokenFactory {
    fn create(&self, timeout: u64) -> (impl TCancellationNotifier, impl TCancellationWatcher) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (
            CancellationNotifier {
                sender: tx,
                timeout,
            },
            rx,
        )
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
