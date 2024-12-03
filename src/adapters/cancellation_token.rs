use crate::services::query_manager::interface::{
    TCancellationNotifier, TCancellationTokenFactory, TCancellationWatcher,
};

pub struct CancellationToken {
    sender: tokio::sync::oneshot::Sender<()>,
    receiver: tokio::sync::oneshot::Receiver<()>,
}
impl TCancellationTokenFactory for CancellationToken {
    fn create() -> Self {
        let (tx, rx) = tokio::sync::oneshot::channel();
        Self {
            sender: tx,
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
impl TCancellationNotifier for tokio::sync::oneshot::Sender<()> {
    async fn notify(self, millis: u64) -> anyhow::Result<()> {
        let local_set = tokio::task::LocalSet::new();
        local_set.spawn_local(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(millis)).await;
            let _ = self.send(());
        });

        Ok(())
    }
}
