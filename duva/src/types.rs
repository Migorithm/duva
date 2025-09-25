#[derive(Debug)]
pub(crate) struct Callback<T>(pub(crate) tokio::sync::oneshot::Sender<T>);

impl<T> Callback<T> {
    pub(crate) fn create() -> (Self, CallbackAwaiter<T>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (Callback(tx), CallbackAwaiter(rx))
    }

    pub(crate) fn send(self, value: T) {
        let _ = self.0.send(value);
    }
}

#[derive(Debug)]
pub(crate) struct CallbackAwaiter<T>(pub(crate) tokio::sync::oneshot::Receiver<T>);

impl<T> CallbackAwaiter<T> {
    pub(crate) async fn recv(self) -> T {
        self.0.await.expect("Channel Closed")
    }

    pub(crate) async fn wait(self) {
        let _ = self.0.await;
    }
}

impl<T> PartialEq for Callback<T> {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl<T> Eq for Callback<T> {}
