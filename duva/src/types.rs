#[derive(Debug)]
pub(crate) struct Callback<T>(pub(crate) tokio::sync::oneshot::Sender<T>);

impl<T> Callback<T> {
    pub(crate) fn create() -> (Self, CallbackAwaiter<T>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (Callback(tx), rx.into())
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
}

impl<T> From<tokio::sync::oneshot::Sender<T>> for Callback<T> {
    fn from(sender: tokio::sync::oneshot::Sender<T>) -> Self {
        Callback(sender)
    }
}

impl<T> From<tokio::sync::oneshot::Receiver<T>> for CallbackAwaiter<T> {
    fn from(value: tokio::sync::oneshot::Receiver<T>) -> Self {
        CallbackAwaiter(value)
    }
}

impl<T> PartialEq for Callback<T> {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl<T> Eq for Callback<T> {}
