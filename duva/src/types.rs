use tokio::net::TcpStream;

#[derive(Debug)]
pub(crate) struct Callback<T>(pub(crate) tokio::sync::oneshot::Sender<T>);
impl<T> Callback<T> {
    pub(crate) fn send(self, value: T) {
        let _ = self.0.send(value);
    }
}
impl<T> Callback<T> {
    pub(crate) fn create() -> (Self, tokio::sync::oneshot::Receiver<T>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (Callback(tx), rx)
    }
}

impl<T> From<tokio::sync::oneshot::Sender<T>> for Callback<T> {
    fn from(sender: tokio::sync::oneshot::Sender<T>) -> Self {
        Callback(sender)
    }
}

impl<T> PartialEq for Callback<T> {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl<T> Eq for Callback<T> {}

#[derive(Debug)]
pub(crate) struct ConnectionStream(pub(crate) TcpStream);
impl PartialEq for ConnectionStream {
    fn eq(&self, other: &Self) -> bool {
        self.0.peer_addr().unwrap() == other.0.peer_addr().unwrap()
    }
}
impl Eq for ConnectionStream {}

impl From<TcpStream> for ConnectionStream {
    fn from(stream: TcpStream) -> Self {
        ConnectionStream(stream)
    }
}
