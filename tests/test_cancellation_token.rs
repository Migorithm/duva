/// The following is to test out the set operation with expiry
/// Firstly, we set a key with a value and an expiry of 300ms
/// Then we get the key and check if the value is returned
/// After 300ms, we get the key again and check if the value is not returned (-1)
mod common;
use common::{
    init_config_with_free_port, integration_test_config, start_test_server, TestStreamHandler,
};
use redis_starter_rust::services::query_manager::interface::{
    TCancellationNotifier, TCancellationTokenFactory, TCancellationWatcher,
};
use tokio::net::TcpStream;

#[tokio::test]
async fn test_cancellation_token() {
    // GIVEN
    let config = integration_test_config(init_config_with_free_port().await);

    let _ = start_test_server::<TestCancellationToken>(config).await;

    let mut client_stream = TcpStream::connect(config.bind_addr()).await.unwrap();
    let mut h: TestStreamHandler = client_stream.split().into();

    // WHEN
    h.send(b"*5\r\n$3\r\nSET\r\n$10\r\nsomanyrand\r\n$3\r\nbar\r\n$2\r\npx\r\n$3\r\n300\r\n")
        .await;

    // THEN
    assert_eq!(
        h.get_response().await,
        "-Error operation cancelled due to timeout\r\n"
    );
}

pub struct TestCancellationToken {
    sender: TestNotifier,
    receiver: tokio::sync::oneshot::Receiver<()>,
}
impl TCancellationTokenFactory for TestCancellationToken {
    fn create(_timeout: u64) -> Self {
        let (tx, rx) = tokio::sync::oneshot::channel();
        Self {
            sender: TestNotifier(tx),
            receiver: rx,
        }
    }
    fn split(self) -> (impl TCancellationNotifier, impl TCancellationWatcher) {
        (self.sender, self.receiver)
    }
}

struct TestNotifier(tokio::sync::oneshot::Sender<()>);
impl TCancellationNotifier for TestNotifier {
    fn notify(self) {
        let _ = self.0.send(());
    }
}
