/// The following is to test out the set operation with expiry
/// Firstly, we set a key with a value and an expiry of 300ms
/// Then we get the key and check if the value is returned
/// After 300ms, we get the key again and check if the value is not returned (-1)
mod common;
use common::{init_config_with_free_port, start_test_server, TestStreamHandler};
use redis_starter_rust::services::query_manager::interface::{
    TCancellationNotifier, TCancellationTokenFactory, TCancellationWatcher,
};
use tokio::net::TcpStream;

#[tokio::test]
async fn test_cancellation_token() {
    // GIVEN
    let config = init_config_with_free_port().await;

    let _ = start_test_server(TestCancellationTokenFactory, config.clone()).await;

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

#[derive(Clone, Copy)]
pub struct TestCancellationTokenFactory;
impl TCancellationTokenFactory for TestCancellationTokenFactory {
    fn create(&self, _timeout: u64) -> (impl TCancellationNotifier, impl TCancellationWatcher) {
        let (tx, rx) = tokio::sync::oneshot::channel();

        (TestNotifier(tx), rx)
    }
}

struct TestNotifier(tokio::sync::oneshot::Sender<()>);
impl TCancellationNotifier for TestNotifier {
    fn notify(self) {
        let _ = self.0.send(());
    }
}
