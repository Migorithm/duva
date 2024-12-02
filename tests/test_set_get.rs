/// The following is to test out the set operation with expiry
/// Firstly, we set a key with a value and an expiry of 300ms
/// Then we get the key and check if the value is returned
/// After 300ms, we get the key again and check if the value is not returned (-1)
mod common;
use common::{integration_test_config, start_test_server, TestStreamHandler};
use tokio::net::TcpStream;

#[tokio::test]
async fn test() {
    // GIVEN
    let config = integration_test_config().await;

    let _ = start_test_server::<
        (
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        ),
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    >(config)
    .await;

    let mut client_stream = TcpStream::connect(config.bind_addr()).await.unwrap();
    let mut h: TestStreamHandler = client_stream.split().into();

    // WHEN
    h.send(b"*5\r\n$3\r\nSET\r\n$10\r\nsomanyrand\r\n$3\r\nbar\r\n$2\r\npx\r\n$3\r\n300\r\n")
        .await;

    // THEN
    assert_eq!(h.get_response().await, "+OK\r\n");

    // WHEN
    h.send(b"*2\r\n$3\r\nGET\r\n$10\r\nsomanyrand\r\n").await;

    // THEN
    let res = h.get_response().await;
    assert_eq!(res, "$3\r\nbar\r\n");

    // WHEN - wait for 300ms
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    h.send(b"*2\r\n$3\r\nGET\r\n$10\r\nsomanyrand\r\n").await;

    // THEN
    let res = h.get_response().await;
    assert_eq!(res, "$-1\r\n");
}
