/// The following is to test out the set operation with expiry
/// Firstly, we set a key with a value and an expiry of 300ms
/// Then we get the key and check if the value is returned
/// After 300ms, we get the key again and check if the value is not returned (-1)
mod common;
use crate::common::{bulk_string, get_command, null_response, ok_response, set_command_with_expiry};
use common::{integration_test_config, start_test_server, TestStreamHandler};
use tokio::net::TcpStream;

#[tokio::test]
async fn test() {
    // GIVEN
    let config = integration_test_config().await;

    let _ = start_test_server(config).await;

    let mut client_stream = TcpStream::connect(config.bind_addr()).await.unwrap();
    let mut h: TestStreamHandler = client_stream.split().into();

    h.send(set_command_with_expiry("somanyrand", "bar", 300).as_slice()).await;
    // THEN
    assert_eq!(h.get_response().await, ok_response());

    // WHEN
    h.send(get_command("somanyrand").as_slice()).await;

    // THEN
    let res = h.get_response().await;
    assert_eq!(res, bulk_string("bar"));

    // WHEN - wait for 300ms
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    h.send(get_command("somanyrand").as_slice()).await;

    // THEN
    let res = h.get_response().await;
    assert_eq!(res, null_response());
}
