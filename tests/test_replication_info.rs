/// Cache config should be injected to the handler!
/// This is to enable client to configure things dynamically.

/// if the value of dir is /tmp, then the expected response to CONFIG GET dir is:
/// *2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n
mod common;
use common::{integration_test_config, TestStreamHandler};
use redis_starter_rust::{adapters::persistence::EnDecoder, start_up};

use tokio::net::TcpStream;

#[tokio::test]
async fn test() {
    // GIVEN
    //TODO test config should be dynamically configured
    let config = integration_test_config().await;

    tokio::spawn(start_up(config, 3, EnDecoder));
    //warm up time
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    let mut client_stream = TcpStream::connect(config.bind_addr()).await.unwrap();
    let mut h: TestStreamHandler = client_stream.split().into();

    // WHEN
    h.send(b"*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n").await;

    // THEN
    assert_eq!(h.get_response().await, "$11\r\nrole:master\r\n");
}
