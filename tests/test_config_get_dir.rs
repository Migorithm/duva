/// Cache config should be injected to the handler!
/// This is to enable client to configure things dynamically.

/// if the value of dir is /tmp, then the expected response to CONFIG GET dir is:
/// *2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n
mod common;
use common::{integration_test_config, start_test_server, TestStreamHandler};

use tokio::net::TcpStream;

#[tokio::test]
async fn test() {
    // GIVEN
    //TODO test config should be dynamically configured
    let config = integration_test_config().await;

    let _ = start_test_server(config).await;

    let mut client_stream = TcpStream::connect(config.bind_addr()).await.unwrap();

    let mut h: TestStreamHandler = client_stream.split().into();

    // WHEN
    h.send(b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$3\r\ndir\r\n")
        .await;

    // THEN
    assert_eq!(h.get_response().await, "*2\r\n$3\r\ndir\r\n$0\r\n\r\n");
}
