/// Cache config should be injected to the handler!
/// This is to enable client to configure things dynamically.

/// if the value of dir is /tmp, then the expected response to CONFIG GET dir is:
/// *2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n
mod common;
use common::{spawn_server_process, terminate_process};
use redis_starter_rust::client_utils::ClientStreamHandler;

use crate::common::array;

use tokio::net::TcpStream;

#[tokio::test]
async fn test_config_get_dir() {
    // GIVEN
    //TODO test config should be dynamically configured
    let master_port = spawn_server_process();

    let mut client_stream = TcpStream::connect(format!("localhost:{}", master_port)).await.unwrap();

    let mut h: ClientStreamHandler = client_stream.split().into();

    // WHEN
    h.send(
        {
            let command = "GET";
            let key = "dir";
            array(vec!["CONFIG", command, key]).into_bytes()
        }
        .as_slice(),
    )
    .await;

    // THEN
    assert_eq!(h.get_response().await, array(vec!["dir", "."]));

    terminate_process(master_port);
}
