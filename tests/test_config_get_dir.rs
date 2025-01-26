/// if the value of dir is /tmp, then the expected response to CONFIG GET dir is:
/// *2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n
mod common;
use crate::common::array;
use common::spawn_server_process;
use redis_starter_rust::client_utils::ClientStreamHandler;

#[tokio::test]
async fn test_config_get_dir() {
    // GIVEN
    //TODO test config should be dynamically configured
    let process = spawn_server_process();

    let mut h = ClientStreamHandler::new(process.bind_addr()).await;

    // WHEN
    h.send({
        let command = "GET";
        let key = "dir";
        &array(vec!["CONFIG", command, key])
    })
    .await;

    // THEN
    assert_eq!(h.get_response().await, array(vec!["dir", "."]));
}
