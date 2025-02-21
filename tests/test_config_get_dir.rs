/// if the value of dir is /tmp, then the expected response to CONFIG GET dir is:
/// *2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n
mod common;
use std::time::Duration;

use crate::common::array;
use common::spawn_server_process;
use duva::client_utils::ClientStreamHandler;
use tokio::time::sleep;

#[tokio::test]
async fn test_config_get_dir() {
    // GIVEN
    let process = spawn_server_process();

    sleep(Duration::from_millis(500)).await;
    let mut h = ClientStreamHandler::new(process.bind_addr()).await;

    let command = "GET";
    let key = "dir";
    let cmd = array(vec!["CONFIG", command, key]);
    // WHEN
    h.send(&cmd).await;

    // THEN
    assert_eq!(h.get_response().await, array(vec!["dir", "."]));
}
