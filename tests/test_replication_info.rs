/// Cache config should be injected to the handler!
/// This is to enable client to configure things dynamically.

/// if the value of dir is /tmp, then the expected response to CONFIG GET dir is:
/// *2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n
mod common;

use common::{array, spawn_server_process};

use duva::client_utils::ClientStreamHandler;

#[tokio::test]
async fn test_replication_info() {
    // GIVEN
    let process = spawn_server_process();
    let mut h = ClientStreamHandler::new(process.bind_addr()).await;

    // WHEN
    let res = h.send_and_get(&array(vec!["INFO", "replication"])).await;

    // THEN
    let info: Vec<&str> = res.split("\r\n").collect();

    // THEN
    assert_eq!(info[0], "$249");
    assert_eq!(info[1], "role:master");
    assert_eq!(info[2], "connected_slaves:0");
    assert!(info[3].starts_with("master_replid:"));
    assert_eq!(info[4], "master_repl_offset:0");
    assert_eq!(info[5], "second_repl_offset:-1");
    assert_eq!(info[6], "repl_backlog_active:0");
    assert_eq!(info[7], "repl_backlog_size:1048576");
    assert_eq!(info[8], "repl_backlog_first_byte_offset:0");
    assert_eq!(info[9], "self_identifier:127.0.0.1:49152");
}
