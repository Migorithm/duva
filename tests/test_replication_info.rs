/// Cache config should be injected to the handler!
/// This is to enable client to configure things dynamically.

/// if the value of dir is /tmp, then the expected response to CONFIG GET dir is:
/// *2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n
mod common;

use common::{array, spawn_server_process};

use duva::{client_utils::ClientStreamHandler, services::query_io::QueryIO};

#[tokio::test]
async fn test_replication_info() {
    // GIVEN
    let process = spawn_server_process();
    let mut h = ClientStreamHandler::new(process.bind_addr()).await;

    // WHEN
    let res = h.send_and_get(&array(vec!["INFO", "replication"])).await;

    // THEN
    assert_eq!(res, QueryIO::BulkString("role:master\r\nconnected_slaves:0\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\nsecond_repl_offset:-1\r\nrepl_backlog_active:0\r\nrepl_backlog_size:1048576\r\nrepl_backlog_first_byte_offset:0\r\nself_identifier:localhost:49152".into()).serialize());
}
