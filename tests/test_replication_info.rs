/// Cache config should be injected to the handler!
/// This is to enable client to configure things dynamically.

/// if the value of dir is /tmp, then the expected response to CONFIG GET dir is:
/// *2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n
mod common;
use crate::common::{bulk_string, info_command};
use common::{init_config_manager_with_free_port, start_test_server, TestStreamHandler};

use redis_starter_rust::{
    adapters::cancellation_token::CancellationTokenFactory, services::cluster::actor::ClusterActor,
};
use tokio::net::TcpStream;

#[tokio::test]
async fn test_replication_info() {
    // GIVEN
    //TODO test config should be dynamically configured
    let config = init_config_manager_with_free_port().await;

    start_test_server(
        CancellationTokenFactory,
        config.clone(),
        ClusterActor::new(),
    )
    .await;

    let mut client_stream = TcpStream::connect(config.bind_addr()).await.unwrap();
    let mut h: TestStreamHandler = client_stream.split().into();

    // WHEN
    h.send(info_command("replication").as_slice()).await;

    // THEN
    assert_eq!(h.get_response().await, bulk_string(
        "role:master\r\nconnected_slaves:0\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\nsecond_repl_offset:-1\r\nrepl_backlog_active:0\r\nrepl_backlog_size:1048576\r\nrepl_backlog_first_byte_offset:0"
    ));
}
