use common::{
    bulk_string, info_command, init_config_with_free_port, init_slave_config_with_free_port,
    start_test_server, TestStreamHandler,
};
use redis_starter_rust::adapters::cancellation_token::CancellationTokenFactory;
use tokio::net::TcpStream;
mod common;

#[tokio::test]
async fn test_handshake() {
    // GIVEN
    //TODO test config should be dynamically configured

    let master_config = init_config_with_free_port().await;
    start_test_server(CancellationTokenFactory, master_config.clone()).await;

    let slave_config = init_slave_config_with_free_port(master_config.port).await;
    start_test_server(CancellationTokenFactory, slave_config).await;

    let slave_config = init_slave_config_with_free_port(master_config.port).await;
    start_test_server(CancellationTokenFactory, slave_config).await;

    let mut client_stream = TcpStream::connect(master_config.bind_addr()).await.unwrap();
    let mut h: TestStreamHandler = client_stream.split().into();
    h.send(info_command("replication").as_slice()).await;

    // THEN
    assert_eq!(h.get_response().await, bulk_string(
        "role:master\r\nconnected_slaves:2\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\nsecond_repl_offset:-1\r\nrepl_backlog_active:0\r\nrepl_backlog_size:1048576\r\nrepl_backlog_first_byte_offset:0"
    ));
}
