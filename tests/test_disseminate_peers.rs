/// This is to test if the server successfully disseminates the peers to the client
/// The client should be able to get the list of peers from the server
mod common;
use common::{find_free_port_in_range, start_test_server, TestStreamHandler};
use redis_starter_rust::{
    adapters::cancellation_token::CancellationTokenFactory,
    services::config::{config_actor::Config, config_manager::ConfigManager},
};
use tokio::net::TcpStream;

#[tokio::test]
#[ignore = "not yet ready"]
async fn test_disseminate_peers() {
    // GIVEN - master server configuration
    let mut config = Config::default();
    config.peers = vec!["localhost:6378".to_string()];
    let mut manager = ConfigManager::new(config);

    // ! `peer_bind_addr` is bind_addr dedicated for peer connections
    manager.port = find_free_port_in_range(6000, 6553).await.unwrap();

    let master_cluster_bind_addr = manager.peer_bind_addr();

    let _ = start_test_server(CancellationTokenFactory, manager).await;

    let mut client_stream = TcpStream::connect(master_cluster_bind_addr).await.unwrap();

    let mut h: TestStreamHandler = client_stream.split().into();
    h.send(b"*1\r\n$4\r\nPING\r\n").await;

    // THEN
    assert_eq!(h.get_response().await, "+PONG\r\n");
}
