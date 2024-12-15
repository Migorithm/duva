/// After three-way handshake, client will receive peers from the master server
mod common;
use common::{
    find_free_port_in_range, start_test_server, threeway_handshake_helper, TestStreamHandler,
};
use redis_starter_rust::{
    adapters::cancellation_token::CancellationTokenFactory,
    services::config::{config_actor::Config, config_manager::ConfigManager},
};
use tokio::net::TcpStream;

#[tokio::test]
async fn test_disseminate_peers() {
    // GIVEN - master server configuration
    let mut config = Config::default();
    let peer_address_to_test = "localhost:6378";
    config.peers = vec![peer_address_to_test.to_string()];
    let mut manager = ConfigManager::new(config);

    // ! `peer_bind_addr` is bind_addr dedicated for peer connections
    manager.port = find_free_port_in_range(6000, 6553).await.unwrap();

    let master_cluster_bind_addr = manager.peer_bind_addr();
    let _ = start_test_server(CancellationTokenFactory, manager).await;

    let mut client_stream = TcpStream::connect(master_cluster_bind_addr).await.unwrap();

    let mut h: TestStreamHandler = client_stream.split().into();

    let client_fake_port = 6889;
    threeway_handshake_helper(&mut h, client_fake_port).await;

    //THEN
    assert_eq!(
        h.get_response().await,
        format!("+PEERS {peer_address_to_test}\r\n")
    );
}
