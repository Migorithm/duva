/// After three-way handshake, client will receive peers from the master server
mod common;
use common::{find_free_port_in_range, start_test_server, threeway_handshake_helper};
use redis_starter_rust::{
    adapters::cancellation_token::CancellationTokenFactory,
    services::{
        config::{config_actor::Config, config_manager::ConfigManager},
        stream_manager::interface::TStream,
    },
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

    let client_fake_port = 6889;
    let message = threeway_handshake_helper(&mut client_stream, client_fake_port).await;

    let expected = format!("+PEERS {}\r\n", peer_address_to_test);
    if let Some(combined) = message {
        assert_eq!(combined.serialize(), expected);
    } else {
        let values = client_stream.read_value().await.unwrap();
        assert_eq!(values.serialize(), expected);
    }

    //THEN
}
