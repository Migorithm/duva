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
use tokio::time::timeout;

async fn replica_server_helper(replica_port: u16) {
    let slave_cluster_bind_addr = format!("localhost:{}", replica_port + 10000); // note we add 10000 this is convention
    let listener = tokio::net::TcpListener::bind(&slave_cluster_bind_addr)
        .await
        .unwrap();
    while let Ok((mut stream, _)) = listener.accept().await {
        let mut count = 0;
        while count < 5 {
            let values = stream.read_value().await.unwrap();
            // TODO PING may not be used. It is just a placeholder
            assert_eq!(values.serialize(), "+PING\r\n");
            count += 1;
        }
        break;
    }
}

#[tokio::test]
async fn test_heartbeat_sent_to_multiple_replicas() {
    // GIVEN
    // run fake replica server on specific port
    let fake_repl_port = 6781;
    let fake_repl_address = "localhost:".to_string() + &fake_repl_port.to_string();
    // create master server with fake replica address as peers
    let mut config = Config::default();
    config.peers = vec![fake_repl_address.to_string()];
    let mut manager = ConfigManager::new(config);
    manager.port = find_free_port_in_range(6000, 6553).await.unwrap();
    let master_cluster_bind_addr = manager.peer_bind_addr();
    let _ = start_test_server(CancellationTokenFactory, manager.clone()).await;
    // run the fake replica server in advance
    let handler = tokio::spawn(replica_server_helper(fake_repl_port + 10000));

    // run target replica server on specific port
    let target_repl = 6782;
    {
        let mut config = Config::default();
        config.replication.master_port = Some(manager.port);
        config.replication.master_host = Some("localhost".to_string());
        let mut manager = ConfigManager::new(config);
        manager.port = target_repl;
        let _ = start_test_server(CancellationTokenFactory, manager).await;
    }

    // WHEN target replica connects to master
    let mut repl1_connecting_to_master = TcpStream::connect(master_cluster_bind_addr.clone())
        .await
        .unwrap();
    threeway_handshake_helper(&mut repl1_connecting_to_master, target_repl).await;

    // THEN target replica sends 5 PING messages to fake replica
    // TODO: remove timeout when we implement the actual cluster heartbeat
    timeout(std::time::Duration::from_secs(10), handler)
        .await
        .unwrap()
        .unwrap();
}
