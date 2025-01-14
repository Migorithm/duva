mod common;
use common::{start_test_server, threeway_handshake_helper, PORT_DISTRIBUTOR};
use redis_starter_rust::{
    adapters::cancellation_token::CancellationTokenFactory,
    services::{
        config::{actor::ConfigActor, manager::ConfigManager},
        interface::TStream,
    },
};
use std::collections::HashMap;
use tokio::{net::TcpListener, time::timeout};
use tokio::{net::TcpStream, task::JoinHandle};

async fn receive_server_ping_from_replica_stream(
    master_cluster_bind_addr: String,
    replica_port: u16,
) -> JoinHandle<()> {
    // run the fake replica server in advance
    let mut replica_stream = TcpStream::connect(master_cluster_bind_addr.clone()).await.unwrap();
    threeway_handshake_helper(&mut replica_stream, Some(replica_port)).await;

    let listener = TcpListener::bind(format!("localhost:{}", replica_port)).await.unwrap();

    let handler = tokio::spawn(async move {
        // * replica server

        // * faking the replica server
        while let Ok((mut stream, addr)) = listener.accept().await {
            let mut socket_hash = HashMap::new();
            socket_hash.entry(addr).or_insert(0);
            while let Ok(values) = stream.read_value().await {
                if *socket_hash.get(&addr).unwrap() == 5 {
                    break;
                }
                // TODO PING may not be used. It is just a placeholder
                assert_eq!(values.serialize(), "+PING\r\n");
                *socket_hash.get_mut(&addr).unwrap() += 1;
            }
            if socket_hash.values().find(|v| v >= &&5).is_some() {
                break;
            }
        }
    });

    handler
}

#[tokio::test]
#[ignore = "not yet ready"]
async fn test_heartbeat_sent_to_multiple_replicas() {
    // GIVEN
    // create master server with fake replica address as peers
    let config = ConfigActor::default();
    let manager = ConfigManager::new(config);
    let master_cluster_bind_addr = manager.peer_bind_addr();

    let replica_server_cluster_port =
        PORT_DISTRIBUTOR.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let replica_cluster_addr = format!("127.0.0.1:{}", replica_server_cluster_port);
    let _ = start_test_server(CancellationTokenFactory, manager.clone()).await;

    // * pre-connected replica server

    let handler = receive_server_ping_from_replica_stream(
        master_cluster_bind_addr,
        replica_server_cluster_port,
    )
    .await;

    // WHEN - new replica is connecting to master, the newly added server should start sending PING to the other servers attached to the master
    {
        let connecting_replica_port = 6782;
        let mut config = ConfigActor::default();
        config.replication.master_port = Some(manager.port);
        config.replication.master_host = Some("localhost".to_string());
        let mut manager = ConfigManager::new(config);
        manager.port = connecting_replica_port;
        let _ = start_test_server(CancellationTokenFactory, manager).await;
    }

    // THEN
    // TODO: remove timeout when we implement the actual cluster heartbeat
    timeout(std::time::Duration::from_secs(6), handler).await.unwrap().unwrap();
}
