//! This file contains tests for heartbeat between server and client
//! Any interconnected system should have a heartbeat mechanism to ensure that the connection is still alive
//! In this case, the server will send PING message to the client and the client will respond with PONG message
//! The following test simulate the replica server with the use of TcpStream.
//! Actual implementation will be standalone replica server that will be connected to the master server

mod common;

use std::time::Duration;

use common::{find_free_port_in_range, start_test_server, threeway_handshake_helper};
use redis_starter_rust::{
    adapters::cancellation_token::CancellationTokenFactory,
    services::{
        cluster::actor::ClusterActor,
        config::{actor::ConfigActor, manager::ConfigManager},
        stream_manager::interface::TStream,
    },
};
use tokio::{net::TcpStream, task::JoinHandle, time::timeout};

// The following simulate the replica server by creating a TcpStream that will be connected by the master server
async fn receive_server_ping_from_replica_stream(
    replica_server_port: u16,
    master_cluster_bind_addr: String,
) -> JoinHandle<()> {
    let mut stream_handler = TcpStream::connect(master_cluster_bind_addr.clone())
        .await
        .unwrap();
    threeway_handshake_helper(&mut stream_handler, replica_server_port).await;

    tokio::spawn(async move {
        let mut count = 0;
        while let Ok(values) = stream_handler.read_value().await {
            if count == 5 {
                break;
            }
            // TODO PING may not be used. It is just a placeholder
            assert_eq!(values.serialize(), "+PING\r\n");
            count += 1;
        }
    })
}

#[tokio::test]
async fn test_heartbeat() {
    // GIVEN
    // run the random server on a random port
    let config = ConfigActor::default();
    let mut manager = ConfigManager::new(config);
    manager.port = find_free_port_in_range(6000, 6553).await.unwrap();
    let master_cluster_bind_addr = manager.peer_bind_addr();
    let _ = start_test_server(CancellationTokenFactory, manager, ClusterActor::new()).await;

    // run the client bind stream on a random port so it can later get connection request from server
    let handler =
        receive_server_ping_from_replica_stream(6778, master_cluster_bind_addr.clone()).await;

    //WHEN we await on handler, it will receive 5 PING messages
    timeout(Duration::from_secs(6), handler)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn test_heartbeat_sent_to_multiple_replicas() {
    // GIVEN
    // run the random server on a random port
    let config = ConfigActor::default();
    let mut manager = ConfigManager::new(config);
    manager.port = find_free_port_in_range(6000, 6553).await.unwrap();
    let master_cluster_bind_addr = manager.peer_bind_addr();
    let _ = start_test_server(CancellationTokenFactory, manager, ClusterActor::new()).await;

    // run the client bind stream on a random port so it can later get connection request from server
    let repl1_handler =
        receive_server_ping_from_replica_stream(6779, master_cluster_bind_addr.clone()).await;
    let repl2_handler =
        receive_server_ping_from_replica_stream(6780, master_cluster_bind_addr.clone()).await;

    //WHEN we await on handler, it will receive 5 PING messages
    let _ = tokio::join!(repl1_handler, repl2_handler);
}
