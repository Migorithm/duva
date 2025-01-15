mod common;
use common::threeway_handshake_helper;
use redis_starter_rust::services::interface::TStream;
use std::collections::HashMap;
use tokio::net::TcpListener;
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

    // WHEN - new replica is connecting to master, the newly added server should start sending PING to the other servers attached to the master

    // THEN
}
