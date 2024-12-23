/// Three-way handshake test
/// 1. Client sends PING command
/// 2. Client sends REPLCONF listening-port command as follows:
///    *3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n{len-of-client-port}\r\n{client-port}\r\n
/// 3. Client sends REPLCONF capa command as follows:
///    *3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n
/// 4. Client send PSYNC command as follows:
///    *3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n
///
///
use common::{find_free_port_in_range, start_test_server, TestStreamHandler};
use redis_starter_rust::{
    adapters::cancellation_token::CancellationTokenFactory,
    services::{
        cluster::actor::ClusterActor,
        config::{actor::Config, manager::ConfigManager},
    },
};
use tokio::net::TcpStream;
mod common;

#[tokio::test]
async fn test_threeway_handshake() {
    // GIVEN - master server configuration
    let config = Config::default();
    let mut manager = ConfigManager::new(config);

    // ! `peer_bind_addr` is bind_addr dedicated for peer connections
    manager.port = find_free_port_in_range(6000, 6553).await.unwrap();
    let master_cluster_bind_addr = manager.peer_bind_addr();
    let cluster_actor: ClusterActor = ClusterActor::new();

    let _ = start_test_server(CancellationTokenFactory, manager, cluster_actor).await;
    let mut client_stream = TcpStream::connect(master_cluster_bind_addr).await.unwrap();
    let mut h: TestStreamHandler = client_stream.split().into();

    // WHEN - client sends PING command
    h.send(b"*1\r\n$4\r\nPING\r\n").await;

    // THEN
    assert_eq!(h.get_response().await, "+PONG\r\n");

    // WHEN - client sends REPLCONF listening-port command
    let client_fake_port = 6778;
    h.send(
        format!(
            "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{}\r\n",
            client_fake_port
        )
        .as_bytes(),
    )
    .await;

    // THEN
    assert_eq!(h.get_response().await, "+OK\r\n");

    // WHEN - client sends REPLCONF capa command
    h.send(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
        .await;

    // THEN - client receives OK
    assert_eq!(h.get_response().await, "+OK\r\n");

    // WHEN - client sends PSYNC command
    h.send(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        .await;

    // THEN - client receives FULLRESYNC - this is a dummy response
    assert_eq!(
        h.get_response().await,
        "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"
    );
}
