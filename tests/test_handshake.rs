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
use common::{find_free_port_in_range, run_server_process, start_test_server, TestStreamHandler};
use redis_starter_rust::{
    adapters::cancellation_token::CancellationTokenFactory,
    services::{
        cluster::actor::ClusterActor,
        config::{actor::ConfigActor, manager::ConfigManager},
    },
};
use std::io::{BufRead, BufReader, Read};
use tokio::net::TcpStream;

mod common;

#[tokio::test]
async fn test_master_threeway_handshake() {
    // GIVEN - master server configuration
    let config = ConfigActor::default();
    let mut manager = ConfigManager::new(config);

    // ! `peer_bind_addr` is bind_addr dedicated for peer connections
    manager.port = find_free_port_in_range(6000, 6553).await.unwrap();
    let master_cluster_bind_addr = manager.peer_bind_addr();
    let cluster_actor: ClusterActor = ClusterActor::default();

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
    // ! The first argument is the replication ID of the master
    // ! Since this is the first time the replica is connecting to the master, the replication ID will be ? (a question mark)
    h.send(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        .await;

    // THEN - client receives FULLRESYNC - this is a dummy response
    assert!(h
        .get_response()
        .await
        .starts_with("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"));
}

#[tokio::test]
async fn test_slave_threeway_handshake() {
    // GIVEN - master server configuration
    // Find free ports for the master and replica
    let master_port = find_free_port_in_range(6000, 6553).await.unwrap();
    let replica_port = find_free_port_in_range(6001, 6553).await.unwrap();

    // Start the master server as a child process
    let mut master_process = run_server_process(master_port, None);

    let master_stdout = master_process.stdout.take();
    wait_for_message(
        master_stdout.expect("failed to take stdout"),
        format!(
            "listening peer connection on localhost:{}...",
            master_port + 10000
        )
        .as_str(),
    );

    // WHEN run replica
    let mut replica_process =
        run_server_process(replica_port, Some(format!("localhost:{}", master_port)));

    // Read stdout from the replica process
    let mut stdout = replica_process.stdout.take();
    wait_for_message(
        stdout.take().unwrap(),
        "[INFO] Three-way handshake completed",
    );
}

fn wait_for_message<T: Read>(read: T, target: &str) {
    let mut buf = BufReader::new(read).lines();

    while let Some(Ok(line)) = buf.next() {
        if line == target {
            break;
        }
    }
}
