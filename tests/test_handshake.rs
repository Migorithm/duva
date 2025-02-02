/// Three-way handshake test
/// 1. Client sends PING command
/// 2. Client sends REPLCONF listening-port command as follows:
///    *3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n{len-of-client-port}\r\n{client-port}\r\n
/// 3. Client sends REPLCONF capa command as follows:
///    *3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n
/// 4. Client send PSYNC command as follows:
///    *3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n
use common::{spawn_server_as_slave, spawn_server_process};
use duva::client_utils::ClientStreamHandler;

mod common;

#[tokio::test]
async fn test_master_threeway_handshake() {
    // GIVEN - master server configuration
    let process = spawn_server_process();
    let mut h = ClientStreamHandler::new(format!("localhost:{}", process.port() + 10000)).await;

    // Case 1 - client sends PING command
    assert_eq!(h.send_and_get(b"*1\r\n$4\r\nPING\r\n").await, "+PONG\r\n");

    // Case 2 - client sends REPLCONF listening-port command
    let client_fake_port = 6778;
    assert_eq!(
        h.send_and_get(
            format!(
                "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{client_fake_port}\r\n",
            )
            .as_bytes(),
        )
        .await,
        "+OK\r\n"
    );

    // Case3 - client sends REPLCONF capa command
    assert_eq!(
        h.send_and_get(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n").await,
        "+OK\r\n"
    );

    // WHEN - client sends PSYNC command
    // ! The first argument is the replication ID of the master
    // ! Since this is the first time the replica is connecting to the master, the replication ID will be ? (a question mark)
    h.send(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n").await;
    assert!(h
        .send_and_get(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        .await
        // dummy response
        .starts_with("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"));
}

#[tokio::test]
async fn test_slave_threeway_handshake() {
    // GIVEN - master server configuration
    // Find free ports for the master and replica
    let master_process = spawn_server_process();

    // WHEN run replica
    let mut replica_process = spawn_server_as_slave(&master_process);

    // Read stdout from the replica process
    replica_process.wait_for_message("[INFO] Three-way handshake completed", 1);
}
