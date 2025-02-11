/// After three-way handshake, client will receive peers from the master server
mod common;
use common::{spawn_server_as_slave, spawn_server_process};
use duva::client_utils::ClientStreamHandler;
use duva::write_array;
use crate::common::array;

#[tokio::test]
async fn test_receive_full_sync() {
    // GIVEN
    // Start the master server as a child process
    let master_process = spawn_server_process();
    let mut h = ClientStreamHandler::new(master_process.bind_addr()).await;

    h.send_and_get(&array(vec!["SET", "foo", "bar"])).await;
    assert_eq!(h.send_and_get(&array(vec!["KEYS", "*"])).await, array(vec!["foo"]));

    // WHEN run replica
    let mut replica_process = spawn_server_as_slave(&master_process);

    // THEN
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    replica_process.wait_for_message("[INFO] Snapshot Replaced with Keys: [b\"foo\"]", 1).unwrap();
}
