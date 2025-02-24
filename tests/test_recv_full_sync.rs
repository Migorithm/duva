/// After three-way handshake, client will receive peers from the leader server
mod common;
use crate::common::array;
use common::{FileName, spawn_server_process};
use duva::client_utils::ClientStreamHandler;

#[tokio::test]
async fn test_receive_full_sync() {
    // GIVEN
    let file_name: FileName = FileName(None);
    // Start the leader server as a child process
    let leader_p = spawn_server_process(None, &file_name);
    let mut h = ClientStreamHandler::new(leader_p.bind_addr()).await;

    h.send_and_get(&array(vec!["SET", "foo", "bar"])).await;
    assert_eq!(h.send_and_get(&array(vec!["KEYS", "*"])).await, array(vec!["foo"]));

    // WHEN run replica
    let mut replica_process = spawn_server_process(leader_p.bind_addr().into(), &file_name);

    // THEN
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    replica_process.wait_for_message("[INFO] Full Sync Keys: [b\"foo\"]", 1).unwrap();
}
