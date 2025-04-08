use crate::common::{ServerEnv, array, spawn_server_process};

use duva::clients::ClientStreamHandler;
use duva::domains::query_parsers::query_io::QueryIO;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

// TODO response cannot be deterministic!
#[tokio::test]
async fn test_snapshot_persists_and_recovers_state() {
    // GIVEN
    let env = ServerEnv::default().with_file_name(create_unique_file_name("test_save_dump"));
    let leader_process = spawn_server_process(&env);

    let mut h = ClientStreamHandler::new(leader_process.bind_addr()).await;

    // WHEN
    // set without expiry time
    let res = h.send_and_get(&array(vec!["SET", "foo", "bar"])).await;
    assert_eq!(res, QueryIO::SimpleString("OK RINDEX 1".into()).serialize());

    // set with expiry time
    assert_eq!(
        h.send_and_get(&array(vec!["SET", "foo2", "bar2", "PX", "9999999999"])).await,
        QueryIO::SimpleString("OK RINDEX 2".into()).serialize()
    );

    // check keys
    assert_eq!(h.send_and_get(&array(vec!["KEYS", "*"])).await, array(vec!["foo2", "foo"]));

    // check replication info
    let res = h.send_and_get(&array(vec!["INFO", "replication"])).await;
    let info: Vec<&str> = res.split("\r\n").collect();

    // WHEN
    assert_eq!(h.send_and_get(&array(vec!["SAVE"])).await, QueryIO::Null.serialize());

    // wait for the file to be created
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    // kill leader process
    drop(leader_process);

    // run server with the same file name
    let new_process = spawn_server_process(&env);

    // wait for the server to start
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let mut client = ClientStreamHandler::new(new_process.bind_addr()).await;

    assert_eq!(client.send_and_get(&array(vec!["KEYS", "*"])).await, array(vec!["foo2", "foo"]));

    // replication info
    let res = client.send_and_get(&array(vec!["INFO", "replication"])).await;
    let info2: Vec<&str> = res.split("\r\n").collect();

    // THEN
    assert_eq!(info, info2);
}

fn create_unique_file_name(function_name: &str) -> String {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();

    format!("test_{}_{}.rdb", function_name, timestamp)
}
