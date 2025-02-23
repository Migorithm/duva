/// Cache config should be injected to the handler!
/// This is to enable client to configure things dynamically.

/// if the value of dir is /tmp, then the expected response to CONFIG GET dir is:
/// *2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n
mod common;
use crate::common::array;
use common::spawn_server_process;
use duva::client_utils::ClientStreamHandler;
use duva::domains::query_parsers::query_io::QueryIO;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

// TODO response cannot be deterministic!
#[tokio::test]
async fn test_save_read_snapshot() {
    // GIVEN
    let file_name = Some(create_unique_file_name("test_save_dump"));
    let leader_process = spawn_server_process(file_name.clone());

    let mut h = ClientStreamHandler::new(leader_process.bind_addr()).await;

    // WHEN
    // set without expiry time
    let res = h.send_and_get(&array(vec!["SET", "foo", "bar"])).await;
    assert_eq!(res, QueryIO::SimpleString("OK".into()).serialize());

    // set with expiry time
    assert_eq!(
        h.send_and_get(&array(vec!["SET", "foo2", "bar2", "PX", "9999999999"])).await,
        QueryIO::SimpleString("OK".into()).serialize()
    );

    // check keys
    assert_eq!(h.send_and_get(&array(vec!["KEYS", "*"])).await, array(vec!["foo2", "foo"]));

    // check replication info
    let res = h.send_and_get(&array(vec!["INFO", "replication"])).await;
    let info: Vec<&str> = res.split("\r\n").collect();
    let prev_leader_repl_id = info[3].split(":").collect::<Vec<&str>>()[1];
    let prev_leader_repl_offset = info[4].split(":").collect::<Vec<&str>>()[1];

    // THEN
    assert_eq!(h.send_and_get(&array(vec!["SAVE"])).await, QueryIO::Null.serialize());

    // wait for the file to be created
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // THEN
    // kill leader process
    drop(leader_process);

    // run server with the same file name
    let leader_process = spawn_server_process(file_name.clone());

    // wait for the server to start
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let mut h = ClientStreamHandler::new(leader_process.bind_addr()).await;

    // keys
    assert_eq!(h.send_and_get(&array(vec!["KEYS", "*"])).await, array(vec!["foo2", "foo"]));

    // replication info
    let res = h.send_and_get(&array(vec!["INFO", "replication"])).await;
    let info: Vec<&str> = res.split("\r\n").collect();
    let leader_repl_id = info[3].split(":").collect::<Vec<&str>>()[1];
    let leader_repl_offset = info[4].split(":").collect::<Vec<&str>>()[1];

    // THEN
    assert_eq!(leader_repl_id, prev_leader_repl_id);
    assert_eq!(leader_repl_offset, prev_leader_repl_offset);

    let _ = std::fs::remove_file(file_name.as_ref().unwrap());
    let aof_filename = file_name.as_ref().unwrap().to_string() + ".aof";
    let _ = std::fs::remove_file(aof_filename);
}

fn create_unique_file_name(function_name: &str) -> String {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();

    format!("test_{}_{}.rdb", function_name, timestamp)
}
